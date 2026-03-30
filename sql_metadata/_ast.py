"""Wrap ``sqlglot.parse()`` to produce an AST from raw SQL strings.

This module is the single entry point for SQL parsing in the v3 pipeline.
It handles dialect detection, comment stripping, malformed-query rejection,
and ``REPLACE INTO`` rewriting so that downstream extractors always receive
a clean ``sqlglot.exp.Expression`` tree (or ``None`` / ``ValueError``).

Design notes:

* **Multi-dialect retry** — :meth:`ASTParser._parse` tries several sqlglot
  dialects in order (e.g. ``[None, "mysql"]``) and picks the first result
  that is not degraded (no phantom tables, no unexpected ``Command`` nodes).
* **REPLACE INTO rewrite** — sqlglot parses ``REPLACE INTO`` as an
  ``exp.Command`` (opaque text), so we rewrite it to ``INSERT INTO``
  before parsing and set a flag so the caller can restore the original
  :class:`QueryType`.
* **Qualified CTE names** — names like ``db.cte_name`` confuse sqlglot,
  so :func:`_normalize_cte_names` replaces them with underscore-based
  placeholders and returns a reverse map for later restoration.
"""

import itertools
import re

import sqlglot
from sqlglot import Dialect
from sqlglot import exp
from sqlglot.dialects.tsql import TSQL
from sqlglot.errors import ParseError, TokenError
from sqlglot.tokens import Tokenizer

from sql_metadata._comments import strip_comments_for_parsing as _strip_comments

#: Table names that indicate a degraded parse result.
_BAD_TABLE_NAMES = frozenset({"IGNORE", ""})

#: SQL keywords that should not appear as bare column names.
_BAD_COLUMN_NAMES = frozenset({"UNIQUE", "DISTINCT", "SELECT", "FROM", "WHERE"})


class _HashVarDialect(Dialect):
    """Custom sqlglot dialect that treats ``#WORD`` as identifiers.

    MSSQL uses ``#`` to prefix temporary table names (e.g. ``#temp``)
    and some template engines use ``#VAR#`` placeholders.  The default
    sqlglot tokenizer treats ``#`` as an unknown single-character token;
    this dialect moves it into ``VAR_SINGLE_TOKENS`` so it becomes part
    of a ``VAR`` token instead.

    Used by :meth:`ASTParser._detect_dialects` when hash-variables are
    detected in the SQL.
    """

    class Tokenizer(Tokenizer):
        """Tokenizer subclass that includes ``#`` in variable tokens."""

        SINGLE_TOKENS = {**Tokenizer.SINGLE_TOKENS}
        SINGLE_TOKENS.pop("#", None)
        VAR_SINGLE_TOKENS = {*Tokenizer.VAR_SINGLE_TOKENS, "#"}


class _BracketedTableDialect(TSQL):
    """TSQL dialect for queries containing ``[bracketed]`` identifiers.

    sqlglot's TSQL dialect correctly interprets square-bracket quoting,
    which the default dialect does not.  This thin subclass exists so that
    :meth:`ASTParser._detect_dialects` can return a concrete class that
    :func:`extract_tables` in ``_tables.py`` can later ``isinstance``-check
    to enable bracket-preserving table name construction.
    """

    pass


def _strip_outer_parens(sql: str) -> str:
    """Strip redundant outer parentheses from *sql*.

    Needed because sqlglot cannot parse double-wrapped non-SELECT
    statements like ``((UPDATE ...))``.  Uses ``itertools.accumulate``
    to verify balanced parens in one pass, with recursion for nesting.
    """
    s = sql.strip()

    def _is_wrapped(text):
        if len(text) < 2 or text[0] != "(" or text[-1] != ")":
            return False
        inner = text[1:-1]
        depths = list(itertools.accumulate(
            (1 if c == "(" else -1 if c == ")" else 0) for c in inner
        ))
        return not depths or min(depths) >= 0

    # Recursively strip (using recursion, not a while loop)
    if _is_wrapped(s):
        return _strip_outer_parens(s[1:-1].strip())
    return s


def _normalize_cte_names(sql: str) -> tuple:
    """Replace qualified CTE names with simple placeholders.

    sqlglot cannot parse ``WITH db.cte_name AS (...)`` because it
    interprets ``db.cte_name`` as a table reference.  This function
    rewrites such names to ``db__DOT__cte_name`` and returns a mapping
    so that the original qualified names can be restored after extraction.

    :param sql: SQL string that may contain qualified CTE names.
    :type sql: str
    :returns: A 2-tuple of ``(modified_sql, {placeholder: original_name})``.
    :rtype: tuple
    """
    name_map = {}
    # Find WITH ... AS patterns with qualified names
    pattern = re.compile(
        r"(\bWITH\s+|,\s*)(\w+\.\w+)(\s+AS\s*\()",
        re.IGNORECASE,
    )

    def replacer(match):
        prefix = match.group(1)
        qualified_name = match.group(2)
        suffix = match.group(3)
        # Create a placeholder with double underscores
        placeholder = qualified_name.replace(".", "__DOT__")
        name_map[placeholder] = qualified_name
        return f"{prefix}{placeholder}{suffix}"

    modified = pattern.sub(replacer, sql)

    # Also replace references to qualified CTE names in FROM/JOIN clauses
    for placeholder, original in name_map.items():
        # Replace references but not the definition (already replaced)
        # Use word boundary to avoid partial matches
        modified = re.sub(
            r"\b" + re.escape(original) + r"\b",
            placeholder,
            modified,
        )

    return modified, name_map


class ASTParser:
    """Lazy wrapper around ``sqlglot.parse()`` with dialect auto-detection.

    Instantiated once per :class:`Parser` with the raw SQL string.  The
    actual parsing is deferred until :attr:`ast` is first accessed, at
    which point the SQL is cleaned (comments stripped, ``REPLACE INTO``
    rewritten, qualified CTE names normalised) and parsed through one or
    more sqlglot dialects until a satisfactory AST is obtained.

    :param sql: Raw SQL query string.
    :type sql: str
    """

    def __init__(self, sql: str) -> None:
        """Initialise the parser without triggering SQL parsing.

        :param sql: Raw SQL query string.
        :type sql: str
        """
        self._raw_sql = sql
        self._ast = None
        self._dialect = None
        self._parsed = False
        self._is_replace = False
        self._cte_name_map = {}  # placeholder → original qualified name

    @property
    def ast(self) -> exp.Expression:
        """The sqlglot AST for the query, lazily parsed on first access.

        :returns: Root AST node, or ``None`` for empty/comment-only queries.
        :rtype: exp.Expression
        :raises ValueError: If the SQL is malformed and cannot be parsed.
        """
        if self._parsed:
            return self._ast
        self._parsed = True
        self._ast = self._parse(self._raw_sql)
        return self._ast

    @property
    def dialect(self):
        """The sqlglot dialect that produced the current AST.

        Set as a side-effect of :attr:`ast` access.  May be ``None``
        (default dialect), a string like ``"mysql"``, or a custom
        :class:`Dialect` subclass such as :class:`_HashVarDialect`.

        :returns: The dialect used, or ``None`` for the default dialect.
        :rtype: Optional[Union[str, type]]
        """
        _ = self.ast
        return self._dialect

    @property
    def is_replace(self) -> bool:
        """Whether the original query was a ``REPLACE INTO`` statement.

        ``REPLACE INTO`` is rewritten to ``INSERT INTO`` before parsing
        (sqlglot otherwise produces an opaque ``Command`` node).  This
        flag allows :attr:`Parser.query_type` to restore the correct
        :class:`QueryType.REPLACE` value.

        :returns: ``True`` if the query was rewritten from ``REPLACE``.
        :rtype: bool
        """
        _ = self.ast
        return self._is_replace

    @property
    def cte_name_map(self) -> dict:
        """Map of placeholder CTE names back to their original qualified form.

        Populated by :func:`_normalize_cte_names` during parsing.  Keys
        are underscore-separated placeholders (``db__DOT__name``), values
        are the original dotted names (``db.name``).

        :returns: Placeholder-to-original mapping (may be empty).
        :rtype: dict
        """
        # Ensure parsing has happened
        _ = self.ast
        return self._cte_name_map

    def _preprocess_sql(self, sql: str) -> str:
        """Apply all preprocessing steps to raw SQL before dialect parsing.

        Steps (in order):

        1. Rewrite ``REPLACE INTO`` → ``INSERT INTO`` (sets
           ``self._is_replace``).
        2. Strip comments.
        3. Normalise qualified CTE names (sets ``self._cte_name_map``).
        4. Strip DB2 isolation-level clauses.
        5. Detect malformed ``WITH...AS(...)  AS`` patterns.
        6. Strip redundant outer parentheses.

        :param sql: Raw SQL string.
        :type sql: str
        :returns: Cleaned SQL ready for dialect parsing, or ``None`` if
            the input is effectively empty after preprocessing.
        :rtype: Optional[str]
        :raises ValueError: If a malformed WITH pattern is detected.
        """
        if re.match(r"\s*REPLACE\b", sql, re.IGNORECASE):
            sql = re.sub(
                r"\bREPLACE\s+INTO\b",
                "INSERT INTO",
                sql,
                count=1,
                flags=re.IGNORECASE,
            )
            self._is_replace = True

        # Rewrite SELECT...INTO var1,var2 FROM → SELECT...FROM
        # so sqlglot doesn't treat variables as tables.
        sql = re.sub(
            r"(?i)(\bSELECT\b.+?)\bINTO\b.+?\bFROM\b",
            r"\1FROM",
            sql,
            count=1,
            flags=re.DOTALL,
        )

        clean_sql = _strip_comments(sql)
        if not clean_sql.strip():
            return None

        clean_sql, self._cte_name_map = _normalize_cte_names(clean_sql)
        clean_sql = re.sub(
            r"\bwith\s+(ur|cs|rs|rr)\s*$", "", clean_sql, flags=re.IGNORECASE
        ).strip()

        self._detect_malformed_with(clean_sql)

        clean_sql = _strip_outer_parens(clean_sql)
        return clean_sql if clean_sql.strip() else None

    @staticmethod
    def _detect_malformed_with(clean_sql: str) -> None:
        """Raise ``ValueError`` if the SQL contains a malformed WITH pattern.

        Detects ``WITH...AS(...)  AS <keyword>`` or
        ``WITH...AS(...)  AS <word> <keyword>`` — an extra ``AS`` token
        after the CTE body that indicates malformed SQL.

        :param clean_sql: Preprocessed SQL string.
        :type clean_sql: str
        :raises ValueError: If a malformed WITH pattern is found.
        """
        if not re.match(r"\s*WITH\b", clean_sql, re.IGNORECASE):
            return
        main_kw = r"(?:SELECT|INSERT|UPDATE|DELETE)"
        if re.search(
            r"\)\s+AS\s+" + main_kw + r"\b", clean_sql, re.IGNORECASE
        ) or re.search(r"\)\s+AS\s+\w+\s+" + main_kw + r"\b", clean_sql, re.IGNORECASE):
            raise ValueError("This query is wrong")

    def _is_degraded_result(self, result: exp.Expression, clean_sql: str) -> bool:
        """Check whether a parse result is degraded.

        Returns ``True`` when a better dialect should be tried.

        A result is degraded if it is an unexpected ``exp.Command`` or
        if :meth:`_has_parse_issues` detects structural problems.

        :param result: Parsed AST node.
        :type result: exp.Expression
        :param clean_sql: Preprocessed SQL string.
        :type clean_sql: str
        :returns: ``True`` if the result is degraded.
        :rtype: bool
        """
        if isinstance(result, exp.Command) and not self._is_expected_command(clean_sql):
            return True
        return self._has_parse_issues(result, clean_sql)

    def _try_parse_dialects(self, clean_sql: str, dialects: list) -> exp.Expression:
        """Try parsing *clean_sql* with each dialect, returning the best result.

        Iterates over *dialects* in order, returning the first
        non-degraded parse result.  A result is considered degraded if
        it is an unexpected ``exp.Command`` or has parse issues detected
        by :meth:`_has_parse_issues`.

        :param clean_sql: Preprocessed SQL string.
        :type clean_sql: str
        :param dialects: Ordered list of dialect identifiers to try.
        :type dialects: list
        :returns: Root AST node.
        :rtype: exp.Expression
        :raises ValueError: If all dialect attempts fail.
        """
        last_result = None
        for dialect in dialects:
            try:
                result = self._parse_with_dialect(clean_sql, dialect)
                if result is None:
                    continue
                last_result = result
                is_last = dialect == dialects[-1]
                if not is_last and self._is_degraded_result(result, clean_sql):
                    continue
                self._dialect = dialect
                return result
            except (ParseError, TokenError):
                if dialect is not None and dialect == dialects[-1]:
                    raise ValueError("This query is wrong")
                continue

        if last_result is not None:
            return last_result
        raise ValueError("This query is wrong")

    @staticmethod
    def _parse_with_dialect(clean_sql: str, dialect) -> exp.Expression:
        """Parse *clean_sql* with a single dialect, suppressing warnings.

        :param clean_sql: Preprocessed SQL string.
        :type clean_sql: str
        :param dialect: sqlglot dialect identifier.
        :returns: Parsed AST node (unwrapped from Subquery if needed),
            or ``None`` if parsing produced no result.
        :rtype: Optional[exp.Expression]
        """
        import logging

        logger = logging.getLogger("sqlglot")
        old_level = logger.level
        logger.setLevel(logging.CRITICAL)
        try:
            results = sqlglot.parse(
                clean_sql,
                dialect=dialect,
                error_level=sqlglot.ErrorLevel.WARN,
            )
        finally:
            logger.setLevel(old_level)

        if not results or results[0] is None:
            return None
        result = results[0]
        if isinstance(result, exp.Subquery) and not result.alias:
            result = result.this
        return result

    def _parse(self, sql: str) -> exp.Expression:
        """Parse *sql* into a sqlglot AST, trying multiple dialects.

        Applies preprocessing (comment stripping, CTE normalisation,
        REPLACE INTO rewriting, etc.) then iterates over candidate
        dialects, returning the first non-degraded result.

        :param sql: Raw SQL string (may include comments).
        :type sql: str
        :returns: Root AST node, or ``None`` for empty input.
        :rtype: Optional[exp.Expression]
        :raises ValueError: If all dialect attempts fail or the SQL is
            detected as malformed.
        """
        if not sql or not sql.strip():
            return None

        clean_sql = self._preprocess_sql(sql)
        if clean_sql is None:
            return None

        dialects = self._detect_dialects(clean_sql)
        return self._try_parse_dialects(clean_sql, dialects)

    @staticmethod
    def _is_expected_command(sql: str) -> bool:
        """Check whether *sql* is legitimately parsed as an ``exp.Command``.

        Some statements (e.g. ``CREATE FUNCTION``) are intentionally left
        unparsed by sqlglot and returned as ``exp.Command``.  This method
        distinguishes those from statements that *should* have produced a
        richer AST node.

        :param sql: Cleaned SQL string (comments already stripped).
        :type sql: str
        :returns: ``True`` if ``Command`` is the expected parse result.
        :rtype: bool
        """
        upper = sql.strip().upper()
        return upper.startswith("CREATE FUNCTION")

    @staticmethod
    def _has_parse_issues(ast: exp.Expression, sql: str = "") -> bool:
        """Detect signs of a degraded or incorrect parse.

        Checks for:

        * Table nodes with empty or keyword-like names (``IGNORE``, ``""``).
        * Column nodes whose name is a SQL keyword (``UNIQUE``, ``DISTINCT``)
          without a table qualifier — usually means the parser misidentified
          a keyword as a column.

        Called during the dialect-retry loop to decide whether to try the
        next dialect.

        :param ast: Root AST node to inspect.
        :type ast: exp.Expression
        :param sql: Original SQL (currently unused, reserved for future
            heuristics).
        :type sql: str
        :returns: ``True`` if the AST looks degraded.
        :rtype: bool
        """
        for table in ast.find_all(exp.Table):
            if table.name in _BAD_TABLE_NAMES:
                return True
        for col in ast.find_all(exp.Column):
            if col.name.upper() in _BAD_COLUMN_NAMES and not col.table:
                return True
        return False

    @staticmethod
    def _detect_dialects(sql: str) -> list:
        """Choose an ordered list of sqlglot dialects to try for *sql*.

        Inspects the SQL for dialect-specific syntax and returns a list
        of dialect identifiers (``None`` = default, ``"mysql"``, or a
        custom :class:`Dialect` subclass) to try in order.  The first
        dialect whose result passes :meth:`_has_parse_issues` wins.

        Heuristics:

        * ``#WORD`` → :class:`_HashVarDialect` (MSSQL temp tables).
        * Back-ticks → ``"mysql"``.
        * Square brackets or ``TOP`` → :class:`_BracketedTableDialect`.
        * ``UNIQUE`` → try default, MySQL, Oracle.
        * ``LATERAL VIEW`` → ``"spark"`` (Hive).

        :param sql: Cleaned SQL string.
        :type sql: str
        :returns: Ordered list of dialects to attempt.
        :rtype: list
        """
        from sql_metadata._comments import _has_hash_variables

        upper = sql.upper()
        # #WORD variables (MSSQL) — use custom dialect that treats # as identifier
        if _has_hash_variables(sql):
            return [_HashVarDialect, None, "mysql"]
        if "`" in sql:
            return ["mysql", None]
        if "[" in sql:
            return [_BracketedTableDialect, None, "mysql"]
        if " TOP " in upper:
            return [_BracketedTableDialect, None, "mysql"]
        if " UNIQUE " in upper:
            return [None, "mysql", "oracle"]
        if "LATERAL VIEW" in upper:
            return ["spark", None, "mysql"]
        return [None, "mysql"]
