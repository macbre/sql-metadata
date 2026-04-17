"""Raw SQL preprocessing before AST construction.

Pure string transformations — no sqlglot dependency.  Handles comment
stripping, ``REPLACE INTO`` rewriting, qualified CTE name normalisation,
DB2 isolation-level clauses, malformed-query rejection, and redundant
outer-parenthesis removal.
"""

import itertools
import re
from typing import NamedTuple

from sqlglot.errors import TokenError
from sqlglot.tokens import Tokenizer, TokenType

from sql_metadata.comments import strip_comments_for_parsing as _strip_comments
from sql_metadata.exceptions import InvalidQueryDefinition
from sql_metadata.utils import DOT_PLACEHOLDER


class CleanResult(NamedTuple):
    """Result of :meth:`SqlCleaner.clean`."""

    sql: str | None
    is_replace: bool
    cte_name_map: dict[str, str]


def _is_wrapped(text: str) -> bool:
    """Check whether *text* is wrapped in balanced outer parentheses.

    :param text: SQL string to check.
    :type text: str
    :returns: ``True`` if *text* has balanced outer parentheses.
    :rtype: bool
    """
    if len(text) < 2 or text[0] != "(" or text[-1] != ")":
        return False
    inner = text[1:-1]
    depths = list(
        itertools.accumulate(
            (1 if c == "(" else -1 if c == ")" else 0) for c in inner
        )
    )
    return not depths or min(depths) >= 0


def _strip_outer_parens(sql: str, _depth: int = 0) -> str:
    """Strip redundant outer parentheses from *sql*.

    Needed because sqlglot cannot parse double-wrapped non-SELECT
    statements like ``((UPDATE ...))``.  Uses ``itertools.accumulate``
    to verify balanced parens in one pass, with recursion for nesting.
    A depth guard prevents stack overflow on pathological input.

    :param sql: SQL string that may be wrapped in outer parentheses.
    :type sql: str
    :returns: The unwrapped SQL string.
    :rtype: str
    """
    if _depth > 100:
        return sql
    s = sql.strip()
    if _is_wrapped(s):
        return _strip_outer_parens(s[1:-1].strip(), _depth + 1)
    return s


def _normalize_cte_names(sql: str) -> tuple[str, dict[str, str]]:
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

    def replacer(match: re.Match[str]) -> str:
        prefix = match.group(1)
        qualified_name = match.group(2)
        suffix = match.group(3)
        placeholder = qualified_name.replace(".", DOT_PLACEHOLDER)
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


class SqlCleaner:
    """Preprocess raw SQL strings before dialect parsing.

    All methods are ``@staticmethod`` — the class serves as a namespace
    grouping :meth:`clean` (full preprocessing pipeline consumed by
    :class:`ASTParser`) and :meth:`preprocess_query` (quoting/whitespace
    normalisation consumed by :attr:`Parser.query`).
    """

    @staticmethod
    def preprocess_query(sql: str) -> str:
        """Normalise quoting and whitespace in raw SQL.

        Walks sqlglot's tokenizer output, emitting each token's original
        text verbatim except double-quoted identifiers, which are rewritten
        to backtick-quoted.  Because string literals are ``STRING`` tokens,
        any ``"`` characters inside them are preserved automatically — no
        sentinel substitution needed.  Newlines between tokens (which is
        where whitespace and comments live) are collapsed to spaces.

        :param sql: Raw SQL string.
        :type sql: str
        :returns: The normalised SQL string, or ``""`` for empty input.
        :rtype: str
        """
        if not sql:
            return ""
        try:
            # Use the default tokenizer unconditionally — the MySQL
            # tokenizer reclassifies ``"X"`` as a STRING token (because
            # MySQL with ANSI_QUOTES off treats double-quotes as strings),
            # which would skip the identifier rewrite below.
            tokens = list(Tokenizer().tokenize(sql))
        except TokenError:
            # Malformed SQL — fall back to plain whitespace collapse.
            return re.sub(r" {2,}", " ", sql.replace("\n", " ")).strip()

        parts: list[str] = []
        prev_end = 0
        for tok in tokens:
            # Gap before the token holds whitespace and comments —
            # collapse newlines so the output stays single-line.
            parts.append(sql[prev_end:tok.start].replace("\n", " "))
            raw = sql[tok.start:tok.end + 1]
            if tok.token_type == TokenType.IDENTIFIER and raw.startswith('"'):
                # e.g. "col" → `col`.  String literals are STRING tokens
                # and never enter this branch, so embedded " are safe.
                parts.append(f"`{tok.text}`")
            else:
                parts.append(raw)
            prev_end = tok.end + 1
        parts.append(sql[prev_end:].replace("\n", " "))
        return re.sub(r" {2,}", " ", "".join(parts))

    @staticmethod
    def clean(sql: str) -> CleanResult:
        """Apply all preprocessing steps to raw SQL.

        Steps (in order):

        1. Rewrite ``REPLACE INTO`` → ``INSERT INTO``.
        2. Rewrite ``SELECT...INTO var FROM`` → ``SELECT...FROM``.
        3. Strip comments.
        4. Normalise qualified CTE names.
        5. Strip DB2 isolation-level clauses.
        6. Detect malformed ``WITH...AS(...)  AS`` patterns.
        7. Strip redundant outer parentheses.

        :param sql: Raw SQL string.
        :type sql: str
        :returns: Cleaning result with preprocessed SQL (``None`` if
            effectively empty), replace flag, and CTE name map.
        :rtype: CleanResult
        :raises ValueError: If a malformed WITH pattern is detected.
        """
        is_replace = False
        if re.match(r"\s*REPLACE\b", sql, re.IGNORECASE):
            sql = re.sub(
                r"\bREPLACE\s+INTO\b",
                "INSERT INTO",
                sql,
                count=1,
                flags=re.IGNORECASE,
            )
            is_replace = True

        # Rewrite SELECT...INTO var1,var2 FROM → SELECT...FROM
        # so sqlglot doesn't treat variables as tables.
        # Only match when INTO target has a comma (variable assignment),
        # not MSSQL's SELECT...INTO new_table FROM (table creation).
        sql = re.sub(
            r"(?i)(\bSELECT\b.+?)\bINTO\b\s+\w+\s*,.*?\bFROM\b",
            r"\1FROM",
            sql,
            count=1,
            flags=re.DOTALL,
        )

        clean_sql = _strip_comments(sql)
        if not clean_sql.strip():
            return CleanResult(sql=None, is_replace=is_replace, cte_name_map={})

        clean_sql, cte_name_map = _normalize_cte_names(clean_sql)
        clean_sql = re.sub(
            r"\bwith\s+(ur|cs|rs|rr)\s*$", "", clean_sql, flags=re.IGNORECASE
        ).strip()

        SqlCleaner._detect_malformed_with(clean_sql)

        clean_sql = _strip_outer_parens(clean_sql)
        if not clean_sql.strip():
            return CleanResult(
                sql=None, is_replace=is_replace, cte_name_map=cte_name_map
            )

        return CleanResult(
            sql=clean_sql, is_replace=is_replace, cte_name_map=cte_name_map
        )

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
            raise InvalidQueryDefinition(
                "Malformed WITH clause — extra AS keyword after CTE body"
            )
