"""SQL dialect detection, parsing, and parse-quality validation.

Combines dialect heuristics (which sqlglot dialect to try), the actual
``sqlglot.parse()`` call, and degraded-result detection into a single
class so that callers only need to call :meth:`DialectParser.parse`.
"""

import logging
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.dialects.redshift import Redshift
from sqlglot.dialects.tsql import TSQL
from sqlglot.errors import ParseError, TokenError
from sqlglot.parsers.redshift import RedshiftParser
from sqlglot.tokens import Tokenizer as BaseTokenizer

from sql_metadata.comments import _has_hash_variables
from sql_metadata.exceptions import InvalidQueryDefinition

#: Table names that indicate a degraded parse result.
_BAD_TABLE_NAMES = frozenset({"IGNORE", ""})

#: SQL keywords that should not appear as bare column names.
_BAD_COLUMN_NAMES = frozenset({"UNIQUE", "DISTINCT", "SELECT", "FROM", "WHERE"})


# ---------------------------------------------------------------------------
# Custom dialect classes
# ---------------------------------------------------------------------------


class HashVarDialect(Dialect):
    """Custom sqlglot dialect that treats ``#WORD`` as identifiers.

    MSSQL uses ``#`` to prefix temporary table names (e.g. ``#temp``)
    and some template engines use ``#VAR#`` placeholders.  The default
    sqlglot tokenizer treats ``#`` as an unknown single-character token;
    this dialect moves it into ``VAR_SINGLE_TOKENS`` so it becomes part
    of a ``VAR`` token instead.
    """

    class Tokenizer(BaseTokenizer):
        """Tokenizer subclass that includes ``#`` in variable tokens."""

        SINGLE_TOKENS = {**BaseTokenizer.SINGLE_TOKENS}
        SINGLE_TOKENS.pop("#", None)
        VAR_SINGLE_TOKENS = {*BaseTokenizer.VAR_SINGLE_TOKENS, "#"}


class _RedshiftAppendParser(RedshiftParser):
    """Redshift parser extended with ``ALTER TABLE ... APPEND FROM``."""

    def _parse_alter_table_append(self) -> "exp.Expr | None":
        self._match_text_seq("FROM")
        return self._parse_table()

    ALTER_PARSERS = {
        **RedshiftParser.ALTER_PARSERS,
        "APPEND": lambda self: self._parse_alter_table_append(),
    }


class RedshiftAppendDialect(Redshift):
    """Redshift dialect extended with ``ALTER TABLE ... APPEND FROM`` support.

    Redshift's ``APPEND FROM`` syntax is not natively supported by sqlglot,
    which causes the statement to degrade to ``exp.Command``.  This dialect
    adds an ``APPEND`` entry to ``ALTER_PARSERS`` so the statement is parsed
    as a proper ``exp.Alter`` with ``exp.Table`` nodes.
    """

    Parser = _RedshiftAppendParser


class BracketedTableDialect(TSQL):
    """TSQL dialect for queries containing ``[bracketed]`` identifiers.

    sqlglot's TSQL dialect correctly interprets square-bracket quoting,
    which the default dialect does not.  This thin subclass exists so
    that ``TableExtractor`` can ``isinstance``-check to enable
    bracket-preserving table name construction.
    """


# ---------------------------------------------------------------------------
# DialectParser
# ---------------------------------------------------------------------------


class DialectParser:
    """Detect the appropriate sqlglot dialect and parse SQL into an AST.

    SQL varies across database engines — back-ticks (MySQL), square
    brackets (TSQL), ``#temp`` tables (MSSQL), ``LATERAL VIEW`` (Hive),
    etc.  A single sqlglot dialect cannot handle all of them, so this
    class first inspects the raw SQL for dialect markers, then tries
    candidate dialects in order and picks the first result that passes
    quality checks.
    """

    def parse(self, clean_sql: str) -> tuple[exp.Expression, DialectType]:
        """Parse *clean_sql* into a sqlglot AST, returning ``(ast, dialect)``.

        Entry point for the two-phase process: first
        :meth:`_detect_dialects` builds a priority-ordered list of
        candidate dialects from syntactic markers in the SQL, then
        :meth:`_try_dialects` attempts each one and returns the first
        non-degraded result.

        :param clean_sql: Preprocessed SQL string produced by
            :class:`~sql_metadata.sql_cleaner.SqlCleaner` (comments
            stripped, outer parentheses removed, CTE names normalised).
        :returns: 2-tuple of ``(ast_root_node, winning_dialect)``.
        :raises InvalidQueryDefinition: If every candidate dialect
            fails to produce a usable AST.
        """
        dialects = self._detect_dialects(clean_sql)
        return self._try_dialects(clean_sql, dialects)

    # -- dialect detection --------------------------------------------------

    @staticmethod
    def _detect_dialects(sql: str) -> list[Any]:
        """Build a priority-ordered list of sqlglot dialects for *sql*.

        Scans the SQL string for syntactic markers that reveal which
        database engine produced it and returns the most likely dialect
        first.  Every list includes at least one fallback so that the
        subsequent :meth:`_try_dialects` loop always has alternatives.

        Heuristics (checked in order, first match wins):

        * ``#WORD`` patterns → :class:`HashVarDialect` (MSSQL ``#temp``
          tables or ``#VAR#`` template placeholders).
        * Back-tick quoting → ``"mysql"`` (MySQL-style identifiers).
        * ``LATERAL VIEW`` → ``"spark"`` (Hive/Spark explode syntax).
        * Square brackets or ``TOP`` keyword →
          :class:`BracketedTableDialect` (TSQL bracket-quoted names).
        * ``UNIQUE`` keyword → default, ``"mysql"``, ``"oracle"``
          (ambiguous across engines).
        * ``APPEND FROM`` → :class:`RedshiftAppendDialect` (Redshift
          ``ALTER TABLE … APPEND FROM`` not natively supported).
        * No markers → default dialect with ``"mysql"`` fallback.

        :param sql: Cleaned SQL string.
        :returns: Ordered list of dialect identifiers or classes to
            attempt.
        """
        upper = sql.upper()
        if _has_hash_variables(sql):
            return [HashVarDialect, None, "mysql"]
        if "`" in sql:
            return ["mysql", None]
        if "LATERAL VIEW" in upper:
            return ["spark", None, "mysql"]
        if "[" in sql or " TOP " in upper:
            return [BracketedTableDialect, None, "mysql"]
        if " UNIQUE " in upper:
            return [None, "mysql", "oracle"]
        if "APPEND FROM" in upper:
            return [RedshiftAppendDialect, None, "mysql"]
        return [None, "mysql"]

    # -- parsing ------------------------------------------------------------

    def _try_dialects(
        self, clean_sql: str, dialects: list[Any]
    ) -> tuple[exp.Expression, DialectType]:
        """Try each candidate dialect in order and return the first good result.

        Iterates over *dialects*, calling :meth:`_parse_with_dialect` for
        each.  A result is accepted immediately if it is the last dialect
        in the list (best-effort) or if :meth:`_is_degraded` reports no
        quality issues.  Degraded results from non-last dialects are
        skipped so the next candidate gets a chance.

        :param clean_sql: Preprocessed SQL string.
        :param dialects: Priority-ordered list from :meth:`_detect_dialects`.
        :returns: 2-tuple of ``(ast_root_node, winning_dialect)``.
        :raises InvalidQueryDefinition: If the last dialect raises a
            parse error, or if no dialect produces a usable AST.
        """
        for dialect in dialects:
            try:
                result = self._parse_with_dialect(clean_sql, dialect)
                if result is None:
                    continue
                is_last = dialect == dialects[-1]
                if not is_last and self._is_degraded(result, clean_sql):
                    continue
                return result, dialect
            except (ParseError, TokenError):
                if dialect is not None and dialect == dialects[-1]:
                    raise InvalidQueryDefinition(
                        "Query could not be parsed — SQL syntax error"
                    )
                continue

        raise InvalidQueryDefinition(
            "Query could not be parsed — no dialect could handle this SQL"
        )

    @staticmethod
    def _parse_with_dialect(clean_sql: str, dialect: Any) -> exp.Expression | None:
        """Parse *clean_sql* with a single sqlglot dialect.

        Uses ``ErrorLevel.WARN`` so that sqlglot returns a best-effort
        AST instead of raising on the first syntax problem — the caller
        decides whether the result is good enough via
        :meth:`_is_degraded`.

        The sqlglot logger is temporarily raised to ``CRITICAL`` during
        the parse call because ``WARN`` mode emits noisy warnings for
        every token it cannot handle.  Since :meth:`_try_dialects`
        intentionally tries multiple dialects expecting some to produce
        degraded results, those warnings are expected and would mislead
        end-users if left visible.

        :param clean_sql: Preprocessed SQL string.
        :param dialect: A sqlglot dialect identifier, class, or ``None``
            for the default dialect.
        :returns: The root AST node, or ``None`` if sqlglot could not
            produce any result.
        """
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
        return results[0]  # type: ignore[return-value]

    # -- quality checks -----------------------------------------------------

    def _is_degraded(self, result: exp.Expression, clean_sql: str) -> bool:
        """Return ``True`` when the parse result is low quality.

        A degraded result means the dialect parsed the SQL without
        raising, but the AST is suspicious — either the whole statement
        collapsed into an opaque ``exp.Command`` (when it should not
        have) or :meth:`_has_parse_issues` found placeholder-like table
        or column names.  When ``True``, :meth:`_try_dialects` skips
        this dialect and moves on to the next candidate.

        :param result: Root AST node from :meth:`_parse_with_dialect`.
        :param clean_sql: Original cleaned SQL (needed to check whether
            ``exp.Command`` is expected).
        :returns: ``True`` if the result should be discarded in favour
            of the next dialect.
        """
        if isinstance(result, exp.Command) and not self._is_expected_command(clean_sql):
            return True
        return self._has_parse_issues(result)

    @staticmethod
    def _is_expected_command(sql: str) -> bool:
        """Return ``True`` when *sql* legitimately parses as ``exp.Command``.

        Some dialect-specific DDL (e.g. Hive ``CREATE FUNCTION … USING
        JAR … WITH SERDEPROPERTIES``) is not supported by any sqlglot
        dialect and always degrades to ``exp.Command``.  This method
        whitelists those known cases so :meth:`_is_degraded` does not
        reject them.

        :param sql: Cleaned SQL string.
        :returns: ``True`` if ``exp.Command`` is the expected result.
        """
        upper = sql.strip().upper()
        return upper.startswith("CREATE FUNCTION")

    @staticmethod
    def _has_parse_issues(ast: exp.Expression) -> bool:
        """Walk the AST looking for signs of a degraded or incorrect parse.

        When sqlglot misinterprets a query it often places SQL keywords
        (``UNIQUE``, ``DISTINCT``, etc.) into column or table name
        positions, or produces table nodes with empty names.  This
        method scans all :class:`~sqlglot.exp.Table` and
        :class:`~sqlglot.exp.Column` nodes for those telltale patterns.

        :param ast: Root AST node to inspect.
        :returns: ``True`` if suspicious nodes were found.
        """
        for table in ast.find_all(exp.Table):
            if table.name in _BAD_TABLE_NAMES:
                return True
        for col in ast.find_all(exp.Column):
            if col.name.upper() in _BAD_COLUMN_NAMES and not col.table:
                return True
        return False
