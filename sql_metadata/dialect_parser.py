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
    """Detect the appropriate sqlglot dialect and parse SQL into an AST."""

    def parse(self, clean_sql: str) -> tuple[exp.Expression, DialectType]:
        """Parse *clean_sql*, returning ``(ast, dialect)``.

        Detects candidate dialects via heuristics, tries each in order,
        and returns the first non-degraded result.

        :param clean_sql: Preprocessed SQL string (comments stripped, etc.).
        :type clean_sql: str
        :returns: 2-tuple of ``(ast_node, winning_dialect)``.
        :rtype: tuple
        :raises ValueError: If all dialect attempts fail.
        """
        dialects = self._detect_dialects(clean_sql)
        return self._try_dialects(clean_sql, dialects)

    # -- dialect detection --------------------------------------------------

    @staticmethod
    def _detect_dialects(sql: str) -> list[Any]:
        """Choose an ordered list of sqlglot dialects to try for *sql*.

        Heuristics:

        * ``#WORD`` → :class:`HashVarDialect` (MSSQL temp tables).
        * Back-ticks → ``"mysql"``.
        * Square brackets or ``TOP`` → :class:`BracketedTableDialect`.
        * ``UNIQUE`` → try default, MySQL, Oracle.
        * ``LATERAL VIEW`` → ``"spark"`` (Hive).

        :param sql: Cleaned SQL string.
        :type sql: str
        :returns: Ordered list of dialects to attempt.
        :rtype: list
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
        """Try parsing *clean_sql* with each dialect, returning the best.

        :returns: 2-tuple of ``(ast_node, winning_dialect)``.
        :raises ValueError: If all dialect attempts fail.
        """
        last_result = None
        winning_dialect = None
        for dialect in dialects:
            try:
                result = self._parse_with_dialect(clean_sql, dialect)
                if result is None:
                    continue
                last_result = result
                winning_dialect = dialect
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

        # TODO: revisit if sqlglot starts returning None from parse for last dialect
        if last_result is not None:  # pragma: no cover
            return last_result, winning_dialect
        raise InvalidQueryDefinition(
            "Query could not be parsed — no dialect could handle this SQL"
        )

    @staticmethod
    def _parse_with_dialect(clean_sql: str, dialect: Any) -> exp.Expression | None:
        """Parse *clean_sql* with a single dialect, suppressing warnings."""
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
        assert result is not None  # guaranteed by check above
        # TODO: revisit if sqlglot returns top-level Subquery
        if isinstance(result, exp.Subquery) and not result.alias:  # pragma: no cover
            inner = result.this
            if isinstance(inner, exp.Expression):
                return inner
        return result  # type: ignore[return-value]

    # -- quality checks -----------------------------------------------------

    def _is_degraded(self, result: exp.Expression, clean_sql: str) -> bool:
        """Return ``True`` when a better dialect should be tried."""
        if isinstance(result, exp.Command) and not self._is_expected_command(clean_sql):
            return True
        return self._has_parse_issues(result)

    @staticmethod
    def _is_expected_command(sql: str) -> bool:
        """Check whether *sql* legitimately parses as ``exp.Command``."""
        upper = sql.strip().upper()
        return upper.startswith("CREATE FUNCTION")

    @staticmethod
    def _has_parse_issues(ast: exp.Expression) -> bool:
        """Detect signs of a degraded or incorrect parse.

        Checks for table nodes with empty/keyword-like names and column
        nodes whose name is a SQL keyword without a table qualifier.
        """
        for table in ast.find_all(exp.Table):
            if table.name in _BAD_TABLE_NAMES:
                return True
        for col in ast.find_all(exp.Column):
            if col.name.upper() in _BAD_COLUMN_NAMES and not col.table:
                return True
        return False
