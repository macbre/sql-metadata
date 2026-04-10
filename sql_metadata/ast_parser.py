"""Wrap ``sqlglot.parse()`` to produce an AST from raw SQL strings.

Thin orchestrator that composes :class:`~sql_cleaner.SqlCleaner` (raw SQL
preprocessing) and :class:`~dialect_parser.DialectParser` (dialect
detection, parsing, quality validation) so that downstream extractors
always receive a clean ``sqlglot.exp.Expression`` tree (or ``None`` /
``ValueError``).
"""

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sql_metadata.dialect_parser import DialectParser
from sql_metadata.sql_cleaner import SqlCleaner


class ASTParser:
    """Lazy wrapper around SQL parsing with dialect auto-detection.

    Instantiated once per :class:`Parser` with the raw SQL string.  The
    actual parsing is deferred until :attr:`ast` is first accessed, at
    which point the SQL is cleaned and parsed through one or more sqlglot
    dialects until a satisfactory AST is obtained.

    :param sql: Raw SQL query string.
    :type sql: str
    """

    def __init__(self, sql: str) -> None:
        self._raw_sql = sql
        self._ast: exp.Expression | None = None
        self._dialect: DialectType = None
        self._parsed = False
        self._is_replace = False
        self._cte_name_map: dict[str, str] = {}

    @property
    def ast(self) -> exp.Expression | None:
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
    def dialect(self) -> DialectType:
        """The sqlglot dialect that produced the current AST.

        Set as a side-effect of :attr:`ast` access.  May be ``None``
        (default dialect), a string like ``"mysql"``, or a custom
        :class:`Dialect` subclass such as :class:`HashVarDialect`.

        :rtype: DialectType
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

        :rtype: bool
        """
        _ = self.ast
        return self._is_replace

    @property
    def cte_name_map(self) -> dict[str, str]:
        """Map of placeholder CTE names back to their original qualified form.

        Keys are underscore-separated placeholders (``db__DOT__name``),
        values are the original dotted names (``db.name``).

        :rtype: dict[str, str]
        """
        _ = self.ast
        return self._cte_name_map

    def _parse(self, sql: str) -> exp.Expression | None:
        """Parse *sql* into a sqlglot AST.

        Delegates preprocessing to :class:`SqlCleaner` and dialect
        detection / parsing to :class:`DialectParser`.

        :param sql: Raw SQL string (may include comments).
        :type sql: str
        :returns: Root AST node, or ``None`` for empty input.
        :rtype: exp.Expression | None
        :raises ValueError: If the SQL is malformed.
        """
        if not sql or not sql.strip():
            return None

        result = SqlCleaner.clean(sql)
        if result.sql is None:
            return None

        self._is_replace = result.is_replace
        self._cte_name_map = result.cte_name_map

        ast, self._dialect = DialectParser().parse(result.sql)
        return ast
