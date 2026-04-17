"""SQL query parsing facade.

Thin facade that composes the specialised extractors via lazy properties:

* :class:`~ast_parser.ASTParser` — AST construction and dialect detection.
* :class:`~column_extractor.ColumnExtractor` — single-pass column/alias extraction.
* :class:`~table_extractor.TableExtractor` — table extraction with position sorting.
* :class:`~nested_resolver.NestedResolver` — CTE/subquery name and body extraction,
  nested column resolution.
* :mod:`query_type_extractor` — query type detection.
* :mod:`comments` — comment extraction.
"""

import logging
import re
from typing import Any

from sqlglot import exp

from sql_metadata.ast_parser import ASTParser
from sql_metadata.column_extractor import ColumnExtractor
from sql_metadata.comments import extract_comments, strip_comments
from sql_metadata.generalizator import Generalizator
from sql_metadata.keywords_lists import QueryType
from sql_metadata.nested_resolver import NestedResolver
from sql_metadata.query_type_extractor import QueryTypeExtractor
from sql_metadata.sql_cleaner import SqlCleaner
from sql_metadata.table_extractor import TableExtractor
from sql_metadata.utils import UniqueList


class Parser:
    """Parse a SQL query and extract metadata.

    The primary public interface of the ``sql-metadata`` library.  Given a
    raw SQL string, the parser lazily extracts tables, columns, aliases,
    CTE definitions, subqueries, values, comments, and more — each
    available as a cached property.

    :param sql: The SQL query string to parse.
    :type sql: str
    :param disable_logging: If ``True``, suppress all log output.
    :type disable_logging: bool
    """

    def __init__(self, sql: str = "", disable_logging: bool = False) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.disabled = disable_logging

        self._raw_query = sql
        self._query_type: QueryType | None = None

        self._ast_parser = ASTParser(sql)
        self._resolver: NestedResolver | None = None

        self._tokens: list[str] | None = None

        self._columns_extracted = False
        self._columns: UniqueList = UniqueList()
        self._columns_dict: dict[str, UniqueList] = {}
        self._columns_aliases_names: UniqueList = UniqueList()
        self._columns_aliases: dict[str, str | list[str]] = {}
        self._columns_aliases_dict: dict[str, UniqueList] = {}
        self._output_columns: list[str] = []

        self._tables: list[str] | None = None
        self._table_aliases: dict[str, str] | None = None

        self._with_names: list[str] | None = None
        self._with_queries: dict[str, str] | None = None
        self._subqueries: dict[str, str] | None = None
        self._subqueries_names: list[str] | None = None

        self._limit_and_offset: tuple[int, int] | None = None

        self._values: list[Any] | None = None
        self._values_dict: dict[str, int | float | str | list[Any]] | None = None

    def _require_ast(self) -> exp.Expression:
        """Return the AST, asserting it is non-None.

        Every property that needs the AST first triggers a check that
        rejects ``None`` (either a ``ValueError`` from ``ASTParser`` on
        malformed SQL, or an assert in ``query_type`` / ``tables_aliases``
        on empty input).  This helper centralises the resulting
        ``ast is not None`` narrowing so callers can write
        ``ast = self._require_ast()`` instead of a two-line dance.

        :rtype: exp.Expression
        :raises ValueError: Propagated from ``ASTParser`` for malformed SQL.
        """
        ast = self._ast_parser.ast
        assert ast is not None
        return ast

    def _get_resolver(self) -> NestedResolver:
        """Return the cached :class:`NestedResolver` for this query.

        The resolver is created lazily on first access and reused for all
        subsequent column resolution and CTE/subquery work, so the AST is
        only walked once for nested-structure extraction.

        :returns: The shared resolver instance.
        :rtype: NestedResolver
        """
        if self._resolver is None:
            self._resolver = NestedResolver(
                self._require_ast(), parser_factory=Parser
            )
        return self._resolver

    @property
    def query(self) -> str:
        """Return the preprocessed SQL query.

        Preprocessing normalises quoting (double-quoted identifiers become
        backtick-quoted) while preserving quotes inside string literals, and
        collapses newlines and redundant whitespace.

        :rtype: str
        """
        return SqlCleaner.preprocess_query(self._raw_query)

    @property
    def query_type(self) -> QueryType | None:
        """Return the type of the SQL query.

        Delegates to :class:`QueryTypeExtractor` which maps the top-level
        AST node to a :class:`~keywords_lists.QueryType` value (SELECT,
        INSERT, UPDATE, DELETE, CREATE, etc.).  If the AST cannot be built
        (unparseable SQL), the extractor falls back to keyword matching on
        the raw string.

        :rtype: QueryType | None
        """
        if self._query_type:
            return self._query_type
        try:
            ast = self._ast_parser.ast
        except ValueError:
            ast = None
        self._query_type = QueryTypeExtractor(
            ast, self._raw_query, is_replace=self._ast_parser.is_replace
        ).extract()
        return self._query_type

    @property
    def tokens(self) -> list[str]:
        """Return the SQL as a list of token strings.

        Uses the sqlglot tokenizer (dialect-aware) and strips backtick and
        double-quote delimiters from each token so identifiers appear as
        plain names.

        :rtype: list[str]
        """
        if self._tokens is not None:
            return self._tokens
        if not self._raw_query or not self._raw_query.strip():
            self._tokens = []
            return self._tokens
        from sql_metadata.comments import _choose_tokenizer

        sg_tokens = list(
            _choose_tokenizer(self._raw_query).tokenize(self._raw_query)
        )
        self._tokens = [t.text.strip("`").strip('"') for t in sg_tokens]
        return self._tokens

    @property
    def columns(self) -> list[str]:
        """Return the list of column names referenced in the query.

        Walks the sqlglot AST via :class:`ColumnExtractor` in a single DFS
        pass, then resolves CTE and subquery references through
        :class:`NestedResolver`.  When AST construction fails (unparseable
        SQL), falls back to a regex extraction of ``INTO … (col1, col2)``
        column lists.

        :rtype: list[str]
        """
        if self._columns_extracted:
            return self._columns
        self._columns_extracted = True

        try:
            ast = self._require_ast()
            ta = self.tables_aliases
        except ValueError:
            self._columns = UniqueList(self._extract_columns_regex())
            return self._columns

        extractor = ColumnExtractor(ast, ta, self._ast_parser.cte_name_map)
        result = extractor.extract()

        self._columns = result.columns
        self._columns_dict = result.columns_dict
        self._columns_aliases_names = result.alias_names
        self._columns_aliases_dict = result.alias_dict
        self._columns_aliases = result.alias_map
        self._output_columns = result.output_columns

        # Use only aliased subquery names for column resolution —
        # auto-generated names (subquery_1, …) are never referenced in SQL.
        aliased_names = result.subquery_names
        all_names, all_bodies = NestedResolver.extract_subqueries(ast)
        aliased_bodies = {k: v for k, v in all_bodies.items() if k in aliased_names}
        resolver = self._get_resolver()
        self._columns, self._columns_dict, self._columns_aliases = resolver.resolve(
            self._columns,
            self._columns_dict,
            self._columns_aliases,
            aliased_names,
            aliased_bodies,
            self.with_names,
            self.with_queries,
        )
        # Cache full results for the public properties
        self._subqueries_names = all_names
        self._subqueries = all_bodies

        return self._columns

    @property
    def columns_dict(self) -> dict[str, UniqueList]:
        """Return column names organised by query clause.

        Keys are SQL clause names (``"select"``, ``"where"``, ``"join"``,
        ``"order_by"``, ``"group_by"``, etc.) and values are
        :class:`~utils.UniqueList` instances of column names found in that
        clause.  Column aliases are resolved back to their underlying
        columns before inclusion.

        :rtype: dict[str, UniqueList]
        """
        if not self._columns_extracted:
            _ = self.columns
        # Resolve aliases used in other sections
        if self.columns_aliases_dict:
            resolver = self._get_resolver()
            for key, value in self.columns_aliases_dict.items():
                for alias in value:
                    resolved = resolver.resolve_column_alias(
                        alias, self.columns_aliases
                    )
                    for r in resolved:
                        self._columns_dict.setdefault(key, UniqueList()).append(r)
        return self._columns_dict

    @property
    def columns_aliases(self) -> dict[str, str | list[str]]:
        """Return the alias-to-column mapping for column aliases.

        Maps each column alias to the underlying column name(s) it was
        derived from, e.g. ``{"total": "SUM(amount)"}``.  When a CASE
        expression produces multiple source columns the value is a list.

        :rtype: dict[str, str | list[str]]
        """
        if not self._columns_extracted:
            _ = self.columns
        return self._columns_aliases

    @property
    def columns_aliases_dict(self) -> dict[str, UniqueList]:
        """Return column alias names organised by query clause.

        Keys are SQL clause names and values are :class:`~utils.UniqueList`
        instances of alias names defined in that clause.  Complements
        :attr:`columns_aliases` which maps alias → source column.

        :rtype: dict[str, UniqueList]
        """
        if not self._columns_extracted:
            _ = self.columns
        return self._columns_aliases_dict

    @property
    def columns_aliases_names(self) -> list[str]:
        """Return the names of all column aliases used in the query.

        :rtype: list[str]
        """
        if not self._columns_extracted:
            _ = self.columns
        return self._columns_aliases_names

    @property
    def output_columns(self) -> list[str]:
        """Return the ordered list of SELECT output column names.

        Combines real columns and aliases in their original position.
        For example, ``SELECT a, b AS c FROM t`` returns ``["a", "c"]``.

        :rtype: list[str]
        """
        if not self._columns_extracted:
            _ = self.columns
        return self._output_columns

    @property
    def tables(self) -> list[str]:
        """Return the list of table names referenced in the query.

        Tables are extracted from the AST by :class:`TableExtractor`,
        sorted by their position in the SQL text, and filtered to exclude
        CTE names (which appear in :attr:`with_names` instead).

        :rtype: list[str]
        """
        if self._tables is not None:
            return self._tables
        _ = self.query_type
        ast = self._require_ast()
        cte_names = set(self.with_names)
        for placeholder in self._ast_parser.cte_name_map:
            cte_names.add(placeholder)
        extractor = TableExtractor(
            ast,
            cte_names,
            dialect=self._ast_parser.dialect,
        )
        self._tables = extractor.extract()
        return self._tables

    @property
    def tables_aliases(self) -> dict[str, str]:
        """Return the table alias mapping for this query.

        Maps each table alias to the real table name it refers to, e.g.
        ``{"u": "users", "o": "orders"}``.

        :rtype: dict[str, str]
        """
        if self._table_aliases is not None:
            return self._table_aliases
        extractor = TableExtractor(self._require_ast())
        self._table_aliases = extractor.extract_aliases(self.tables)
        return self._table_aliases

    @property
    def with_names(self) -> list[str]:
        """Return the CTE (Common Table Expression) names from the query.

        :rtype: list[str]
        """
        if self._with_names is not None:
            return self._with_names
        resolver = self._get_resolver()
        self._with_names = resolver.extract_cte_names(
            self._ast_parser.cte_name_map
        )
        return self._with_names

    @property
    def with_queries(self) -> dict[str, str]:
        """Return the SQL body for each CTE defined in the query.

        Maps each CTE name to its defining SQL text, e.g.
        ``{"active_users": "SELECT id FROM users WHERE active = 1"}``.

        :rtype: dict[str, str]
        """
        if self._with_queries is not None:
            return self._with_queries
        resolver = self._get_resolver()
        self._with_queries = resolver.extract_cte_bodies(
            self._ast_parser.cte_name_map
        )
        return self._with_queries

    @property
    def subqueries(self) -> dict[str, str]:
        """Return the SQL body for each subquery in the query.

        Maps each subquery name to its SQL text.  Aliased subqueries use
        their alias as the key; unaliased ones get auto-generated names
        (``subquery_1``, ``subquery_2``, …).

        :rtype: dict[str, str]
        """
        if self._subqueries is not None:
            return self._subqueries
        self._subqueries_names, self._subqueries = (
            NestedResolver.extract_subqueries(self._require_ast())
        )
        return self._subqueries

    @property
    def subqueries_names(self) -> list[str]:
        """Return the names of all subqueries (innermost first).

        Aliased subqueries use their alias; unaliased ones get
        auto-generated names (``subquery_1``, ``subquery_2``, …).

        :rtype: list[str]
        """
        if self._subqueries_names is not None:
            return self._subqueries_names
        self._subqueries_names, self._subqueries = (
            NestedResolver.extract_subqueries(self._require_ast())
        )
        return self._subqueries_names

    @staticmethod
    def _extract_int_from_node(node: Any) -> int | None:
        """Safely extract an integer value from a Limit or Offset node.

        :param node: A sqlglot AST node (typically ``exp.Limit`` or
            ``exp.Offset``), or ``None``.
        :type node: Any
        :returns: The integer value, or ``None`` if the node is absent or
            cannot be converted.
        :rtype: int | None
        """
        if not node:
            return None
        try:
            return int(node.expression.this)
        except (ValueError, AttributeError, TypeError):
            return None

    @property
    def limit_and_offset(self) -> tuple[int, int] | None:
        """Return the LIMIT and OFFSET values, if present.

        Extracts values from the AST first; when the AST has no Limit node
        (e.g. dialect-specific syntax), falls back to regex matching on the
        raw SQL.

        :rtype: tuple[int, int] | None
        """
        if self._limit_and_offset is not None:
            return self._limit_and_offset

        ast = self._ast_parser.ast
        if ast is None:
            return None

        select = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)
        if select is None:
            return None

        limit_val = self._extract_int_from_node(select.args.get("limit"))
        offset_val = self._extract_int_from_node(select.args.get("offset"))

        if limit_val is None:
            return self._extract_limit_regex()

        self._limit_and_offset = limit_val, offset_val or 0
        return self._limit_and_offset

    @property
    def values(self) -> list[Any]:
        """Return the list of literal values from INSERT/REPLACE queries.

        A single-row INSERT returns a flat list of values; a multi-row
        INSERT returns a list of lists (one per row).

        :rtype: list[Any]
        """
        if self._values is not None:
            return self._values
        self._values = self._extract_values()
        return self._values

    @property
    def values_dict(self) -> dict[str, Any] | None:
        """Return column-value pairs from INSERT/REPLACE queries.

        Maps each column name to its value (single-row) or list of values
        (multi-row).  When column names cannot be determined, auto-generated
        names (``column_1``, ``column_2``, …) are used as keys.

        :rtype: dict[str, Any] | None
        """
        values = self.values
        if self._values_dict is not None or not values:
            return self._values_dict
        columns = self.columns

        is_multi = values and isinstance(values[0], list)
        first_row = values[0] if is_multi else values
        if not columns:
            columns = [f"column_{ind + 1}" for ind in range(len(first_row))]

        if is_multi:
            self._values_dict = {
                col: [row[i] for row in values] for i, col in enumerate(columns)
            }
        else:
            self._values_dict = dict(zip(columns, values))
        return self._values_dict

    @property
    def comments(self) -> list[str]:
        """Return all comments from the SQL query.

        :rtype: list[str]
        """
        return extract_comments(self._raw_query)

    @property
    def without_comments(self) -> str:
        """Return the SQL with all comments removed.

        :rtype: str
        """
        return strip_comments(self._raw_query)

    @property
    def generalize(self) -> str:
        """Return a generalised (anonymised) version of the query.

        :rtype: str
        """
        return Generalizator(self._raw_query).generalize

    def _extract_values(self) -> list[Any]:
        """Extract literal values from INSERT/REPLACE query AST.

        :returns: A flat list for single-row inserts, a list of lists for
            multi-row inserts, or an empty list when no VALUES clause exists.
        :rtype: list[Any]
        """
        try:
            ast = self._ast_parser.ast
        except ValueError:
            return []

        if ast is None:
            return []

        values_node = ast.find(exp.Values)
        if not values_node:
            return []

        rows = []
        for tup in values_node.expressions:
            rows.append([self._convert_value(val) for val in tup.expressions])
        if len(rows) == 1:
            return rows[0]
        return rows

    @staticmethod
    def _convert_value(val: exp.Expression) -> int | float | str:
        """Convert a sqlglot literal AST node to a Python type.

        :param val: A sqlglot expression node (typically ``exp.Literal``
            or ``exp.Neg``).
        :type val: exp.Expression
        :returns: The Python int, float, or str representation.
        :rtype: int | float | str
        """
        if isinstance(val, exp.Literal):
            if val.is_int:
                return int(val.this)
            if val.is_number:
                return float(val.this)
            return str(val.this)
        if isinstance(val, exp.Neg):
            inner = val.this
            if isinstance(inner, exp.Literal):
                if inner.is_int:
                    return -int(inner.this)
                return -float(inner.this)
        return str(val)

    def _extract_limit_regex(self) -> tuple[int, int] | None:
        """Extract LIMIT and OFFSET using regex as a fallback.

        Handles both ``LIMIT n OFFSET m`` and MySQL-style ``LIMIT m, n``
        syntax.

        :returns: A ``(limit, offset)`` tuple, or ``None`` if no LIMIT
            clause is found.
        :rtype: tuple[int, int] | None
        """
        sql = strip_comments(self._raw_query)
        match = re.search(r"LIMIT\s+(\d+)\s*,\s*(\d+)", sql, re.IGNORECASE)
        if match:
            offset_val = int(match.group(1))
            limit_val = int(match.group(2))
            self._limit_and_offset = limit_val, offset_val
            return self._limit_and_offset

        match = re.search(
            r"LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?",
            sql,
            re.IGNORECASE,
        )
        if match:
            limit_val = int(match.group(1))
            offset_val = int(match.group(2)) if match.group(2) else 0
            self._limit_and_offset = limit_val, offset_val
            return self._limit_and_offset
        return None

    def _extract_columns_regex(self) -> list[str]:
        """Extract column names from ``INTO … (col1, col2)`` using regex.

        Used as a fallback when AST construction fails (e.g. malformed SQL
        that still contains an identifiable INSERT column list).

        :returns: Column names, or an empty list if no match is found.
        :rtype: list[str]
        """
        match = re.search(
            r"INTO\s+\S+\s*\(([^)]+)\)",
            self._raw_query,
            re.IGNORECASE,
        )
        if not match:
            return []
        cols = []
        for col in match.group(1).split(","):
            col = col.strip().strip("`").strip('"').strip("'")
            if col:
                cols.append(col)
        return cols

    def _resolve_column_alias(self, alias: str | list[str]) -> list[str]:
        """Recursively resolve a column alias to its underlying column(s).

        Delegates to :meth:`NestedResolver.resolve_column_alias` which
        follows alias chains through CTEs and subqueries.

        :param alias: The alias name or list of alias names to resolve.
        :type alias: str | list[str]
        :returns: The resolved column name(s).
        :rtype: list[str]
        """
        resolver = self._get_resolver()
        return resolver.resolve_column_alias(alias, self.columns_aliases)
