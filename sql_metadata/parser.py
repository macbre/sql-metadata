"""SQL query parsing facade.

Thin facade over the sqlglot AST-based extractors defined in
``_ast.py``, ``_tables.py``, ``_extract.py``, ``_bodies.py``, and
``_query_type.py``.  The :class:`Parser` class exposes every piece of
extracted metadata as a lazily-evaluated, cached property so that each
extraction runs at most once per instance.
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple, Union

from sql_metadata._ast import ASTParser
from sql_metadata._bodies import extract_cte_bodies, extract_subquery_bodies
from sql_metadata._comments import extract_comments, strip_comments
from sql_metadata._extract import extract_all, extract_cte_names, extract_subquery_names
from sql_metadata._query_type import extract_query_type
from sql_metadata.keywords_lists import QueryType
from sql_metadata._tables import extract_table_aliases, extract_tables
from sql_metadata.generalizator import Generalizator
from sql_metadata.utils import UniqueList, flatten_list


class Parser:  # pylint: disable=R0902
    """Parse a SQL query and extract metadata.

    The primary public interface of the ``sql-metadata`` library.  Given a
    raw SQL string, the parser lazily extracts tables, columns, aliases,
    CTE definitions, subqueries, values, comments, and more — each
    available as a cached property.

    All heavy work (AST construction, extraction walks) is deferred until
    the corresponding property is first accessed, and the result is cached
    for subsequent accesses.

    :param sql: The SQL query string to parse.
    :type sql: str
    :param disable_logging: If ``True``, suppress all log output from this
        parser instance.
    :type disable_logging: bool
    """

    def __init__(self, sql: str = "", disable_logging: bool = False) -> None:
        """Initialise the parser and prepare internal caches.

        No parsing or extraction happens at construction time — all work
        is deferred to property access.

        :param sql: Raw SQL query string.
        :type sql: str
        :param disable_logging: Suppress log output if ``True``.
        :type disable_logging: bool
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.disabled = disable_logging

        self._raw_query = sql
        self._query_type = None

        self._ast_parser = ASTParser(sql)

        self._tokens = None

        self._columns = None
        self._columns_dict = None
        self._columns_aliases_names = None
        self._columns_aliases = None
        self._columns_aliases_dict = None
        self._columns_with_tables_aliases = {}

        self._tables = None
        self._table_aliases = None

        self._with_names = None
        self._with_queries = None
        self._subqueries = None
        self._subqueries_names = None
        self._subqueries_parsers = {}
        self._with_parsers = {}

        self._limit_and_offset = None

        self._values = None
        self._values_dict = None

    @property
    def query(self) -> str:
        """Return the preprocessed SQL query.

        Applies quote normalisation (double-quotes → backticks inside
        non-string contexts) and collapses newlines/double-spaces.

        :returns: Preprocessed SQL string.
        :rtype: str
        """
        return self._preprocess_query().replace("\n", " ").replace("  ", " ")

    def _preprocess_query(self) -> str:
        """Normalise quoting in the raw query.

        Replaces double-quoted identifiers with backtick-quoted ones while
        preserving double-quotes that appear inside single-quoted strings.
        This ensures consistent quoting for downstream consumers.

        :returns: Quote-normalised SQL string, or ``""`` for empty input.
        :rtype: str
        """
        if self._raw_query == "":
            return ""

        def replace_quotes_in_string(match):
            return re.sub('"', "<!!__QUOTE__!!>", match.group())

        def replace_back_quotes_in_string(match):
            return re.sub("<!!__QUOTE__!!>", '"', match.group())

        query = re.sub(r"'.*?'", replace_quotes_in_string, self._raw_query)
        query = re.sub(r'"([^`]+?)"', r"`\1`", query)
        query = re.sub(r"'.*?'", replace_back_quotes_in_string, query)
        return query

    @property
    def query_type(self) -> str:
        """Return the type of the SQL query.

        Lazily determined from the AST root node type via
        :func:`extract_query_type`.  For ``REPLACE INTO`` queries that
        were rewritten to ``INSERT INTO`` during parsing, the type is
        restored to :attr:`QueryType.REPLACE`.

        :returns: A :class:`QueryType` enum value (e.g. ``"SELECT"``).
        :rtype: str
        :raises ValueError: If the query is empty or malformed.
        """
        if self._query_type:
            return self._query_type
        try:
            ast = self._ast_parser.ast
        except ValueError:
            ast = None
        self._query_type = extract_query_type(ast, self._raw_query)
        if self._query_type == QueryType.INSERT and self._ast_parser.is_replace:
            self._query_type = QueryType.REPLACE
        return self._query_type

    @property
    def tokens(self) -> List[str]:
        """Return the SQL as a list of token strings.

        Uses the sqlglot tokenizer to split the raw query into tokens,
        stripping backticks and double-quotes from identifiers.  Comments
        are not included (use :attr:`comments` for those).

        :returns: List of token text values.
        :rtype: List[str]
        """
        if self._tokens is not None:
            return self._tokens
        if not self._raw_query or not self._raw_query.strip():
            self._tokens = []
            return self._tokens
        from sql_metadata._comments import _choose_tokenizer

        try:
            sg_tokens = list(
                _choose_tokenizer(self._raw_query).tokenize(self._raw_query)
            )
        except Exception:
            sg_tokens = []
        self._tokens = [t.text.strip("`").strip('"') for t in sg_tokens]
        return self._tokens

    @property
    def columns(self) -> List[str]:
        """Return the list of column names referenced in the query.

        Lazily extracts columns via :func:`extract_all`, then resolves
        subquery/CTE column references via :meth:`_resolve_nested_columns`.
        Falls back to regex extraction for malformed queries that raise
        ``ValueError`` during AST construction.

        :returns: Ordered list of unique column names.
        :rtype: List[str]
        """
        if self._columns is not None:
            return self._columns

        try:
            ast = self._ast_parser.ast
            ta = self.tables_aliases
        except ValueError:
            cols = self._extract_columns_regex()
            self._columns = cols
            self._columns_dict = {}
            self._columns_aliases_names = []
            self._columns_aliases_dict = {}
            self._columns_aliases = {}
            return self._columns

        (
            columns,
            columns_dict,
            alias_names,
            alias_dict,
            alias_map,
            with_names,
            subquery_names,
        ) = extract_all(
            ast=ast,
            table_aliases=ta,
            cte_name_map=self._ast_parser.cte_name_map,
        )

        self._columns = columns
        self._columns_dict = columns_dict
        self._columns_aliases_names = alias_names
        self._columns_aliases_dict = alias_dict
        self._columns_aliases = alias_map if alias_map else {}

        # Cache CTE/subquery names from the same extraction
        if self._with_names is None:
            self._with_names = with_names
        if self._subqueries_names is None:
            self._subqueries_names = subquery_names

        # Resolve subquery/CTE column references
        self._resolve_nested_columns()

        return self._columns

    def _resolve_and_filter_columns(
        self, columns, drop_bare_aliases: bool = True
    ) -> "UniqueList":
        """Apply subquery/CTE resolution and bare-alias handling to a column list.

        Phase 1 replaces ``subquery.column`` references with the actual
        column from the nested definition.  Phase 2 handles bare column
        names that are aliases defined inside a nested query: when
        *drop_bare_aliases* is ``True`` the bare reference is dropped
        (the resolved column already appears elsewhere); when ``False``
        the resolved value replaces the bare reference in place.

        :param columns: Column names to process.
        :type columns: Iterable[str]
        :param drop_bare_aliases: If ``True``, drop bare aliases instead
            of replacing them.
        :type drop_bare_aliases: bool
        :returns: Processed column list.
        :rtype: UniqueList
        """
        resolved = UniqueList()
        for col in columns:
            result = self._resolve_sub_queries(col)
            if isinstance(result, list):
                resolved.extend(result)
            else:
                resolved.append(result)

        final = UniqueList()
        for col in resolved:
            if "." not in col:
                new_col = self._resolve_bare_through_nested(col)
                if new_col != col:
                    if not drop_bare_aliases:
                        if isinstance(new_col, list):
                            final.extend(new_col)
                        else:
                            final.append(new_col)
                    continue
            final.append(col)
        return final

    def _resolve_nested_columns(self) -> None:
        """Resolve columns that reference subqueries or CTEs.

        Two-phase resolution:

        1. Replace ``subquery.column`` references with the actual column
           from the subquery/CTE definition.
        2. Drop bare column names that are actually aliases defined inside
           a nested query — the resolved column already appears at its
           natural SQL-text position.

        Also applies the same resolution to :attr:`columns_dict`.

        :returns: Nothing — modifies ``self._columns`` and
            ``self._columns_dict`` in place.
        :rtype: None
        """
        self._columns = self._resolve_and_filter_columns(
            self._columns, drop_bare_aliases=True
        )

        if self._columns_dict:
            for section, cols in list(self._columns_dict.items()):
                self._columns_dict[section] = self._resolve_and_filter_columns(
                    cols, drop_bare_aliases=False
                )

    def _lookup_alias_in_nested(
        self,
        col_name: str,
        names: List[str],
        definitions: Dict,
        parser_cache: Dict,
        check_columns: bool = False,
    ):
        """Search for a bare column as an alias in a set of nested queries.

        Iterates through *names*, parses each definition (caching results
        in *parser_cache*), and checks whether *col_name* is a known alias.
        If found, resolves it and records the mapping in
        ``self._columns_aliases``.

        :param col_name: Column name to look up.
        :type col_name: str
        :param names: Ordered nested query names (subquery or CTE).
        :type names: List[str]
        :param definitions: Mapping of name → SQL body text.
        :type definitions: Dict[str, str]
        :param parser_cache: Mutable cache of name → Parser instances.
        :type parser_cache: Dict[str, Parser]
        :param check_columns: If ``True``, also return *col_name* unchanged
            when it appears in the parsed columns (subquery behaviour).
        :type check_columns: bool
        :returns: Resolved column name(s), or ``None`` if not found.
        :rtype: Optional[Union[str, List[str]]]
        """
        for nested_name in names:
            nested_def = definitions.get(nested_name)
            if not nested_def:
                continue
            nested_parser = parser_cache.setdefault(nested_name, Parser(nested_def))
            if col_name in nested_parser.columns_aliases_names:
                resolved = nested_parser._resolve_column_alias(col_name)
                if self._columns_aliases is not None:
                    immediate = nested_parser.columns_aliases.get(col_name, resolved)
                    self._columns_aliases[col_name] = immediate
                return resolved
            if check_columns and col_name in nested_parser.columns:
                return col_name
        return None

    def _resolve_bare_through_nested(self, col_name: str) -> Union[str, List[str]]:
        """Resolve a bare column name through subquery/CTE alias definitions.

        Checks whether *col_name* is defined as an alias inside any known
        subquery or CTE, and if so, resolves it to the underlying column.
        Also records the alias mapping in ``self._columns_aliases`` for
        downstream consumers.

        :param col_name: A column name without a table qualifier.
        :type col_name: str
        :returns: The resolved column name(s), or *col_name* unchanged.
        :rtype: Union[str, List[str]]
        """
        result = self._lookup_alias_in_nested(
            col_name,
            self.subqueries_names,
            self.subqueries,
            self._subqueries_parsers,
            check_columns=True,
        )
        if result is not None:
            return result
        result = self._lookup_alias_in_nested(
            col_name,
            self.with_names,
            self.with_queries,
            self._with_parsers,
        )
        if result is not None:
            return result
        return col_name

    @property
    def columns_dict(self) -> Dict[str, List[str]]:
        """Return column names organised by query section.

        Keys are section names like ``"select"``, ``"where"``, ``"join"``,
        ``"order_by"``, etc.  Values are :class:`UniqueList` instances.
        Alias references used in non-SELECT sections are resolved to their
        underlying column names and added to the appropriate section.

        :returns: Mapping of section name → column list.
        :rtype: Dict[str, List[str]]
        """
        if self._columns_dict is None:
            _ = self.columns
        # Resolve aliases used in other sections
        if self.columns_aliases_dict:
            for key, value in self.columns_aliases_dict.items():
                for alias in value:
                    resolved = self._resolve_column_alias(alias)
                    if isinstance(resolved, list):
                        for r in resolved:
                            self._columns_dict.setdefault(key, UniqueList()).append(r)
                    else:
                        self._columns_dict.setdefault(key, UniqueList()).append(
                            resolved
                        )
        return self._columns_dict

    @property
    def columns_aliases(self) -> Dict:
        """Return the alias-to-column mapping for column aliases.

        Keys are alias names, values are the column name(s) each alias
        refers to (a string for single-column aliases, a list for
        multi-column aliases).

        :returns: Alias mapping dictionary.
        :rtype: Dict[str, Union[str, list]]
        """
        if self._columns_aliases is None:
            _ = self.columns
        return self._columns_aliases

    @property
    def columns_aliases_dict(self) -> Dict[str, List[str]]:
        """Return column alias names organised by query section.

        Similar to :attr:`columns_dict` but for alias names rather than
        column names.  Used by :attr:`columns_dict` to resolve aliases
        that appear in non-SELECT sections (e.g. ``ORDER BY alias``).

        :returns: Mapping of section name → alias name list.
        :rtype: Dict[str, List[str]]
        """
        if self._columns_aliases_dict is None:
            _ = self.columns
        return self._columns_aliases_dict

    @property
    def columns_aliases_names(self) -> List[str]:
        """Return the names of all column aliases used in the query.

        :returns: Ordered list of alias names.
        :rtype: List[str]
        """
        if self._columns_aliases_names is None:
            _ = self.columns
        return self._columns_aliases_names

    @property
    def tables(self) -> List[str]:
        """Return the list of table names referenced in the query.

        Tables are extracted from the AST via :func:`extract_tables`,
        excluding CTE names.  Results are sorted by their first occurrence
        in the raw SQL (left-to-right order).

        :returns: Ordered list of unique table names.
        :rtype: List[str]
        :raises ValueError: If the query is malformed.
        """
        if self._tables is not None:
            return self._tables
        _ = self.query_type
        cte_names = set(self.with_names)
        for placeholder in self._ast_parser.cte_name_map:
            cte_names.add(placeholder)
        self._tables = extract_tables(
            self._ast_parser.ast,
            self._raw_query,
            cte_names,
            dialect=self._ast_parser.dialect,
        )
        return self._tables

    @staticmethod
    def _extract_int_from_node(node) -> Optional[int]:
        """Safely extract an integer value from a ``Limit`` or ``Offset`` node.

        :param node: An AST node whose ``expression.this`` holds the value.
        :returns: The integer value, or ``None`` on failure.
        :rtype: Optional[int]
        """
        if not node:
            return None
        try:
            return int(node.expression.this)
        except (ValueError, AttributeError):
            return None

    @property
    def limit_and_offset(self) -> Optional[Tuple[int, int]]:
        """Return the ``LIMIT`` and ``OFFSET`` values, if present.

        Extracts values from the AST's ``limit`` and ``offset`` nodes.
        Falls back to regex extraction for non-standard syntax (e.g.
        ``LIMIT offset, count``).

        :returns: A ``(limit, offset)`` tuple, or ``None`` if not set.
        :rtype: Optional[Tuple[int, int]]
        """
        if self._limit_and_offset is not None:
            return self._limit_and_offset

        from sqlglot import exp

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
    def tables_aliases(self) -> Dict[str, str]:
        """Return the table alias mapping for this query.

        :returns: Dictionary mapping alias names to real table names.
        :rtype: Dict[str, str]
        """
        if self._table_aliases is not None:
            return self._table_aliases
        self._table_aliases = extract_table_aliases(self._ast_parser.ast, self.tables)
        return self._table_aliases

    @property
    def with_names(self) -> List[str]:
        """Return the CTE (Common Table Expression) names from the query.

        :returns: Ordered list of CTE alias names.
        :rtype: List[str]
        """
        if self._with_names is not None:
            return self._with_names
        self._with_names = extract_cte_names(
            self._ast_parser.ast, self._ast_parser.cte_name_map
        )
        return self._with_names

    @property
    def with_queries(self) -> Dict[str, str]:
        """Return the SQL body for each CTE defined in the query.

        Keys are CTE names, values are the SQL text inside the ``AS (...)``
        parentheses, with original casing preserved.

        :returns: Mapping of CTE name → body SQL.
        :rtype: Dict[str, str]
        """
        if self._with_queries is not None:
            return self._with_queries
        self._with_queries = extract_cte_bodies(
            self._ast_parser.ast,
            self._raw_query,
            self.with_names,
            self._ast_parser.cte_name_map,
        )
        return self._with_queries

    @property
    def subqueries(self) -> Dict:
        """Return the SQL body for each aliased subquery in the query.

        Keys are subquery alias names, values are the SQL text inside
        the parentheses, with original casing preserved.

        :returns: Mapping of subquery name → body SQL.
        :rtype: Dict[str, str]
        """
        if self._subqueries is not None:
            return self._subqueries
        self._subqueries = extract_subquery_bodies(
            self._ast_parser.ast, self._raw_query, self.subqueries_names
        )
        return self._subqueries

    @property
    def subqueries_names(self) -> List[str]:
        """Return the alias names of all subqueries in the query.

        Subqueries are returned in post-order (innermost first), which is
        the order needed for correct column resolution.

        :returns: Ordered list of subquery alias names.
        :rtype: List[str]
        """
        if self._subqueries_names is not None:
            return self._subqueries_names
        self._subqueries_names = extract_subquery_names(self._ast_parser.ast)
        return self._subqueries_names

    @property
    def values(self) -> List:
        """Return the list of literal values from ``INSERT``/``REPLACE`` queries.

        Values are extracted from the AST's ``Values`` / ``Tuple`` nodes
        and converted to Python types (``int``, ``float``, or ``str``).

        :returns: Flat list of values in insertion order.
        :rtype: List[Union[int, float, str]]
        """
        if self._values:
            return self._values
        self._values = self._extract_values()
        return self._values

    @property
    def values_dict(self) -> Dict:
        """Return column-value pairs from ``INSERT``/``REPLACE`` queries.

        Pairs each value from :attr:`values` with its corresponding column
        name from :attr:`columns`.  If column names are not available,
        generates placeholder names (``column_1``, ``column_2``, ...).

        :returns: Mapping of column name → value.
        :rtype: Dict[str, Union[int, float, str]]
        """
        values = self.values
        if self._values_dict or not values:
            return self._values_dict
        try:
            columns = self.columns
        except ValueError:
            columns = []
        if not columns:
            columns = [f"column_{ind + 1}" for ind in range(len(values))]
        self._values_dict = dict(zip(columns, values))
        return self._values_dict

    @property
    def comments(self) -> List[str]:
        """Return all comments from the SQL query.

        Comments are returned with their delimiters preserved (``--``,
        ``/* */``, ``#``).

        :returns: List of comment strings in source order.
        :rtype: List[str]
        """
        return extract_comments(self._raw_query)

    @property
    def without_comments(self) -> str:
        """Return the SQL with all comments removed.

        :returns: Comment-free SQL with normalised whitespace.
        :rtype: str
        """
        return strip_comments(self._raw_query)

    @property
    def generalize(self) -> str:
        """Return a generalised (anonymised) version of the query.

        Replaces literals with placeholders (``X``, ``N``) and collapses
        multi-value lists.  See :class:`Generalizator` for details.

        :returns: Generalised SQL string.
        :rtype: str
        """
        return Generalizator(self._raw_query).generalize

    def _extract_values(self) -> List:
        """Extract literal values from ``INSERT``/``REPLACE`` query AST.

        Finds the ``exp.Values`` node, iterates its ``Tuple`` children,
        and converts each literal to a Python type via :meth:`_convert_value`.

        :returns: Flat list of values.
        :rtype: List[Union[int, float, str]]
        """
        from sqlglot import exp

        try:
            ast = self._ast_parser.ast
        except ValueError:
            return []

        if ast is None:
            return []

        values_node = ast.find(exp.Values)
        if not values_node:
            return []

        values = []
        for tup in values_node.expressions:
            if isinstance(tup, exp.Tuple):
                for val in tup.expressions:
                    values.append(self._convert_value(val))
            else:
                values.append(self._convert_value(tup))
        return values

    @staticmethod
    def _convert_value(val) -> Union[int, float, str]:
        """Convert a sqlglot literal AST node to a Python type.

        Handles ``exp.Literal`` (integer, float, string) and ``exp.Neg``
        (negative numbers).  Falls back to ``str(val)`` for unrecognised
        node types.

        :param val: sqlglot expression node representing a value.
        :type val: exp.Expression
        :returns: The value as ``int``, ``float``, or ``str``.
        :rtype: Union[int, float, str]
        """
        from sqlglot import exp

        if isinstance(val, exp.Literal):
            if val.is_int:
                return int(val.this)
            if val.is_number:
                return float(val.this)
            return val.this
        if isinstance(val, exp.Neg):
            inner = val.this
            if isinstance(inner, exp.Literal):
                if inner.is_int:
                    return -int(inner.this)
                return -float(inner.this)
        return str(val)

    def _extract_limit_regex(self) -> Optional[Tuple[int, int]]:
        """Extract ``LIMIT`` and ``OFFSET`` using regex as a fallback.

        Handles both ``LIMIT count OFFSET offset`` and the MySQL-style
        ``LIMIT offset, count`` syntax.

        :returns: A ``(limit, offset)`` tuple, or ``None`` if not found.
        :rtype: Optional[Tuple[int, int]]
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

    def _extract_columns_regex(self) -> List[str]:
        """Extract column names from ``INTO ... (col1, col2)`` using regex.

        Fallback for malformed queries where AST construction fails.
        Parses the column list inside parentheses after ``INTO table_name``.

        :returns: List of column names, or ``[]`` if not found.
        :rtype: List[str]
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

    def _resolve_column_alias(
        self, alias: Union[str, List[str]], visited: Set = None
    ) -> Union[str, List]:
        """Recursively resolve a column alias to its underlying column(s).

        Follows the alias chain in :attr:`columns_aliases` until reaching
        a name that is not itself an alias.  Tracks *visited* names to
        prevent infinite loops on circular aliases.

        :param alias: Alias name or list of alias names to resolve.
        :type alias: Union[str, List[str]]
        :param visited: Set of already-visited aliases (cycle detection).
        :type visited: Optional[Set]
        :returns: The resolved column name(s).
        :rtype: Union[str, List]
        """
        visited = visited or set()
        if isinstance(alias, list):
            return [self._resolve_column_alias(x, visited) for x in alias]
        while alias in self.columns_aliases and alias not in visited:
            visited.add(alias)
            alias = self.columns_aliases[alias]
            if isinstance(alias, list):
                return self._resolve_column_alias(alias, visited)
        return alias

    def _resolve_sub_queries(self, column: str) -> Union[str, List[str]]:
        """Resolve a ``subquery.column`` reference to the actual column(s).

        First tries subquery definitions, then CTE definitions.  Delegates
        to :meth:`_resolve_nested_query` for each attempt.

        :param column: Column name, possibly prefixed with a subquery/CTE
            alias (e.g. ``"sq.id"``).
        :type column: str
        :returns: Resolved column name(s).
        :rtype: Union[str, List[str]]
        """
        result = self._resolve_nested_query(
            subquery_alias=column,
            nested_queries_names=self.subqueries_names,
            nested_queries=self.subqueries,
            already_parsed=self._subqueries_parsers,
        )
        if isinstance(result, str):
            result = self._resolve_nested_query(
                subquery_alias=result,
                nested_queries_names=self.with_names,
                nested_queries=self.with_queries,
                already_parsed=self._with_parsers,
            )
        return result if isinstance(result, list) else [result]

    @staticmethod
    def _find_column_fallback(
        column_name: str, subparser: "Parser", original_ref: str
    ) -> Union[str, List[str]]:
        """Find a column by name in the subparser with wildcard fallbacks.

        Tries index-based lookup first.  If not found, checks for
        wildcard columns (``*`` or ``table.*``) that could cover the
        reference.

        :param column_name: Unqualified column name to find.
        :type column_name: str
        :param subparser: Parser instance for the nested query body.
        :type subparser: Parser
        :param original_ref: Original ``prefix.column`` reference.
        :type original_ref: str
        :returns: Resolved column(s), or *original_ref* if not found.
        :rtype: Union[str, List[str]]
        """
        try:
            idx = [x.split(".")[-1] for x in subparser.columns].index(column_name)
        except ValueError:
            if "*" in subparser.columns:
                return column_name
            for table in subparser.tables:
                if f"{table}.*" in subparser.columns:
                    return column_name
            return original_ref
        return [subparser.columns[idx]]

    @staticmethod
    def _resolve_column_in_subparser(
        column_name: str, subparser: "Parser", original_ref: str
    ) -> Union[str, List[str]]:
        """Resolve a column name through a parsed nested query.

        Checks aliases, wildcards (``*``), and index-based column mapping
        in *subparser*.  Returns *original_ref* unchanged if the column
        cannot be resolved.

        :param column_name: The column part of a ``prefix.column`` reference.
        :type column_name: str
        :param subparser: Parser instance for the nested query body.
        :type subparser: Parser
        :param original_ref: The full ``prefix.column`` string, returned
            as a fallback when resolution fails.
        :type original_ref: str
        :returns: Resolved column name(s), or *original_ref*.
        :rtype: Union[str, List[str]]
        """
        if column_name in subparser.columns_aliases_names:
            resolved = subparser._resolve_column_alias(column_name)
            if isinstance(resolved, list):
                return flatten_list(resolved)
            return [resolved]
        if column_name == "*":
            return subparser.columns
        return Parser._find_column_fallback(column_name, subparser, original_ref)

    @staticmethod
    def _resolve_nested_query(
        subquery_alias: str,
        nested_queries_names: List[str],
        nested_queries: Dict,
        already_parsed: Dict,
    ) -> Union[str, List[str]]:
        """Resolve a ``prefix.column`` reference through a nested query.

        Splits *subquery_alias* on ``.``, checks whether the prefix
        matches a known nested query name, then parses that query (caching
        the :class:`Parser` instance in *already_parsed*) to find the
        actual column.  Handles alias resolution, wildcard expansion
        (``prefix.*``), and index-based column mapping.

        :param subquery_alias: Column reference like ``"sq.column_name"``.
        :type subquery_alias: str
        :param nested_queries_names: Known subquery/CTE names.
        :type nested_queries_names: List[str]
        :param nested_queries: Mapping of name → SQL body text.
        :type nested_queries: Dict[str, str]
        :param already_parsed: Cache of name → :class:`Parser` instances.
        :type already_parsed: Dict[str, Parser]
        :returns: Resolved column name(s), or the input unchanged if
            the prefix is not a known nested query.
        :rtype: Union[str, List[str]]
        """
        parts = subquery_alias.split(".")
        if len(parts) != 2 or parts[0] not in nested_queries_names:
            return subquery_alias
        sub_query, column_name = parts[0], parts[-1]
        sub_query_definition = nested_queries.get(sub_query)
        if not sub_query_definition:
            return subquery_alias
        subparser = already_parsed.setdefault(sub_query, Parser(sub_query_definition))
        return Parser._resolve_column_in_subparser(
            column_name, subparser, subquery_alias
        )
