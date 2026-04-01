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
from typing import Any, Dict, List, Optional, Tuple, Union

from sqlglot import exp

from sql_metadata.ast_parser import ASTParser
from sql_metadata.column_extractor import ColumnExtractor
from sql_metadata.comments import extract_comments, strip_comments
from sql_metadata.generalizator import Generalizator
from sql_metadata.keywords_lists import QueryType
from sql_metadata.nested_resolver import NestedResolver
from sql_metadata.query_type_extractor import QueryTypeExtractor
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
        self._query_type: Optional[str] = None

        self._ast_parser = ASTParser(sql)
        self._resolver: Optional[NestedResolver] = None

        self._tokens: Optional[List[str]] = None

        self._columns: Optional[UniqueList] = None
        self._columns_dict: Optional[Dict[str, UniqueList]] = None
        self._columns_aliases_names: Optional[UniqueList] = None
        self._columns_aliases: Optional[Dict[str, Union[str, list]]] = None
        self._columns_aliases_dict: Optional[Dict[str, UniqueList]] = None
        self._columns_with_tables_aliases: Dict[str, str] = {}

        self._tables: Optional[List[str]] = None
        self._table_aliases: Optional[Dict[str, str]] = None

        self._with_names: Optional[List[str]] = None
        self._with_queries: Optional[Dict[str, str]] = None
        self._subqueries: Optional[Dict[str, str]] = None
        self._subqueries_names: Optional[List[str]] = None

        self._limit_and_offset: Optional[Tuple[int, int]] = None

        self._output_columns: Optional[list] = None

        self._values: Optional[List] = None
        self._values_dict: Optional[Dict[str, Union[int, float, str, list]]] = None

    # -------------------------------------------------------------------
    # NestedResolver access
    # -------------------------------------------------------------------

    def _get_resolver(self) -> NestedResolver:
        """Return (and cache) the NestedResolver instance."""
        if self._resolver is None:
            self._resolver = NestedResolver(
                self._ast_parser.ast,
                self._ast_parser.cte_name_map,
            )
        return self._resolver

    # -------------------------------------------------------------------
    # Query preprocessing
    # -------------------------------------------------------------------

    @property
    def query(self) -> str:
        """Return the preprocessed SQL query."""
        return self._preprocess_query().replace("\n", " ").replace("  ", " ")

    def _preprocess_query(self) -> str:
        """Normalise quoting in the raw query."""
        if self._raw_query == "":
            return ""

        def replace_quotes_in_string(match: re.Match[str]) -> str:
            return re.sub('"', "<!!__QUOTE__!!>", match.group())

        def replace_back_quotes_in_string(match: re.Match[str]) -> str:
            return re.sub("<!!__QUOTE__!!>", '"', match.group())

        query = re.sub(r"'.*?'", replace_quotes_in_string, self._raw_query)
        query = re.sub(r'"([^`]+?)"', r"`\1`", query)
        query = re.sub(r"'.*?'", replace_back_quotes_in_string, query)
        return query

    # -------------------------------------------------------------------
    # Query type
    # -------------------------------------------------------------------

    @property
    def query_type(self) -> str:
        """Return the type of the SQL query."""
        if self._query_type:
            return self._query_type
        try:
            ast = self._ast_parser.ast
        except ValueError:
            ast = None
        self._query_type = QueryTypeExtractor(ast, self._raw_query).extract()
        if self._query_type == QueryType.INSERT and self._ast_parser.is_replace:
            self._query_type = QueryType.REPLACE
        return self._query_type

    # -------------------------------------------------------------------
    # Tokens
    # -------------------------------------------------------------------

    @property
    def tokens(self) -> List[str]:
        """Return the SQL as a list of token strings."""
        if self._tokens is not None:
            return self._tokens
        if not self._raw_query or not self._raw_query.strip():
            self._tokens = []
            return self._tokens
        from sql_metadata.comments import _choose_tokenizer

        try:
            sg_tokens = list(
                _choose_tokenizer(self._raw_query).tokenize(self._raw_query)
            )
        # TODO: revisit if sqlglot tokenizer starts raising on specific inputs
        except Exception:  # pragma: no cover
            sg_tokens = []
        self._tokens = [t.text.strip("`").strip('"') for t in sg_tokens]
        return self._tokens

    # -------------------------------------------------------------------
    # Columns
    # -------------------------------------------------------------------

    @property
    def columns(self) -> list:
        """Return the list of column names referenced in the query."""
        if self._columns is not None:
            return self._columns

        try:
            ast = self._ast_parser.ast
            ta = self.tables_aliases
        except ValueError:
            cols = self._extract_columns_regex()
            self._columns = UniqueList(cols)
            self._columns_dict = {}
            self._columns_aliases_names = UniqueList()
            self._columns_aliases_dict = {}
            self._columns_aliases = {}
            self._output_columns = []
            return self._columns

        if ast is None:  # pragma: no cover — tables_aliases raises for None ast
            self._columns = UniqueList()
            self._columns_dict = {}
            self._columns_aliases_names = UniqueList()
            self._columns_aliases_dict = {}
            self._columns_aliases = {}
            self._output_columns = []
            return self._columns

        extractor = ColumnExtractor(ast, ta, self._ast_parser.cte_name_map)
        result = extractor.extract()

        self._columns = result.columns
        self._columns_dict = result.columns_dict
        self._columns_aliases_names = result.alias_names
        self._columns_aliases_dict = result.alias_dict
        self._columns_aliases = result.alias_map if result.alias_map else {}
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
    def columns_dict(self) -> dict:
        """Return column names organised by query section."""
        if self._columns_dict is None:
            _ = self.columns
        assert self._columns_dict is not None
        # Resolve aliases used in other sections
        if self.columns_aliases_dict:
            resolver = self._get_resolver()
            for key, value in self.columns_aliases_dict.items():
                for alias in value:
                    resolved = resolver.resolve_column_alias(
                        alias, self.columns_aliases
                    )
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
        """Return the alias-to-column mapping for column aliases."""
        if self._columns_aliases is None:
            _ = self.columns
        assert self._columns_aliases is not None
        return self._columns_aliases

    @property
    def columns_aliases_dict(self) -> Optional[dict]:
        """Return column alias names organised by query section."""
        if self._columns_aliases_dict is None:
            _ = self.columns
        return self._columns_aliases_dict

    @property
    def columns_aliases_names(self) -> List[str]:
        """Return the names of all column aliases used in the query."""
        if self._columns_aliases_names is None:
            _ = self.columns
        assert self._columns_aliases_names is not None
        return self._columns_aliases_names

    @property
    def output_columns(self) -> list:
        """Return the ordered list of SELECT output column names.

        Combines real columns and aliases in their original position.
        For example, ``SELECT a, b AS c FROM t`` returns ``["a", "c"]``.
        """
        if self._output_columns is None:
            _ = self.columns
        assert self._output_columns is not None
        return self._output_columns

    # -------------------------------------------------------------------
    # Tables
    # -------------------------------------------------------------------

    @property
    def tables(self) -> List[str]:
        """Return the list of table names referenced in the query."""
        if self._tables is not None:
            return self._tables
        _ = self.query_type
        cte_names = set(self.with_names)
        for placeholder in self._ast_parser.cte_name_map:
            cte_names.add(placeholder)
        extractor = TableExtractor(
            self._ast_parser.ast,
            self._raw_query,
            cte_names,
            dialect=self._ast_parser.dialect,
        )
        self._tables = extractor.extract()
        return self._tables

    @property
    def tables_aliases(self) -> Dict[str, str]:
        """Return the table alias mapping for this query."""
        if self._table_aliases is not None:
            return self._table_aliases
        extractor = TableExtractor(self._ast_parser.ast)
        self._table_aliases = extractor.extract_aliases(self.tables)
        return self._table_aliases

    # -------------------------------------------------------------------
    # CTEs and subqueries
    # -------------------------------------------------------------------

    @property
    def with_names(self) -> List[str]:
        """Return the CTE (Common Table Expression) names from the query."""
        if self._with_names is not None:
            return self._with_names
        resolver = self._get_resolver()
        self._with_names = resolver.extract_cte_names(
            self._ast_parser.cte_name_map
        )
        return self._with_names

    @property
    def with_queries(self) -> Dict[str, str]:
        """Return the SQL body for each CTE defined in the query."""
        if self._with_queries is not None:
            return self._with_queries
        resolver = self._get_resolver()
        self._with_queries = resolver.extract_cte_bodies(self.with_names)
        return self._with_queries

    @property
    def subqueries(self) -> Dict:
        """Return the SQL body for each subquery in the query."""
        if self._subqueries is not None:
            return self._subqueries
        self._subqueries_names, self._subqueries = (
            NestedResolver.extract_subqueries(self._ast_parser.ast)
        )
        return self._subqueries

    @property
    def subqueries_names(self) -> List[str]:
        """Return the names of all subqueries (innermost first).

        Aliased subqueries use their alias; unaliased ones get
        auto-generated names (``subquery_1``, ``subquery_2``, …).
        """
        if self._subqueries_names is not None:
            return self._subqueries_names
        self._subqueries_names, self._subqueries = (
            NestedResolver.extract_subqueries(self._ast_parser.ast)
        )
        return self._subqueries_names

    # -------------------------------------------------------------------
    # Limit, offset, values
    # -------------------------------------------------------------------

    @staticmethod
    def _extract_int_from_node(node: Any) -> Optional[int]:
        """Safely extract an integer value from a Limit or Offset node."""
        if not node:
            return None
        try:
            return int(node.expression.this)
        except (ValueError, AttributeError, TypeError):
            return None

    @property
    def limit_and_offset(self) -> Optional[Tuple[int, int]]:
        """Return the LIMIT and OFFSET values, if present."""
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
    def values(self) -> List:
        """Return the list of literal values from INSERT/REPLACE queries."""
        if self._values:
            return self._values
        self._values = self._extract_values()
        return self._values

    @property
    def values_dict(self) -> Optional[Dict]:
        """Return column-value pairs from INSERT/REPLACE queries."""
        values = self.values
        if self._values_dict or not values:
            return self._values_dict
        try:
            columns = self.columns
        # TODO: revisit if .columns starts propagating ValueError to callers
        except ValueError:  # pragma: no cover
            columns = []

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

    # -------------------------------------------------------------------
    # Comments and generalization
    # -------------------------------------------------------------------

    @property
    def comments(self) -> List[str]:
        """Return all comments from the SQL query."""
        return extract_comments(self._raw_query)

    @property
    def without_comments(self) -> str:
        """Return the SQL with all comments removed."""
        return strip_comments(self._raw_query)

    @property
    def generalize(self) -> str:
        """Return a generalised (anonymised) version of the query."""
        return Generalizator(self._raw_query).generalize

    # -------------------------------------------------------------------
    # Internal extraction helpers
    # -------------------------------------------------------------------

    def _extract_values(self) -> List:
        """Extract literal values from INSERT/REPLACE query AST."""
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

        rows = []
        for tup in values_node.expressions:
            if isinstance(tup, exp.Tuple):
                rows.append([self._convert_value(val) for val in tup.expressions])
            # TODO: revisit if sqlglot stops wrapping VALUES items in Tuple
            else:  # pragma: no cover
                rows.append([self._convert_value(tup)])
        if len(rows) == 1:
            return rows[0]
        return rows

    @staticmethod
    def _convert_value(val: exp.Expression) -> Union[int, float, str]:
        """Convert a sqlglot literal AST node to a Python type."""
        from sqlglot import exp

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

    def _extract_limit_regex(self) -> Optional[Tuple[int, int]]:
        """Extract LIMIT and OFFSET using regex as a fallback."""
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
        """Extract column names from ``INTO ... (col1, col2)`` using regex."""
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

    def _resolve_column_alias(self, alias: Union[str, List[str]]) -> Union[str, List]:
        """Recursively resolve a column alias (delegates to NestedResolver)."""
        resolver = self._get_resolver()
        return resolver.resolve_column_alias(alias, self.columns_aliases)
