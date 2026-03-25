"""
This module provides SQL query parsing functions.

Thin facade over sqlglot AST-based extractors.
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple, Union

from sql_metadata._ast import ASTParser
from sql_metadata._bodies import extract_cte_bodies, extract_subquery_bodies
from sql_metadata._comments import extract_comments, strip_comments
from sql_metadata._extract import extract_all, extract_cte_names, extract_subquery_names
from sql_metadata._query_type import extract_query_type
from sql_metadata._tables import extract_table_aliases, extract_tables
from sql_metadata.token import tokenize
from sql_metadata.generalizator import Generalizator
from sql_metadata.utils import UniqueList, flatten_list


class Parser:  # pylint: disable=R0902
    """
    Main class to parse sql query
    """

    def __init__(self, sql: str = "", disable_logging: bool = False) -> None:
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
        """Returns preprocessed query"""
        return self._preprocess_query().replace("\n", " ").replace("  ", " ")

    def _preprocess_query(self) -> str:
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
        """Returns type of the query."""
        if self._query_type:
            return self._query_type
        try:
            ast = self._ast_parser.ast
        except ValueError:
            ast = None
        self._query_type = extract_query_type(ast, self._raw_query)
        return self._query_type

    @property
    def tokens(self) -> list:
        """Tokenizes the query and returns a linked list of SQLToken objects."""
        if self._tokens is not None:
            return self._tokens
        self._tokens = tokenize(self._raw_query)
        if self._tokens:
            _ = self.query_type
        return self._tokens

    @property
    def columns(self) -> List[str]:
        """Returns the list of columns this query refers to"""
        if self._columns is not None:
            return self._columns

        try:
            ast = self._ast_parser.ast
            qt = self.query_type
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
            columns, columns_dict, alias_names, alias_dict,
            alias_map, with_names, subquery_names,
        ) = extract_all(
            ast=ast,
            table_aliases=ta,
            query_type=qt,
            raw_query=self._raw_query,
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

    def _resolve_nested_columns(self) -> None:
        """Resolve columns that reference subqueries or CTEs."""
        resolved = UniqueList()
        for col in self._columns:
            result = self._resolve_sub_queries(col)
            if isinstance(result, list):
                resolved.extend(result)
            else:
                resolved.append(result)

        # Resolve bare column names through subquery/CTE aliases
        final = UniqueList()
        for col in resolved:
            if "." not in col:
                new_col = self._resolve_bare_through_nested(col)
                if new_col != col:
                    # Drop the bare reference — the resolved column is
                    # already in the list from the subquery/CTE body walk
                    # at its natural SQL-text position.
                    continue
            final.append(col)
        self._columns = final

        # Also resolve in columns_dict
        if self._columns_dict:
            for section, cols in list(self._columns_dict.items()):
                new_cols = UniqueList()
                for col in cols:
                    result = self._resolve_sub_queries(col)
                    if isinstance(result, list):
                        new_cols.extend(result)
                    else:
                        new_cols.append(result)
                final_cols = UniqueList()
                for c in new_cols:
                    if "." not in c:
                        new_c = self._resolve_bare_through_nested(c)
                        if new_c != c:
                            if isinstance(new_c, list):
                                final_cols.extend(new_c)
                            else:
                                final_cols.append(new_c)
                            continue
                    final_cols.append(c)
                self._columns_dict[section] = final_cols

    def _resolve_bare_through_nested(
        self, col_name: str
    ) -> Union[str, List[str]]:
        """Resolve a bare column name through subquery/CTE aliases."""
        for sq_name in self.subqueries_names:
            sq_def = self.subqueries.get(sq_name)
            if not sq_def:
                continue
            sq_parser = self._subqueries_parsers.setdefault(
                sq_name, Parser(sq_def)
            )
            if col_name in sq_parser.columns_aliases_names:
                resolved = sq_parser._resolve_column_alias(col_name)
                if self._columns_aliases is not None:
                    # Store immediate alias (one level), not fully resolved
                    immediate = sq_parser.columns_aliases.get(col_name, resolved)
                    self._columns_aliases[col_name] = immediate
                return resolved
            if col_name in sq_parser.columns:
                return col_name
        for cte_name in self.with_names:
            cte_def = self.with_queries.get(cte_name)
            if not cte_def:
                continue
            cte_parser = self._with_parsers.setdefault(
                cte_name, Parser(cte_def)
            )
            if col_name in cte_parser.columns_aliases_names:
                resolved = cte_parser._resolve_column_alias(col_name)
                if self._columns_aliases is not None:
                    immediate = cte_parser.columns_aliases.get(col_name, resolved)
                    self._columns_aliases[col_name] = immediate
                return resolved
        return col_name

    @property
    def columns_dict(self) -> Dict[str, List[str]]:
        """Returns dictionary of column names divided into section of the query."""
        if self._columns_dict is None:
            _ = self.columns
        # Resolve aliases used in other sections
        if self.columns_aliases_dict:
            for key, value in self.columns_aliases_dict.items():
                for alias in value:
                    resolved = self._resolve_column_alias(alias)
                    if isinstance(resolved, list):
                        for r in resolved:
                            self._columns_dict.setdefault(
                                key, UniqueList()
                            ).append(r)
                    else:
                        self._columns_dict.setdefault(
                            key, UniqueList()
                        ).append(resolved)
        return self._columns_dict

    @property
    def columns_aliases(self) -> Dict:
        """Returns a dictionary of column aliases with columns"""
        if self._columns_aliases is None:
            _ = self.columns
        return self._columns_aliases

    @property
    def columns_aliases_dict(self) -> Dict[str, List[str]]:
        """Returns dictionary of column alias names divided into sections."""
        if self._columns_aliases_dict is None:
            _ = self.columns
        return self._columns_aliases_dict

    @property
    def columns_aliases_names(self) -> List[str]:
        """Extract names of the column aliases used in query"""
        if self._columns_aliases_names is None:
            _ = self.columns
        return self._columns_aliases_names

    @property
    def tables(self) -> List[str]:
        """Return the list of tables this query refers to"""
        if self._tables is not None:
            return self._tables
        _ = self.query_type
        cte_names = set(self.with_names)
        for placeholder in self._ast_parser.cte_name_map:
            cte_names.add(placeholder)
        self._tables = extract_tables(
            self._ast_parser.ast, self._raw_query, cte_names,
            dialect=self._ast_parser.dialect,
        )
        return self._tables

    @property
    def limit_and_offset(self) -> Optional[Tuple[int, int]]:
        """Returns value for limit and offset if set"""
        if self._limit_and_offset is not None:
            return self._limit_and_offset

        from sqlglot import exp

        ast = self._ast_parser.ast
        if ast is None:
            return None

        select = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)
        if select is None:
            return None

        limit_node = select.args.get("limit")
        offset_node = select.args.get("offset")
        limit_val = None
        offset_val = None

        if limit_node:
            try:
                limit_val = int(limit_node.expression.this)
            except (ValueError, AttributeError):
                pass

        if offset_node:
            try:
                offset_val = int(offset_node.expression.this)
            except (ValueError, AttributeError):
                pass

        if limit_val is None:
            return self._extract_limit_regex()

        self._limit_and_offset = limit_val, offset_val or 0
        return self._limit_and_offset

    @property
    def tables_aliases(self) -> Dict[str, str]:
        """Returns tables aliases mapping from a given query"""
        if self._table_aliases is not None:
            return self._table_aliases
        self._table_aliases = extract_table_aliases(
            self._ast_parser.ast, self.tables
        )
        return self._table_aliases

    @property
    def with_names(self) -> List[str]:
        """Returns with statements aliases list from a given query"""
        if self._with_names is not None:
            return self._with_names
        self._with_names = extract_cte_names(
            self._ast_parser.ast, self._ast_parser.cte_name_map
        )
        return self._with_names

    @property
    def with_queries(self) -> Dict[str, str]:
        """Returns 'WITH' subqueries with names"""
        if self._with_queries is not None:
            return self._with_queries
        self._with_queries = extract_cte_bodies(
            self._raw_query, self.with_names
        )
        return self._with_queries

    @property
    def subqueries(self) -> Dict:
        """Returns a dictionary with all sub-queries existing in query"""
        if self._subqueries is not None:
            return self._subqueries
        self._subqueries = extract_subquery_bodies(
            self._raw_query, self.subqueries_names
        )
        return self._subqueries

    @property
    def subqueries_names(self) -> List[str]:
        """Returns sub-queries aliases list from a given query"""
        if self._subqueries_names is not None:
            return self._subqueries_names
        self._subqueries_names = extract_subquery_names(self._ast_parser.ast)
        return self._subqueries_names

    @property
    def values(self) -> List:
        """Returns list of values from insert queries"""
        if self._values:
            return self._values
        self._values = self._extract_values()
        return self._values

    @property
    def values_dict(self) -> Dict:
        """Returns dictionary of column-value pairs."""
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
        """Return comments from SQL query"""
        return extract_comments(self._raw_query)

    @property
    def without_comments(self) -> str:
        """Removes comments from SQL query"""
        return strip_comments(self._raw_query)

    @property
    def generalize(self) -> str:
        """Removes most variables from an SQL query and replaces them."""
        return Generalizator(self._raw_query).generalize

    def _extract_values(self) -> List:
        """Extract values from INSERT/REPLACE queries."""
        from sqlglot import exp

        try:
            ast = self._ast_parser.ast
        except ValueError:
            return self._extract_values_regex()

        if ast is None:
            return []

        if isinstance(ast, exp.Command):
            return self._extract_values_regex()

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

    def _extract_values_regex(self) -> List:
        upper = self._raw_query.upper()
        idx = upper.find("VALUES")
        if idx == -1:
            return []
        paren_start = self._raw_query.find("(", idx)
        if paren_start == -1:
            return []
        values = []
        i = paren_start + 1
        sql = self._raw_query
        current = []
        while i < len(sql):
            char = sql[i]
            if char == "'":
                j = i + 1
                while j < len(sql):
                    if sql[j] == "'" and (j + 1 >= len(sql) or sql[j + 1] != "'"):
                        break
                    j += 1
                values.append(sql[i + 1: j])
                i = j + 1
                current = []
            elif char == ",":
                val = "".join(current).strip()
                if val:
                    values.append(self._parse_value_string(val))
                current = []
                i += 1
            elif char == ")":
                val = "".join(current).strip()
                if val:
                    values.append(self._parse_value_string(val))
                break
            else:
                current.append(char)
                i += 1
        return values

    @staticmethod
    def _parse_value_string(val: str):
        try:
            return int(val)
        except ValueError:
            try:
                return float(val)
            except ValueError:
                return val

    def _extract_limit_regex(self) -> Optional[Tuple[int, int]]:
        sql = strip_comments(self._raw_query)
        match = re.search(
            r"LIMIT\s+(\d+)\s*,\s*(\d+)", sql, re.IGNORECASE
        )
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
        """Returns a column name for a given alias."""
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
        """Resolve column references from subqueries and CTEs."""
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
    def _resolve_nested_query(  # noqa: C901
        subquery_alias: str,
        nested_queries_names: List[str],
        nested_queries: Dict,
        already_parsed: Dict,
    ) -> Union[str, List[str]]:
        """Resolve subquery reference to the actual column."""
        parts = subquery_alias.split(".")
        if len(parts) != 2 or parts[0] not in nested_queries_names:
            return subquery_alias
        sub_query, column_name = parts[0], parts[-1]
        sub_query_definition = nested_queries.get(sub_query)
        if not sub_query_definition:
            return subquery_alias
        subparser = already_parsed.setdefault(
            sub_query, Parser(sub_query_definition)
        )
        if column_name in subparser.columns_aliases_names:
            resolved_column = subparser._resolve_column_alias(column_name)
            if isinstance(resolved_column, list):
                resolved_column = flatten_list(resolved_column)
                return resolved_column
            return [resolved_column]
        if column_name == "*":
            return subparser.columns
        try:
            column_index = [x.split(".")[-1] for x in subparser.columns].index(
                column_name
            )
        except ValueError:
            if "*" in subparser.columns:
                return column_name
            for table in subparser.tables:
                if f"{table}.*" in subparser.columns:
                    return column_name
            return subquery_alias
        resolved_column = subparser.columns[column_index]
        return [resolved_column]
