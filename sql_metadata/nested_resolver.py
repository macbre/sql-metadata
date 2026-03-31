"""Nested column resolution and CTE/subquery body extraction.

The :class:`NestedResolver` class owns the complete "look inside nested
queries" concern: rendering CTE/subquery AST nodes back to SQL, parsing
those bodies with sub-:class:`Parser` instances, and resolving
``subquery.column`` references to actual columns.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

if TYPE_CHECKING:
    from sql_metadata.parser import Parser

from sqlglot import exp
from sqlglot.generator import Generator

from sql_metadata.utils import (
    DOT_PLACEHOLDER,
    UniqueList,
    _make_reverse_cte_map,
    flatten_list,
    last_segment,
)

# ---------------------------------------------------------------------------
# Custom SQL generator — preserves function signatures
# ---------------------------------------------------------------------------


class _PreservingGenerator(Generator):
    """Custom SQL generator that preserves function signatures.

    sqlglot normalises certain functions when rendering SQL (e.g.
    ``IFNULL`` → ``COALESCE``, ``DIV`` → ``CAST(… / … AS INT)``).
    This generator overrides those transformations so that the output
    only differs from the input in keyword/function-name casing and
    explicit ``AS`` insertion.
    """

    TRANSFORMS = {
        **Generator.TRANSFORMS,
        exp.CurrentDate: lambda self, e: "CURRENT_DATE()",
        exp.IntDiv: lambda self, e: (
            f"{self.sql(e, 'this')} DIV {self.sql(e, 'expression')}"
        ),
    }

    def coalesce_sql(self, expression: exp.Expression) -> str:
        args = [expression.this] + expression.expressions
        if len(args) == 2:
            return f"IFNULL({self.sql(args[0])}, {self.sql(args[1])})"
        return super().coalesce_sql(expression)  # type: ignore[misc, no-any-return]

    def dateadd_sql(self, expression: exp.Expression) -> str:
        return (
            f"DATE_ADD({self.sql(expression, 'this')}, "
            f"{self.sql(expression, 'expression')})"
        )

    def datesub_sql(self, expression: exp.Expression) -> str:
        return (
            f"DATE_SUB({self.sql(expression, 'this')}, "
            f"{self.sql(expression, 'expression')})"
        )

    def tsordsadd_sql(self, expression: exp.Expression) -> str:
        this = self.sql(expression, "this")
        expr_node = expression.expression
        if isinstance(expr_node, exp.Mul):
            right = expr_node.expression
            if (
                isinstance(right, exp.Neg)
                and isinstance(right.this, exp.Literal)
                and right.this.this == "1"
            ):
                left = self.sql(expr_node, "this")
                return f"DATE_SUB({this}, {left})"
        return f"DATE_ADD({this}, {self.sql(expression, 'expression')})"

    def not_sql(self, expression: exp.Expression) -> str:
        child = expression.this
        if isinstance(child, exp.Is) and isinstance(child.expression, exp.Null):
            return f"{self.sql(child, 'this')} IS NOT NULL"
        if isinstance(child, exp.In):
            return f"{self.sql(child, 'this')} NOT IN ({self.expressions(child)})"
        return super().not_sql(expression)  # type: ignore[arg-type, no-any-return]


_GENERATOR = _PreservingGenerator()


# ---------------------------------------------------------------------------
# NestedResolver class
# ---------------------------------------------------------------------------


class NestedResolver:
    """Resolve column references through subqueries and CTEs.

    Owns the complete lifecycle of nested query resolution:

    1. **Body extraction** — render CTE/subquery AST nodes back to SQL
       via :class:`_PreservingGenerator`.
    2. **Column resolution** — parse bodies with sub-Parsers and resolve
       ``subquery.column`` references to actual columns.
    3. **Bare alias resolution** — detect column names that are actually
       aliases defined inside nested queries.

    :param ast: Root AST node (for body extraction).
    :param cte_name_map: Placeholder → original qualified CTE name mapping.
    """

    def __init__(
        self,
        ast: Optional[exp.Expression],
        cte_name_map: Optional[dict] = None,
    ):
        self._ast = ast
        self._cte_name_map = cte_name_map or {}

        # Lazy caches
        self._subqueries_parsers: Dict = {}
        self._with_parsers: Dict = {}
        self._columns_aliases: Dict = {}
        self._cached_cte_nodes: Optional[list] = None

        # Set by resolve() caller
        self._subqueries_names: List[str] = []
        self._subqueries: Dict = {}
        self._with_names: List[str] = []
        self._with_queries: Dict = {}

    # -------------------------------------------------------------------
    # Name extraction (CTE and subquery names from the AST)
    # -------------------------------------------------------------------

    def _cte_nodes(self) -> list:
        """Return all ``exp.CTE`` nodes from the AST (cached)."""
        if self._cached_cte_nodes is None:
            if self._ast is None:
                self._cached_cte_nodes = []
            else:
                self._cached_cte_nodes = list(self._ast.find_all(exp.CTE))
        return self._cached_cte_nodes

    def extract_cte_names(
        self,
        cte_name_map: Optional[Dict] = None,
    ) -> List[str]:
        """Extract CTE names from the AST.

        Called by :attr:`Parser.with_names`.
        """
        if self._ast is None:
            return []
        cte_name_map = cte_name_map or {}
        reverse_map = _make_reverse_cte_map(cte_name_map)
        names = UniqueList()
        for cte in self._cte_nodes():
            alias = cte.alias
            if alias:
                names.append(reverse_map.get(alias, alias))
        return names

    @staticmethod
    def extract_subquery_names(ast: Optional[exp.Expression]) -> List[str]:
        """Extract aliased subquery names from the AST in post-order.

        Called by :attr:`Parser.subqueries_names`.
        """
        if ast is None:
            return []
        names = UniqueList()
        NestedResolver._collect_subquery_names_postorder(ast, names)
        return names

    @staticmethod
    def _collect_subquery_names_postorder(node: exp.Expression, out: list) -> None:
        """Recursively collect subquery aliases in post-order."""
        for child in node.iter_expressions():
            NestedResolver._collect_subquery_names_postorder(child, out)
        if isinstance(node, exp.Subquery) and node.alias:
            out.append(node.alias)

    # -------------------------------------------------------------------
    # Body extraction
    # -------------------------------------------------------------------

    @staticmethod
    def _body_sql(node: exp.Expression) -> str:
        """Render an AST node to SQL, stripping identifier quoting."""
        body = copy.deepcopy(node)
        for ident in body.find_all(exp.Identifier):
            ident.set("quoted", False)
        return _GENERATOR.generate(body)

    def extract_cte_bodies(
        self,
        cte_names: List[str],
    ) -> Dict[str, str]:
        """Extract CTE body SQL for each name in *cte_names*.

        :param cte_names: Ordered list of CTE names to extract bodies for.
        :returns: Mapping of ``{cte_name: body_sql}``.
        """
        if not self._ast or not cte_names:
            return {}

        alias_to_name: Dict[str, str] = {}
        for name in cte_names:
            placeholder = name.replace(".", DOT_PLACEHOLDER)
            alias_to_name[placeholder.upper()] = name
            alias_to_name[name.upper()] = name
            alias_to_name[last_segment(name).upper()] = name

        results: Dict[str, str] = {}
        for cte in self._cte_nodes():
            alias = cte.alias
            if alias.upper() in alias_to_name:
                original_name = alias_to_name[alias.upper()]
                results[original_name] = self._body_sql(cte.this)

        return results

    def extract_subquery_bodies(
        self,
        subquery_names: List[str],
    ) -> Dict[str, str]:
        """Extract subquery body SQL for each name in *subquery_names*.

        Uses a post-order AST walk so that inner subqueries appear before
        outer ones.

        :param subquery_names: List of subquery alias names to extract.
        :returns: Mapping of ``{subquery_name: body_sql}``.
        """
        if not self._ast or not subquery_names:
            return {}

        names_upper = {n.upper(): n for n in subquery_names}
        results: Dict[str, str] = {}
        self._collect_subqueries_postorder(self._ast, names_upper, results)
        return results

    @staticmethod
    def _collect_subqueries_postorder(
        node: exp.Expression, names_upper: Dict[str, str], out: Dict[str, str]
    ) -> None:
        """Recursively collect subquery bodies in post-order."""
        for child in node.iter_expressions():
            NestedResolver._collect_subqueries_postorder(child, names_upper, out)
        if isinstance(node, exp.Subquery) and node.alias:
            alias_upper = node.alias.upper()
            if alias_upper in names_upper:
                original_name = names_upper[alias_upper]
                out[original_name] = NestedResolver._body_sql(node.this)

    # -------------------------------------------------------------------
    # Column resolution (from parser.py)
    # -------------------------------------------------------------------

    def resolve(
        self,
        columns: "UniqueList",
        columns_dict: Dict,
        columns_aliases: Dict,
        subqueries_names: List[str],
        subqueries: Dict,
        with_names: List[str],
        with_queries: Dict,
    ) -> tuple:
        """Resolve columns that reference subqueries or CTEs.

        Two-phase resolution:

        1. Replace ``subquery.column`` references with the actual column
           from the subquery/CTE definition.
        2. Drop bare column names that are actually aliases defined inside
           a nested query.

        Also applies the same resolution to *columns_dict*.

        :returns: Tuple of ``(columns, columns_dict, columns_aliases)``.
        """
        self._subqueries_names = subqueries_names
        self._subqueries = subqueries
        self._with_names = with_names
        self._with_queries = with_queries
        self._columns_aliases = columns_aliases

        columns = self._resolve_and_filter(columns, drop_bare_aliases=True)

        if columns_dict:
            for section, cols in list(columns_dict.items()):
                columns_dict[section] = self._resolve_and_filter(
                    cols, drop_bare_aliases=False
                )

        return columns, columns_dict, self._columns_aliases

    def _resolve_and_filter(
        self, columns: "UniqueList", drop_bare_aliases: bool = True
    ) -> "UniqueList":
        """Apply subquery/CTE resolution and bare-alias handling."""
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

    def _nested_sources(self) -> list:
        """Return the (names, defs, cache) tuples for subqueries then CTEs."""
        return [
            (self._subqueries_names, self._subqueries, self._subqueries_parsers),
            (self._with_names, self._with_queries, self._with_parsers),
        ]

    def _resolve_sub_queries(self, column: str) -> Union[str, List[str]]:
        """Resolve a ``subquery.column`` reference to actual column(s)."""
        result: Union[str, List[str]] = column
        for names, defs, cache in self._nested_sources():
            if isinstance(result, str):
                result = self._resolve_nested_query(
                    subquery_alias=result,
                    nested_queries_names=names,
                    nested_queries=defs,
                    already_parsed=cache,
                )
        return result if isinstance(result, list) else [result]

    def _resolve_bare_through_nested(self, col_name: str) -> Union[str, List[str]]:
        """Resolve a bare column name through subquery/CTE alias definitions."""
        for i, (names, defs, cache) in enumerate(self._nested_sources()):
            result = self._lookup_alias_in_nested(
                col_name, names, defs, cache, check_columns=(i == 0)
            )
            if result is not None:
                return result
        return col_name

    def _lookup_alias_in_nested(
        self,
        col_name: str,
        names: List[str],
        definitions: Dict,
        parser_cache: Dict,
        check_columns: bool = False,
    ) -> Optional[Union[str, List[str]]]:
        """Search for a bare column as an alias in nested queries."""
        from sql_metadata.parser import Parser

        for nested_name in names:
            nested_def = definitions.get(nested_name)
            if not nested_def:
                continue
            nested_parser = parser_cache.setdefault(nested_name, Parser(nested_def))
            if col_name in nested_parser.columns_aliases_names:
                resolved = self._resolve_column_alias(
                    col_name, nested_parser.columns_aliases
                )
                if self._columns_aliases is not None:
                    immediate = nested_parser.columns_aliases.get(col_name, resolved)
                    self._columns_aliases[col_name] = immediate
                return resolved
            if check_columns and col_name in nested_parser.columns:
                return col_name
        return None

    def resolve_column_alias(
        self, alias: Union[str, List[str]], columns_aliases: Dict
    ) -> Union[str, List]:
        """Public interface for alias resolution (used by parser.py)."""
        return self._resolve_column_alias(alias, columns_aliases)

    def _resolve_column_alias(
        self,
        alias: Union[str, List[str]],
        columns_aliases: Dict,
        visited: Optional[Set] = None,
    ) -> Union[str, List]:
        """Recursively resolve a column alias to its underlying column(s)."""
        visited = visited or set()
        if isinstance(alias, list):
            return [
                self._resolve_column_alias(x, columns_aliases, visited) for x in alias
            ]
        while alias in columns_aliases and alias not in visited:
            visited.add(alias)
            alias = columns_aliases[alias]
            if isinstance(alias, list):
                return self._resolve_column_alias(alias, columns_aliases, visited)
        return alias

    @staticmethod
    def _resolve_nested_query(
        subquery_alias: str,
        nested_queries_names: List[str],
        nested_queries: Dict,
        already_parsed: Dict,
    ) -> Union[str, List[str]]:
        """Resolve a ``prefix.column`` reference through a nested query."""
        from sql_metadata.parser import Parser

        parts = subquery_alias.split(".")
        if len(parts) != 2 or parts[0] not in nested_queries_names:
            return subquery_alias
        sub_query, column_name = parts[0], parts[-1]
        sub_query_definition = nested_queries.get(sub_query)
        if not sub_query_definition:
            return subquery_alias
        subparser = already_parsed.setdefault(sub_query, Parser(sub_query_definition))
        return NestedResolver._resolve_column_in_subparser(
            column_name, subparser, subquery_alias
        )

    @staticmethod
    def _resolve_column_in_subparser(
        column_name: str, subparser: "Parser", original_ref: str
    ) -> Union[str, List[str]]:
        """Resolve a column name through a parsed nested query."""
        if column_name in subparser.columns_aliases_names:
            resolved = subparser._resolve_column_alias(column_name)
            if isinstance(resolved, list):
                return flatten_list(resolved)
            return [resolved]
        if column_name == "*":
            return subparser.columns
        return NestedResolver._find_column_fallback(
            column_name, subparser, original_ref
        )

    @staticmethod
    def _find_column_fallback(
        column_name: str, subparser: "Parser", original_ref: str
    ) -> Union[str, List[str]]:
        """Find a column by name in the subparser with wildcard fallbacks."""
        try:
            idx = [last_segment(x) for x in subparser.columns].index(column_name)
        except ValueError:
            if "*" in subparser.columns:
                return column_name
            for table in subparser.tables:
                if f"{table}.*" in subparser.columns:
                    return column_name
            return original_ref
        return [subparser.columns[idx]]
