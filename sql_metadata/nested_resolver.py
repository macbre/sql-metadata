"""Nested column resolution and CTE/subquery body extraction.

The :class:`NestedResolver` class owns the complete "look inside nested
queries" concern: rendering CTE/subquery AST nodes back to SQL, parsing
those bodies with sub-:class:`Parser` instances, and resolving
``subquery.column`` references to actual columns.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql_metadata.parser import Parser

from sqlglot import exp
from sqlglot.generator import Generator

from sql_metadata.utils import (
    UniqueList,
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
        args_sql = ", ".join(self.sql(a) for a in args)
        return f"COALESCE({args_sql})"

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
# Resolution helpers
# ---------------------------------------------------------------------------


def _is_qualified_reference(result: list[str]) -> bool:
    """Check if result is a single dotted reference like ``['cte.col']``."""
    return len(result) == 1 and "." in result[0]


def _is_not_already_resolved_qualified_reference(
    result: list[str], column: str
) -> bool:
    """Check if result is a qualified reference that changed from the input."""
    return _is_qualified_reference(result) and result != [column]


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
    3. **Unqualified alias resolution** — detect column names that are actually
       aliases defined inside nested queries.

    :param ast: Root AST node (for body extraction).
    """

    def __init__(self, ast: exp.Expression):
        self._ast = ast

        # Lazy caches
        self._subqueries_parsers: dict[str, "Parser"] = {}
        self._with_parsers: dict[str, "Parser"] = {}
        self._columns_aliases: dict[str, str | list[str]] = {}
        self._cached_cte_nodes: list[exp.CTE] | None = None

        # Set by resolve() caller
        self._subqueries_names: list[str] = []
        self._subqueries: dict[str, str] = {}
        self._with_names: list[str] = []
        self._with_queries: dict[str, str] = {}

    # -------------------------------------------------------------------
    # Public API — name extraction
    # -------------------------------------------------------------------

    def extract_cte_names(
        self,
        cte_name_map: dict[str, str],
    ) -> list[str]:
        """Extract CTE names from the AST.

        Called by :attr:`Parser.with_names`.

        :param cte_name_map: Mapping of placeholder names to original
            qualified names, e.g. ``{"db__DOT__cte": "db.cte"}``.
            Built by :func:`SqlCleaner._normalize_cte_names` because
            sqlglot cannot parse dots in CTE names — they get rewritten
            to placeholders before parsing.  This map restores the
            original names in the output.
        :returns: List of CTE names, e.g. ``["db.cte", "sales"]``.
        """
        return UniqueList([
            cte_name_map.get(cte.alias, cte.alias) for cte in self._cte_nodes()
        ])

    def extract_cte_bodies(
        self,
        cte_name_map: dict[str, str],
    ) -> dict[str, str]:
        """Extract CTE body SQL for each CTE in the AST.

        :param cte_name_map: Placeholder-to-original mapping, e.g.
            ``{"db__DOT__cte": "db.cte"}``.  See :meth:`extract_cte_names`
            for details.
        :returns: Mapping of ``{cte_name: body_sql}``,
            e.g. ``{"db.cte": "SELECT id FROM t"}``.
        """
        results: dict[str, str] = {}
        for cte in self._cte_nodes():
            alias = cte.alias
            original_name = cte_name_map.get(alias, alias)
            results[original_name] = self._body_sql(cte.this)

        return results

    @staticmethod
    def extract_subqueries(
        ast: exp.Expression,
    ) -> tuple[list[str], dict[str, str]]:
        """Extract subquery names and bodies in a single post-order walk.

        Aliased subqueries keep their alias as the name.  Unaliased
        subqueries (e.g. ``WHERE id IN (SELECT …)``) get auto-generated
        names ``subquery_1``, ``subquery_2``, etc.

        Example SQL::

            SELECT * FROM (SELECT id FROM t) AS sub
            WHERE id IN (SELECT id FROM t2)

        :returns: ``(names, bodies)`` where *names* is ordered innermost-first,
            e.g. ``(["subquery_1", "sub"], {...})``.
        """
        names: list[str] = UniqueList()
        bodies: dict[str, str] = {}
        NestedResolver._walk_subqueries(ast, names, bodies, 0)
        return names, bodies

    # -------------------------------------------------------------------
    # Public API — column resolution
    # -------------------------------------------------------------------

    def resolve(
        self,
        columns: "UniqueList",
        columns_dict: dict[str, UniqueList],
        columns_aliases: dict[str, str | list[str]],
        subqueries_names: list[str],
        subqueries: dict[str, str],
        with_names: list[str],
        with_queries: dict[str, str],
    ) -> tuple[UniqueList, dict[str, UniqueList], dict[str, str | list[str]]]:
        """Resolve columns that reference subqueries or CTEs.

        Two-phase resolution:

        1. Replace ``subquery.column`` references with the actual column
           from the subquery/CTE definition.
        2. Drop unqualified column names that are actually aliases defined
           inside a nested query.

        Also applies the resolution to *columns_dict*,
        but there instead of dropping we also resolve unqualified aliases.

        Example SQL::

            WITH cte AS (SELECT a FROM t)
            SELECT cte.a FROM cte

        :returns: Tuple of ``(columns, columns_dict, columns_aliases)``.
        """
        self._subqueries_names = subqueries_names
        self._subqueries = subqueries
        self._with_names = with_names
        self._with_queries = with_queries
        self._columns_aliases = columns_aliases

        # For columns drop aliases as we need only actual columns
        columns = self._resolve_and_filter(columns, drop_unqualified_aliases=True)

        if columns_dict:
            # For columns_dict do not drop aliases but instead resolve them to columns.
            # That ensures the column is present in all the relevant sections regardless
            # if it's called directly or by alias i.e. SELECT a AS x FROM tbl ORDER BY x
            # the column a should appear both in select and order_by sections.
            for section, cols in list(columns_dict.items()):
                columns_dict[section] = self._resolve_and_filter(
                    cols, drop_unqualified_aliases=False
                )

        return columns, columns_dict, self._columns_aliases

    def resolve_column_alias(
        self, alias: str | list[str], columns_aliases: dict[str, str | list[str]]
    ) -> list[str]:
        """Public interface for alias resolution (used by parser.py).

        Example SQL::

            SELECT a AS x FROM t ORDER BY x

        Resolves ``"x"`` → ``"a"`` using the alias map.
        """
        return self._resolve_column_alias(alias, columns_aliases)

    # -------------------------------------------------------------------
    # Resolution pipeline — callers before callees
    # -------------------------------------------------------------------

    def _resolve_and_filter(
        self, columns: "UniqueList", drop_unqualified_aliases: bool = True
    ) -> "UniqueList":
        """Apply subquery/CTE resolution and unqualified-alias handling.

        Phase 1: resolve ``sub.col`` references via :meth:`_resolve_sub_queries`.
        Phase 2: detect unqualified names that are nested-query aliases.

        Example SQL::

            SELECT sub.id FROM (SELECT id FROM users) AS sub

        Phase 1 resolves ``sub.id`` → ``id``.
        Phase 2 checks if ``id`` is a nested alias (it is not, so it stays).
        """
        resolved: list[str] = UniqueList()
        for col in columns:
            resolved.extend(self._resolve_sub_queries(col))

        final = UniqueList()
        for col in resolved:
            if "." in col:
                # e.g. schema.col — skip unqualified alias resolution
                final.append(col)
                continue
            new_cols = self._resolve_unqualified_through_nested(col)
            if new_cols != [col]:
                # e.g. SELECT x FROM (SELECT a AS x FROM t) AS sub
                # — "x" resolved to "a", drop the alias from columns
                if not drop_unqualified_aliases:
                    final.extend(new_cols)
                continue
            # e.g. SELECT id FROM t — no alias match, keep as-is
            final.append(col)
        return final

    def _resolve_sub_queries(self, column: str) -> list[str]:
        """Resolve a ``subquery.column`` reference to actual column(s).

        Tries subquery sources first, then CTE sources.

        Example SQL::

            SELECT sub.id FROM (SELECT id FROM users) AS sub

        Resolves ``"sub.id"`` → ``["id"]``.
        """
        result: list[str] = [column]
        for names, defs, cache in self._nested_sources():
            if _is_qualified_reference(result):
                # e.g. "sub.id" — still a qualified reference, try next source
                result = self._resolve_nested_query(
                    subquery_alias=result[0],
                    nested_queries_names=names,
                    nested_queries=defs,
                    already_parsed=cache,
                )
        # Recursively resolve chained CTE references: c3.a → c2.a → c1.a → a
        if _is_not_already_resolved_qualified_reference(result, column):
            return self._resolve_sub_queries(result[0])
        return result

    def _resolve_unqualified_through_nested(
        self, col_name: str
    ) -> list[str]:
        """Resolve an unqualified column name through subquery/CTE alias definitions.

        Checks subquery aliases first (``check_columns=True``), then CTE
        aliases (``check_columns=False``).

        Example SQL::

            SELECT x FROM (SELECT a AS x FROM users) AS sub

        Resolves ``"x"`` → ``["a"]`` (found as alias in subquery body).
        """
        for i, (names, defs, cache) in enumerate(self._nested_sources()):
            # check_columns for subqueries only — prevents CTE aliases
            # from claiming subquery columns, e.g. in:
            #   WITH cte AS (SELECT x AS name FROM t1)
            #   SELECT name FROM (SELECT name FROM t2) AS sub
            # "name" is a real column in sub, not the CTE alias.
            result = self._lookup_alias_in_nested(
                col_name, names, defs, cache, check_columns=(i == 0)
            )
            if result is not None:
                return result
        return [col_name]

    def _lookup_alias_in_nested(
        self,
        col_name: str,
        names: list[str],
        definitions: dict[str, str],
        parser_cache: dict[str, "Parser"],
        check_columns: bool = False,
    ) -> list[str] | None:
        """Search for an unqualified column as an alias in nested queries.

        Parses each nested query body and checks whether *col_name* is a
        known column alias inside that body.  Three outcomes are possible:

        1. **Alias match** — the column is an alias defined inside a nested
           query and gets resolved to the underlying column(s)::

               WITH cte AS (SELECT a AS x FROM t) SELECT x FROM cte
               -- "x" found as alias in CTE body → resolves to ["a"]

           Multi-column aliases are also handled::

               SELECT y FROM (SELECT a + b AS y FROM t) AS sub
               -- "y" found as alias → resolves to ["a + b"]

        2. **Direct column match** (subqueries only, ``check_columns=True``) —
           the column exists directly in the nested query and is kept as-is::

               SELECT id FROM (SELECT id FROM users) AS sub
               -- "id" found as real column in subquery → returns ["id"]

        3. **No match** — the column is not found in any nested query,
           returns ``None`` so the caller can try other sources or keep
           the column unchanged::

               SELECT name FROM (SELECT id FROM users) AS sub
               -- "name" not in subquery → returns None
        """
        from sql_metadata.parser import Parser

        for nested_name in names:
            nested_def = definitions[nested_name]
            nested_parser = parser_cache.setdefault(nested_name, Parser(nested_def))
            if col_name in nested_parser.columns_aliases_names:
                # Path 1: alias match — resolve through the full alias chain
                # e.g. SELECT col1 AS a ... then SELECT a AS x ...
                # resolving "x": follows x → a → col1, returns ["col1"]
                resolved = self._resolve_column_alias(
                    col_name, nested_parser.columns_aliases
                )
                # Record the immediate (one-step) alias mapping for the
                # outer query's columns_aliases property, preserving the
                # direct relationship as written in SQL:
                # e.g. x → a (not the fully resolved x → col1)
                if self._columns_aliases is not None:
                    immediate = nested_parser.columns_aliases.get(col_name, resolved)
                    self._columns_aliases[col_name] = immediate
                return resolved
            if check_columns and col_name in nested_parser.columns:
                # Path 2: direct column match in subquery
                return [col_name]
        # Path 3: not found in any nested query
        return None

    @staticmethod
    def _resolve_nested_query(
        subquery_alias: str,
        nested_queries_names: list[str],
        nested_queries: dict[str, str],
        already_parsed: dict[str, "Parser"],
    ) -> list[str]:
        """Resolve a ``prefix.column`` reference through a nested query.

        Splits the alias on ``"."`` — if the prefix matches a known
        nested query name, parses that query and resolves the column.

        Example SQL::

            SELECT sub.id FROM (SELECT id FROM users) AS sub

        Resolving ``"sub.id"``: prefix ``"sub"`` matches, column
        ``"id"`` is found in the subquery → returns ``["id"]``.
        """
        from sql_metadata.parser import Parser

        parts = subquery_alias.split(".")
        if len(parts) != 2 or parts[0] not in nested_queries_names:
            # e.g. "table.col" or "schema.table.col" — not a subquery ref
            return [subquery_alias]
        sub_query, column_name = parts[0], parts[-1]
        sub_query_definition = nested_queries[sub_query]
        subparser = already_parsed.setdefault(sub_query, Parser(sub_query_definition))
        return NestedResolver._resolve_column_in_subparser(
            column_name, subparser, subquery_alias
        )

    @staticmethod
    def _resolve_column_in_subparser(
        column_name: str, subparser: "Parser", original_ref: str
    ) -> list[str]:
        """Resolve a column name through a parsed nested query.

        Three resolution paths:

        1. Column name is a known alias in the subparser → resolve it.
        2. Column name is ``*`` → return all subparser columns.
        3. Otherwise → fall back to positional/wildcard matching.

        Example SQL (path 1 — alias)::

            SELECT sub.x FROM (SELECT a AS x FROM t) AS sub

        ``"x"`` is an alias → resolves to ``["a"]``.

        Example SQL (path 2 — star)::

            SELECT sub.* FROM (SELECT a, b FROM t) AS sub

        ``"*"`` → returns ``["a", "b"]``.
        """
        if column_name in subparser.columns_aliases_names:
            # e.g. sub.x where x is aliased to a → resolve alias chain
            return subparser._resolve_column_alias(column_name)
        if column_name == "*":
            # e.g. sub.* → return all columns from subquery
            return subparser.columns
        return NestedResolver._find_column_fallback(
            column_name, subparser, original_ref
        )

    @staticmethod
    def _find_column_fallback(
        column_name: str, subparser: "Parser", original_ref: str
    ) -> list[str]:
        """Find a column by name in the subparser with wildcard fallbacks.

        Tries to match *column_name* against the last segment of each
        subparser column.  If no match is found, checks for wildcard
        columns (``*`` or ``table.*``) before giving up.

        Example SQL (positional match)::

            SELECT sub.id FROM (SELECT users.id FROM users) AS sub

        ``"id"`` matches ``"users.id"`` by last segment → ``["users.id"]``.

        Example SQL (wildcard fallback)::

            SELECT sub.id FROM (SELECT * FROM users) AS sub

        ``"id"`` not found, but subparser has ``*`` → returns ``["id"]``.
        """
        try:
            idx = [last_segment(x) for x in subparser.columns].index(column_name)
        except ValueError:
            if "*" in subparser.columns:
                # e.g. SELECT * FROM t — subquery selects everything
                return [column_name]
            for table in subparser.tables:
                if f"{table}.*" in subparser.columns:
                    # e.g. SELECT t.* FROM t — table-qualified wildcard
                    return [column_name]
            # e.g. column not found in subquery at all — keep original ref
            return [original_ref]
        # e.g. "id" matched at position idx → return fully-qualified form
        return [subparser.columns[idx]]

    # -------------------------------------------------------------------
    # Alias resolution
    # -------------------------------------------------------------------

    def _resolve_column_alias(
        self,
        alias: str | list[str],
        columns_aliases: dict[str, str | list[str]],
        visited: set[str] | None = None,
    ) -> list[str]:
        """Recursively resolve a column alias to its underlying column(s).

        Follows alias chains until a non-alias column is reached.
        Tracks visited aliases to prevent infinite loops on circular
        definitions.

        Example SQL::

            WITH cte AS (SELECT a AS x FROM t) SELECT x AS y FROM cte

        Resolving ``"y"`` → ``"x"`` → ``["a"]``.
        """
        visited = visited or set()
        if isinstance(alias, list):
            # e.g. alias mapped to multiple columns — resolve each
            return [
                item
                for x in alias
                for item in self._resolve_column_alias(x, columns_aliases, visited)
            ]
        while alias in columns_aliases and alias not in visited:
            visited.add(alias)
            alias = columns_aliases[alias]
            if isinstance(alias, list):
                # e.g. alias mapped to [col1, col2] — resolve list recursively
                return self._resolve_column_alias(alias, columns_aliases, visited)
        return [alias]

    # -------------------------------------------------------------------
    # Shared helpers
    # -------------------------------------------------------------------

    def _nested_sources(
        self,
    ) -> list[tuple[list[str], dict[str, str], dict[str, "Parser"]]]:
        """Return the (names, defs, cache) tuples for subqueries then CTEs.

        Subqueries are checked first because they are more specific than
        CTEs — a column reference ``sub.col`` should resolve against the
        subquery named ``sub`` before falling back to a CTE with the
        same name.
        """
        return [
            (self._subqueries_names, self._subqueries, self._subqueries_parsers),
            (self._with_names, self._with_queries, self._with_parsers),
        ]

    def _cte_nodes(self) -> list[exp.CTE]:
        """Return all ``exp.CTE`` nodes from the AST (cached).

        Example SQL::

            WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b

        Returns two ``exp.CTE`` nodes (for ``a`` and ``b``).
        """
        if self._cached_cte_nodes is None:
            self._cached_cte_nodes = list(self._ast.find_all(exp.CTE))
        return self._cached_cte_nodes

    # -------------------------------------------------------------------
    # Body extraction helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _body_sql(node: exp.Expression) -> str:
        """Render an AST node to SQL, stripping identifier quoting.

        Example SQL::

            WITH cte AS (SELECT "id" FROM "users") ...

        Renders the CTE body as ``SELECT id FROM users`` (quotes stripped).
        """
        body = copy.deepcopy(node)
        for ident in body.find_all(exp.Identifier):
            ident.set("quoted", False)
        return _GENERATOR.generate(body)

    @staticmethod
    def _walk_subqueries(
        node: exp.Expression,
        names: list[str],
        bodies: dict[str, str],
        counter: int,
    ) -> int:
        """Post-order walk collecting subquery names and bodies.

        Returns the updated *counter* so unnamed subqueries are numbered
        sequentially.

        Example SQL::

            SELECT * FROM (SELECT 1) AS named, (SELECT 2)

        Produces names ``["named", "subquery_1"]`` with corresponding bodies.
        """
        for child in node.iter_expressions():
            counter = NestedResolver._walk_subqueries(
                child, names, bodies, counter
            )
        if isinstance(node, exp.Subquery):
            if node.alias:
                # e.g. (SELECT 1) AS named — use the explicit alias
                name = node.alias
            else:
                # e.g. WHERE id IN (SELECT 1) — auto-generate name
                counter += 1
                name = f"subquery_{counter}"
            names.append(name)
            bodies[name] = NestedResolver._body_sql(node.this)
        return counter
