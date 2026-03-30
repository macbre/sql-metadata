"""Single-pass SQL metadata extraction from a sqlglot AST.

Walks the AST in ``arg_types``-key order (which mirrors the left-to-right
SQL text order) and collects columns, column aliases, CTE names, and
subquery names into a :class:`_Collector` accumulator.  The
:class:`ColumnExtractor` class encapsulates the walk and all helper methods,
replacing the earlier flat-function design with a cohesive class.

The public entry point is :meth:`ColumnExtractor.extract`, which returns an
:class:`ExtractionResult` dataclass consumed by :attr:`Parser.columns`
and friends.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from sqlglot import exp

from sql_metadata.utils import UniqueList

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionResult:
    """Immutable container for column extraction results.

    Replaces the earlier 7-tuple return value with named fields.
    """

    columns: UniqueList
    columns_dict: Dict[str, UniqueList]
    alias_names: UniqueList
    alias_dict: Optional[Dict[str, UniqueList]]
    alias_map: Dict[str, Union[str, list]]
    cte_names: UniqueList
    subquery_names: UniqueList


# ---------------------------------------------------------------------------
# Clause classification (pure functions, no state)
# ---------------------------------------------------------------------------


#: Simple key → clause-name lookup for most ``arg_types`` keys.
_CLAUSE_MAP: Dict[str, str] = {
    "where": "where",
    "group": "group_by",
    "order": "order_by",
    "having": "having",
}

#: Keys that map to the ``"join"`` clause section.
_JOIN_KEYS = frozenset({"on", "using"})


def _classify_expressions_clause(parent_type: type) -> str:
    """Resolve the clause for an ``"expressions"`` key based on the parent node.

    :param parent_type: The type of the parent AST node.
    :returns: ``"update"``, ``"select"``, or ``""`` for other parents.
    """
    if parent_type is exp.Update:
        return "update"
    if parent_type is exp.Select:
        return "select"
    return ""


def _classify_clause(key: str, parent_type: type) -> str:
    """Map an ``arg_types`` key and parent node type to a ``columns_dict`` section.

    :param key: The ``arg_types`` key through which the child was reached.
    :param parent_type: The type of the parent AST node.
    :returns: Section name string, or ``""`` if the key does not map.
    """
    if key == "expressions":
        return _classify_expressions_clause(parent_type)
    if key in _JOIN_KEYS:
        return "join"
    return _CLAUSE_MAP.get(key, "")


# ---------------------------------------------------------------------------
# Pure helpers (no state)
# ---------------------------------------------------------------------------


def _dfs(node: exp.Expression):
    """Yield *node* and all its descendants in depth-first order.

    :param node: Root expression node.
    :yields: Each expression node in DFS pre-order.
    """
    yield node
    for child in node.iter_expressions():
        yield from _dfs(child)


#: Functions whose first argument is a date-part unit keyword, not a column.
_DATE_PART_FUNCTIONS = frozenset({
    "dateadd", "datediff", "datepart", "datename", "date_add", "date_sub",
    "date_diff", "date_trunc", "timestampadd", "timestampdiff",
})


def _is_date_part_unit(node: exp.Column) -> bool:
    """Return True if *node* is the first arg of a date-part function."""
    parent = node.parent
    if (
        isinstance(parent, exp.Anonymous)
        and parent.this.lower() in _DATE_PART_FUNCTIONS
    ):
        exprs = parent.expressions
        return len(exprs) > 0 and exprs[0] is node
    return False


def _make_reverse_cte_map(cte_name_map: Dict) -> Dict[str, str]:
    """Build reverse mapping from placeholder CTE names to originals."""
    reverse = {v.replace(".", "__DOT__"): v for v in cte_name_map.values()}
    reverse.update(cte_name_map)
    return reverse


# ---------------------------------------------------------------------------
# Collector — accumulates results during AST walk
# ---------------------------------------------------------------------------


class _Collector:
    """Mutable accumulator for metadata gathered during the AST walk.

    :param table_aliases: Pre-computed table alias → real name mapping.
    """

    __slots__ = (
        "ta",
        "columns",
        "columns_dict",
        "alias_names",
        "alias_dict",
        "alias_map",
        "cte_names",
        "cte_alias_names",
        "subquery_items",
    )

    def __init__(self, table_aliases: Dict[str, str]):
        self.ta = table_aliases
        self.columns = UniqueList()
        self.columns_dict: Dict[str, UniqueList] = {}
        self.alias_names = UniqueList()
        self.alias_dict: Dict[str, UniqueList] = {}
        self.alias_map: Dict[str, Union[str, list]] = {}
        self.cte_names = UniqueList()
        self.cte_alias_names: set = set()
        self.subquery_items: list = []

    def add_column(self, name: str, clause: str) -> None:
        """Record a column name, filing it into the appropriate section."""
        self.columns.append(name)
        if clause:
            self.columns_dict.setdefault(clause, UniqueList()).append(name)

    def add_alias(self, name: str, target, clause: str) -> None:
        """Record a column alias and its target expression."""
        self.alias_names.append(name)
        if clause:
            self.alias_dict.setdefault(clause, UniqueList()).append(name)
        if target is not None:
            self.alias_map[name] = target


# ---------------------------------------------------------------------------
# arg_types keys to skip during the walk.
# ---------------------------------------------------------------------------

_SKIP_KEYS = frozenset({"conflict", "returning", "alternative"})


# ---------------------------------------------------------------------------
# ColumnExtractor — the main class
# ---------------------------------------------------------------------------


class ColumnExtractor:
    """Single-pass DFS extraction of columns, aliases, CTEs, and subqueries.

    Walks the AST in ``arg_types``-key order and collects all metadata into
    an internal :class:`_Collector`.  Call :meth:`extract` to run the walk
    and return an :class:`ExtractionResult`.

    :param ast: Root AST node.
    :param table_aliases: Table alias → real name mapping.
    :param cte_name_map: Placeholder → original qualified CTE name mapping.
    """

    def __init__(
        self,
        ast: exp.Expression,
        table_aliases: Dict[str, str],
        cte_name_map: Dict = None,
    ):
        self._ast = ast
        self._table_aliases = table_aliases
        self._cte_name_map = cte_name_map or {}
        self._collector = _Collector(table_aliases)
        self._reverse_cte_map = self._build_reverse_cte_map()

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def extract(self) -> ExtractionResult:
        """Run the full extraction walk and return results.

        For ``CREATE TABLE`` statements without a ``SELECT`` (pure DDL),
        only ``ColumnDef`` nodes are collected.
        """
        c = self._collector

        self._seed_cte_names()

        # Handle CREATE TABLE with column defs (no SELECT)
        if isinstance(self._ast, exp.Create) and not self._ast.find(exp.Select):
            for col_def in self._ast.find_all(exp.ColumnDef):
                c.add_column(col_def.name, "")
            return self._build_result()

        # Reset cte_names — walk will re-collect them in order
        c.cte_names = UniqueList()
        self._walk(self._ast)

        # Restore qualified CTE names
        final_cte = UniqueList()
        for name in c.cte_names:
            final_cte.append(self._reverse_cte_map.get(name, name))

        alias_dict = c.alias_dict if c.alias_dict else None
        return ExtractionResult(
            columns=c.columns,
            columns_dict=c.columns_dict,
            alias_names=c.alias_names,
            alias_dict=alias_dict,
            alias_map=c.alias_map,
            cte_names=final_cte,
            subquery_names=self._build_subquery_names(),
        )

    # -------------------------------------------------------------------
    # Static/class methods (also called independently by Parser)
    # -------------------------------------------------------------------

    @staticmethod
    def extract_cte_names(
        ast: exp.Expression, cte_name_map: Dict = None
    ) -> List[str]:
        """Extract CTE names from the AST.

        Called by :attr:`Parser.with_names`.
        """
        if ast is None:
            return []
        cte_name_map = cte_name_map or {}
        reverse_map = _make_reverse_cte_map(cte_name_map)
        names = UniqueList()
        for cte in ast.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                names.append(reverse_map.get(alias, alias))
        return names

    @staticmethod
    def extract_subquery_names(ast: exp.Expression) -> List[str]:
        """Extract aliased subquery names from the AST in post-order.

        Called by :attr:`Parser.subqueries_names`.
        """
        if ast is None:
            return []
        names = UniqueList()
        ColumnExtractor._collect_subqueries_postorder(ast, names)
        return names

    @staticmethod
    def _collect_subqueries_postorder(node: exp.Expression, out: list) -> None:
        """Recursively collect subquery aliases in post-order."""
        for child in node.iter_expressions():
            ColumnExtractor._collect_subqueries_postorder(child, out)
        if isinstance(node, exp.Subquery) and node.alias:
            out.append(node.alias)

    # -------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------

    def _build_reverse_cte_map(self) -> Dict[str, str]:
        """Build reverse mapping from placeholder CTE names to originals."""
        return _make_reverse_cte_map(self._cte_name_map)

    def _seed_cte_names(self) -> None:
        """Pre-populate CTE names in the collector for alias detection."""
        for cte in self._ast.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                self._collector.cte_names.append(
                    self._reverse_cte_map.get(alias, alias)
                )

    def _build_subquery_names(self) -> UniqueList:
        """Sort subquery items by depth (innermost first) and build names list."""
        c = self._collector
        c.subquery_items.sort(key=lambda x: -x[0])
        names = UniqueList()
        for _, name in c.subquery_items:
            names.append(name)
        return names

    def _build_result(self) -> ExtractionResult:
        """Build result from collector (used for early-return CREATE TABLE path)."""
        c = self._collector
        alias_dict = c.alias_dict if c.alias_dict else None
        return ExtractionResult(
            columns=c.columns,
            columns_dict=c.columns_dict,
            alias_names=c.alias_names,
            alias_dict=alias_dict,
            alias_map=c.alias_map,
            cte_names=c.cte_names,
            subquery_names=self._build_subquery_names(),
        )

    # -------------------------------------------------------------------
    # Column name helpers
    # -------------------------------------------------------------------

    def _resolve_table_alias(self, col_table: str) -> str:
        """Replace a table alias with the real table name if mapped."""
        return self._table_aliases.get(col_table, col_table)

    def _column_full_name(self, col: exp.Column) -> str:
        """Build a fully-qualified column name with the table alias resolved."""
        name = col.name.rstrip("#")
        table = col.table
        db = col.args.get("db")
        catalog = col.args.get("catalog")

        if table:
            resolved = self._resolve_table_alias(table)
            parts = []
            if catalog:
                parts.append(
                    catalog.name if isinstance(catalog, exp.Expression) else catalog
                )
            if db:
                parts.append(db.name if isinstance(db, exp.Expression) else db)
            parts.append(resolved)
            parts.append(name)
            return ".".join(parts)
        return name

    @staticmethod
    def _is_star_inside_function(star: exp.Star) -> bool:
        """Determine whether a ``*`` node is inside a function call.

        Uses sqlglot's ``find_ancestor`` to check for ``Func`` or
        ``Anonymous`` (user-defined function) nodes in the parent chain.
        """
        return star.find_ancestor(exp.Func, exp.Anonymous) is not None

    # -------------------------------------------------------------------
    # DFS walk
    # -------------------------------------------------------------------

    def _walk(self, node, clause: str = "", depth: int = 0) -> None:
        """Depth-first walk of the AST in ``arg_types`` key order."""
        if node is None:
            return

        if self._dispatch_leaf(node, clause, depth):
            return

        if hasattr(node, "arg_types"):
            self._walk_children(node, clause, depth)

    def _walk_children(self, node, clause: str, depth: int) -> None:
        """Recurse into children of *node* in ``arg_types`` key order."""
        for key in node.arg_types:
            if key in _SKIP_KEYS:
                continue
            child = node.args.get(key)
            if child is None:
                continue

            new_clause = _classify_clause(key, type(node)) or clause

            if not self._process_child_key(node, key, child, new_clause, depth):
                self._recurse_child(child, new_clause, depth)

    def _dispatch_leaf(self, node, clause: str, depth: int) -> bool:
        """Dispatch leaf-like AST nodes to their specialised handlers.

        Returns ``True`` if handled (stop recursion), ``False`` to continue.
        """
        if isinstance(node, (exp.Values, exp.Star, exp.ColumnDef, exp.Identifier)):
            if isinstance(node, exp.Star):
                self._handle_star(node, clause)
            elif isinstance(node, exp.ColumnDef):
                self._collector.add_column(node.name, clause)
            elif isinstance(node, exp.Identifier):
                self._handle_identifier(node, clause)
            return True
        if isinstance(node, exp.CTE):
            self._handle_cte(node, depth)
            return True
        if isinstance(node, exp.Column):
            self._handle_column(node, clause)
            return True
        if isinstance(node, exp.Subquery) and node.alias:
            self._collector.subquery_items.append((depth, node.alias))
        return False

    def _process_child_key(
        self, node, key: str, child, clause: str, depth: int
    ) -> bool:
        """Handle special cases for SELECT expressions, INSERT schema, JOIN USING.

        Returns ``True`` if handled, ``False`` for default recursive walk.
        """
        if key == "expressions" and isinstance(node, exp.Select):
            self._handle_select_exprs(child, clause, depth)
            return True
        if isinstance(node, exp.Insert) and key == "this":
            self._handle_insert_schema(node)
            return True
        if key == "using" and isinstance(node, exp.Join):
            self._handle_join_using(child)
            return True
        return False

    def _recurse_child(self, child, clause: str, depth: int) -> None:
        """Recursively walk a child value (single expression or list)."""
        if isinstance(child, list):
            for item in child:
                if isinstance(item, exp.Expression):
                    self._walk(item, clause, depth + 1)
        elif isinstance(child, exp.Expression):
            self._walk(child, clause, depth + 1)

    # -------------------------------------------------------------------
    # Node handlers
    # -------------------------------------------------------------------

    def _handle_star(self, node: exp.Star, clause: str) -> None:
        """Handle a standalone Star node (not inside a Column or function)."""
        not_in_col = not isinstance(node.parent, exp.Column)
        if not_in_col and not self._is_star_inside_function(node):
            self._collector.add_column("*", clause)

    def _handle_identifier(self, node: exp.Identifier, clause: str) -> None:
        """Handle an Identifier in a USING clause (not inside a Column)."""
        if not isinstance(
            node.parent,
            (exp.Column, exp.Table, exp.TableAlias, exp.CTE),
        ):
            if clause == "join":
                self._collector.add_column(node.name, clause)

    def _handle_insert_schema(self, node: exp.Insert) -> None:
        """Extract column names from the Schema of an INSERT statement."""
        schema = node.find(exp.Schema)
        if schema and schema.expressions:
            for col_id in schema.expressions:
                name = col_id.name if hasattr(col_id, "name") else str(col_id)
                self._collector.add_column(name, "insert")

    def _handle_join_using(self, child) -> None:
        """Extract column identifiers from a JOIN USING clause."""
        if isinstance(child, list):
            for item in child:
                if hasattr(item, "name"):
                    self._collector.add_column(item.name, "join")

    def _handle_column(self, col: exp.Column, clause: str) -> None:
        """Handle a Column AST node during the walk."""
        c = self._collector

        star = col.find(exp.Star)
        if star:
            table = col.table
            if table:
                table = self._resolve_table_alias(table)
                c.add_column(f"{table}.*", clause)
            else:
                c.add_column("*", clause)
            return

        # Check for CTE column alias reference
        if col.table and col.table in c.cte_names and col.name in c.cte_alias_names:
            c.alias_dict.setdefault(clause, UniqueList()).append(col.name)
            return

        full = self._column_full_name(col)

        # Check if bare name is a known alias
        bare = col.name
        if not col.table and bare in c.alias_names:
            c.alias_dict.setdefault(clause, UniqueList()).append(bare)
            return

        c.add_column(full, clause)

    def _handle_select_exprs(self, exprs, clause: str, depth: int) -> None:
        """Handle the expressions list of a SELECT clause."""
        if not isinstance(exprs, list):
            return

        for expr in exprs:
            if isinstance(expr, exp.Alias):
                self._handle_alias(expr, clause, depth)
            elif isinstance(expr, exp.Star):
                self._collector.add_column("*", clause)
            elif isinstance(expr, exp.Column):
                self._handle_column(expr, clause)
            else:
                cols = self._flat_columns(expr)
                for col in cols:
                    self._collector.add_column(col, clause)

    def _handle_alias(self, alias_node: exp.Alias, clause: str, depth: int) -> None:
        """Handle an Alias node inside a SELECT expression list."""
        c = self._collector
        alias_name = alias_node.alias
        inner = alias_node.this

        select = inner.find(exp.Select)
        if select:
            self._walk(inner, clause, depth + 1)
            target_cols = self._flat_columns_select_only(select)
            target = (
                target_cols[0]
                if len(target_cols) == 1
                else (target_cols if target_cols else None)
            )
            c.add_alias(alias_name, target, clause)
            return

        inner_cols = self._flat_columns(inner)

        if inner_cols:
            for col in inner_cols:
                c.add_column(col, clause)

            unique_inner = list(dict.fromkeys(inner_cols))
            is_self_alias = len(unique_inner) == 1 and (
                unique_inner[0] == alias_name
                or unique_inner[0].split(".")[-1] == alias_name
            )
            is_direct = isinstance(inner, exp.Column)

            if is_direct and is_self_alias:
                pass  # SELECT col AS col — not an alias
            else:
                target = None
                if not is_self_alias:
                    target = unique_inner[0] if len(unique_inner) == 1 else unique_inner
                c.add_alias(alias_name, target, clause)
        else:
            target = None
            if inner.find(exp.Star):
                target = "*"
            c.add_alias(alias_name, target, clause)

    def _handle_cte(self, cte: exp.CTE, depth: int) -> None:
        """Handle a CTE (Common Table Expression) AST node."""
        c = self._collector
        alias = cte.alias
        if not alias:
            return

        c.cte_names.append(alias)

        table_alias = cte.args.get("alias")
        has_col_defs = table_alias and table_alias.columns
        body = cte.this

        if has_col_defs and body and isinstance(body, exp.Select):
            body_cols = self._flat_columns(body)
            real_cols = [x for x in body_cols if x != "*"]
            cte_col_names = [col.name for col in table_alias.columns]

            for col in body_cols:
                c.add_column(col, "select")

            for i, cte_col in enumerate(cte_col_names):
                if i < len(real_cols):
                    target = real_cols[i]
                elif "*" in body_cols:
                    target = "*"
                else:
                    target = None
                c.add_alias(cte_col, target, "select")
                c.cte_alias_names.add(cte_col)
        elif body and isinstance(
            body, (exp.Select, exp.Union, exp.Intersect, exp.Except)
        ):
            self._walk(body, "", depth + 1)

    # -------------------------------------------------------------------
    # Flat column extraction helpers
    # -------------------------------------------------------------------

    def _flat_columns_select_only(self, select: exp.Select) -> list:
        """Extract column/alias names from a SELECT's immediate expressions."""
        cols = []
        for expr in select.expressions or []:
            if isinstance(expr, exp.Alias):
                cols.append(expr.alias)
            elif isinstance(expr, exp.Column):
                cols.append(self._column_full_name(expr))
            elif isinstance(expr, exp.Star):
                cols.append("*")
            else:
                for col_name in self._flat_columns(expr):
                    cols.append(col_name)
        return cols

    def _collect_column_from_node(
        self, child: exp.Expression, seen_stars: set
    ) -> Union[str, None]:
        """Extract a column name from a single DFS node."""
        if isinstance(child, exp.Column):
            if _is_date_part_unit(child):
                return None
            star = child.find(exp.Star)
            if star:
                seen_stars.add(id(star))
                table = child.table
                if table:
                    table = self._resolve_table_alias(table)
                    return f"{table}.*"
                return "*"
            return self._column_full_name(child)
        if isinstance(child, exp.Star):
            if id(child) not in seen_stars and not isinstance(child.parent, exp.Column):
                if not self._is_star_inside_function(child):
                    return "*"
        return None

    def _flat_columns(self, node: exp.Expression) -> list:
        """Extract all column names from an expression subtree via DFS."""
        cols = []
        if node is None:
            return cols
        seen_stars = set()
        for child in _dfs(node):
            name = self._collect_column_from_node(child, seen_stars)
            if name is not None:
                cols.append(name)
        return cols

