"""
Single-pass SQL metadata extraction from sqlglot AST.

Uses arg_types-order DFS walk to extract columns, aliases, CTE names,
and subquery names in SQL-text order. Replaces _columns.py, _ctes.py,
_subqueries.py.
"""

from typing import Dict, List, Union

from sqlglot import exp

from sql_metadata.utils import UniqueList


# ---------------------------------------------------------------------------
# Column name helpers
# ---------------------------------------------------------------------------

def _resolve_table_alias(col_table: str, aliases: Dict[str, str]) -> str:
    return aliases.get(col_table, col_table)


def _column_full_name(col: exp.Column, aliases: Dict[str, str]) -> str:
    """Build full column name with resolved table prefix."""
    name = col.name.rstrip("#")  # Strip MSSQL template delimiters (#WORD#)
    table = col.table
    db = col.args.get("db")
    catalog = col.args.get("catalog")

    if table:
        resolved = _resolve_table_alias(table, aliases)
        parts = []
        if catalog:
            parts.append(
                catalog.name if isinstance(catalog, exp.Expression) else catalog
            )
        if db:
            parts.append(
                db.name if isinstance(db, exp.Expression) else db
            )
        parts.append(resolved)
        parts.append(name)
        return ".".join(parts)
    return name


def _is_star_inside_function(star: exp.Star) -> bool:
    parent = star.parent
    while parent:
        if isinstance(parent, (exp.Func, exp.Anonymous)):
            return True
        if isinstance(parent, (exp.Select, exp.Where, exp.Order, exp.Group)):
            break
        parent = parent.parent
    return False


# ---------------------------------------------------------------------------
# Clause classification
# ---------------------------------------------------------------------------

def _classify_clause(key: str, parent_type: type) -> str:  # noqa: C901
    """Map an arg_types key + parent type to a columns_dict section name."""
    if key == "expressions":
        if parent_type is exp.Update:
            return "update"
        if parent_type is exp.Select:
            return "select"
        return ""
    if key == "where":
        return "where"
    if key in ("on", "using"):
        return "join"
    if key == "group":
        return "group_by"
    if key == "order":
        return "order_by"
    if key == "having":
        return "having"
    return ""


# ---------------------------------------------------------------------------
# Collector — accumulates results during AST walk
# ---------------------------------------------------------------------------

class _Collector:
    __slots__ = (
        "ta", "columns", "columns_dict", "alias_names",
        "alias_dict", "alias_map", "cte_names", "cte_alias_names",
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
        self.cte_alias_names: set = set()  # CTE column-def alias names
        self.subquery_items: list = []  # (depth, name)

    def add_column(self, name: str, clause: str) -> None:
        self.columns.append(name)
        if clause:
            self.columns_dict.setdefault(clause, UniqueList()).append(name)

    def add_alias(
        self, name: str, target, clause: str
    ) -> None:
        self.alias_names.append(name)
        if clause:
            self.alias_dict.setdefault(clause, UniqueList()).append(name)
        if target is not None:
            self.alias_map[name] = target


# ---------------------------------------------------------------------------
# AST walk — arg_types-order DFS
# ---------------------------------------------------------------------------

def _walk(node, c: _Collector, clause: str = "", depth: int = 0) -> None:  # noqa: C901
    """Walk AST in arg_types key order, collecting metadata."""
    if node is None:
        return

    # ---- Skip VALUES (literal values, not column references) ----
    if isinstance(node, exp.Values):
        return

    # ---- CTE: record name, handle column defs, walk body ----
    if isinstance(node, exp.CTE):
        _handle_cte(node, c, depth)
        return

    # ---- Subquery with alias: record name ----
    if isinstance(node, exp.Subquery) and node.alias:
        c.subquery_items.append((depth, node.alias))

    # ---- Column node ----
    if isinstance(node, exp.Column):
        _handle_column(node, c, clause)
        return

    # ---- Star (standalone, not inside Column or function) ----
    if isinstance(node, exp.Star):
        if not isinstance(node.parent, exp.Column) and not _is_star_inside_function(
            node
        ):
            c.add_column("*", clause)
        return

    # ---- ColumnDef (CREATE TABLE) ----
    if isinstance(node, exp.ColumnDef):
        c.add_column(node.name, clause)
        return

    # ---- Identifier in USING clause (not inside Column) ----
    if isinstance(node, exp.Identifier) and not isinstance(node.parent, (
        exp.Column, exp.Table, exp.TableAlias, exp.CTE,
    )):
        if clause == "join":
            c.add_column(node.name, clause)
        return

    # ---- Recurse into children in arg_types order ----
    if not hasattr(node, "arg_types"):
        return

    # Keys to skip (don't extract columns from these)
    _SKIP_KEYS = {"conflict", "returning", "alternative"}

    for key in node.arg_types:
        if key in _SKIP_KEYS:
            continue
        child = node.args.get(key)
        if child is None:
            continue

        new_clause = _classify_clause(key, type(node)) or clause

        # SELECT expressions may contain Alias nodes
        if key == "expressions" and isinstance(node, exp.Select):
            _handle_select_exprs(child, c, new_clause, depth)
            continue

        # INSERT Schema column names
        if isinstance(node, exp.Insert) and key == "this":
            schema = node.find(exp.Schema)
            if schema and schema.expressions:
                for col_id in schema.expressions:
                    name = col_id.name if hasattr(col_id, "name") else str(col_id)
                    c.add_column(name, "insert")
            continue

        # JOIN USING — extract column identifiers
        if key == "using" and isinstance(node, exp.Join):
            if isinstance(child, list):
                for item in child:
                    if hasattr(item, "name"):
                        c.add_column(item.name, "join")
            continue

        # Walk children
        if isinstance(child, list):
            for item in child:
                if isinstance(item, exp.Expression):
                    _walk(item, c, new_clause, depth + 1)
        elif isinstance(child, exp.Expression):
            _walk(child, c, new_clause, depth + 1)


# ---------------------------------------------------------------------------
# Node handlers
# ---------------------------------------------------------------------------

def _handle_column(col: exp.Column, c: _Collector, clause: str) -> None:
    """Handle a Column node, detecting CTE alias references."""
    star = col.find(exp.Star)
    if star:
        table = col.table
        if table:
            table = _resolve_table_alias(table, c.ta)
            c.add_column(f"{table}.*", clause)
        else:
            c.add_column("*", clause)
        return

    # Check for CTE column alias reference (e.g., query1.c2 where c2 is CTE alias)
    if col.table and col.table in c.cte_names and col.name in c.cte_alias_names:
        c.alias_dict.setdefault(clause, UniqueList()).append(col.name)
        return

    full = _column_full_name(col, c.ta)

    # Check if bare name is a known alias (used in WHERE/ORDER BY/GROUP BY)
    bare = col.name
    if not col.table and bare in c.alias_names:
        c.alias_dict.setdefault(clause, UniqueList()).append(bare)
        return

    c.add_column(full, clause)


def _handle_select_exprs(
    exprs, c: _Collector, clause: str, depth: int
) -> None:
    """Handle SELECT expression list, detecting aliases."""
    if not isinstance(exprs, list):
        return

    for expr in exprs:
        if isinstance(expr, exp.Alias):
            _handle_alias(expr, c, clause, depth)
        elif isinstance(expr, exp.Star):
            c.add_column("*", clause)
        elif isinstance(expr, exp.Column):
            _handle_column(expr, c, clause)
        else:
            # Complex expression (function, CASE, etc.) — extract columns
            cols = _flat_columns(expr, c.ta)
            for col in cols:
                c.add_column(col, clause)


def _handle_alias(
    alias_node: exp.Alias, c: _Collector, clause: str, depth: int
) -> None:
    """Handle an Alias in SELECT — extract inner columns and record alias."""
    alias_name = alias_node.alias
    inner = alias_node.this

    # For subqueries inside aliases, walk to collect nested aliases
    # but only use the immediate SELECT columns for the alias target
    select = inner.find(exp.Select)
    if select:
        _walk(inner, c, clause, depth + 1)
        target_cols = _flat_columns_select_only(select, c.ta)
        target = target_cols[0] if len(target_cols) == 1 else (
            target_cols if target_cols else None
        )
        c.add_alias(alias_name, target, clause)
        return

    inner_cols = _flat_columns(inner, c.ta)

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
        # Check if inner has a star in a function (e.g., COUNT(*) as alias)
        target = None
        if inner.find(exp.Star):
            target = "*"
        c.add_alias(alias_name, target, clause)


def _handle_cte(cte: exp.CTE, c: _Collector, depth: int) -> None:
    """Handle a CTE node — record name, extract body, handle column defs."""
    alias = cte.alias
    if not alias:
        return

    # Restore qualified name if placeholder was used
    c.cte_names.append(alias)

    table_alias = cte.args.get("alias")
    has_col_defs = table_alias and table_alias.columns
    body = cte.this

    if has_col_defs and body and isinstance(body, exp.Select):
        # CTE with column definitions: body cols + alias mapping
        body_cols = _flat_columns(body, c.ta)
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
        # CTE without column defs — walk query-like bodies
        _walk(body, c, "", depth + 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flat_columns_select_only(select: exp.Select, aliases: Dict[str, str]) -> list:
    """Extract column/alias names from a SELECT's immediate expressions only."""
    cols = []
    for expr in (select.expressions or []):
        if isinstance(expr, exp.Alias):
            cols.append(expr.alias)
        elif isinstance(expr, exp.Column):
            cols.append(_column_full_name(expr, aliases))
        elif isinstance(expr, exp.Star):
            cols.append("*")
        else:
            # Function or complex expression — extract column names
            for col_name in _flat_columns(expr, aliases):
                cols.append(col_name)
    return cols


def _flat_columns(node: exp.Expression, aliases: Dict[str, str]) -> list:  # noqa: C901
    """Extract all column names from an expression subtree (DFS)."""
    cols = []
    if node is None:
        return cols
    seen_stars = set()
    for child in _dfs(node):
        if isinstance(child, exp.Column):
            star = child.find(exp.Star)
            if star:
                seen_stars.add(id(star))
                table = child.table
                if table:
                    table = _resolve_table_alias(table, aliases)
                    cols.append(f"{table}.*")
                else:
                    cols.append("*")
            else:
                cols.append(_column_full_name(child, aliases))
        elif isinstance(child, exp.Star):
            if id(child) not in seen_stars and not isinstance(
                child.parent, exp.Column
            ):
                if not _is_star_inside_function(child):
                    cols.append("*")
    return cols


def _dfs(node: exp.Expression):
    yield node
    for child in node.iter_expressions():
        yield from _dfs(child)


# ---------------------------------------------------------------------------
# CTE / Subquery name extraction (also used standalone)
# ---------------------------------------------------------------------------

def extract_cte_names(ast: exp.Expression, cte_name_map: Dict = None) -> List[str]:
    """Extract CTE names from WITH clauses."""
    if ast is None:
        return []
    cte_name_map = cte_name_map or {}
    reverse_map = {v.replace(".", "__DOT__"): v for v in cte_name_map.values()}
    reverse_map.update(cte_name_map)
    names = UniqueList()
    for cte in ast.find_all(exp.CTE):
        alias = cte.alias
        if alias:
            names.append(reverse_map.get(alias, alias))
    return names


def extract_subquery_names(ast: exp.Expression) -> List[str]:
    """Extract aliased subquery names in post-order (children before parent)."""
    if ast is None:
        return []
    names = UniqueList()
    _collect_subqueries_postorder(ast, names)
    return names


def _collect_subqueries_postorder(node: exp.Expression, out: list) -> None:
    """Post-order DFS: yield children's subquery aliases before parent's."""
    for child in node.iter_expressions():
        _collect_subqueries_postorder(child, out)
    if isinstance(node, exp.Subquery) and node.alias:
        out.append(node.alias)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_all(  # noqa: C901
    ast: exp.Expression,
    table_aliases: Dict[str, str],
    cte_name_map: Dict = None,
) -> tuple:
    """
    Extract all metadata from AST in a single pass.

    Returns:
        (columns, columns_dict, alias_names, alias_dict, alias_map,
         cte_names, subquery_names)
    """
    if ast is None:
        return [], {}, [], None, {}, [], []

    cte_name_map = cte_name_map or {}

    c = _Collector(table_aliases)

    # Seed CTE names for alias detection (needed before walk)
    reverse_map = {v.replace(".", "__DOT__"): v for v in cte_name_map.values()}
    reverse_map.update(cte_name_map)
    for cte in ast.find_all(exp.CTE):
        alias = cte.alias
        if alias:
            c.cte_names.append(reverse_map.get(alias, alias))

    # Handle CREATE TABLE with column defs (no SELECT)
    if isinstance(ast, exp.Create) and not ast.find(exp.Select):
        for col_def in ast.find_all(exp.ColumnDef):
            c.add_column(col_def.name, "")
        return _result(c)

    # Reset cte_names — walk will re-collect them in order
    c.cte_names = UniqueList()

    # Walk AST
    _walk(ast, c)

    # Restore qualified CTE names
    final_cte = UniqueList()
    for name in c.cte_names:
        final_cte.append(reverse_map.get(name, name))

    # Sort subquery names by depth (inner first)
    c.subquery_items.sort(key=lambda x: -x[0])
    subquery_names = UniqueList()
    for _, name in c.subquery_items:
        subquery_names.append(name)

    alias_dict = c.alias_dict if c.alias_dict else None
    return (
        c.columns,
        c.columns_dict,
        c.alias_names,
        alias_dict,
        c.alias_map,
        final_cte,
        subquery_names,
    )


def _result(c: _Collector) -> tuple:
    alias_dict = c.alias_dict if c.alias_dict else None
    c.subquery_items.sort(key=lambda x: -x[0])
    subquery_names = UniqueList()
    for _, name in c.subquery_items:
        subquery_names.append(name)
    return (
        c.columns,
        c.columns_dict,
        c.alias_names,
        alias_dict,
        c.alias_map,
        c.cte_names,
        subquery_names,
    )
