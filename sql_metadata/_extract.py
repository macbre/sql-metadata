"""Single-pass SQL metadata extraction from a sqlglot AST.

Walks the AST in ``arg_types``-key order (which mirrors the left-to-right
SQL text order) and collects columns, column aliases, CTE names, and
subquery names into a :class:`_Collector` accumulator.  This module
replaces the earlier multi-pass ``_columns.py``, ``_ctes.py``, and
``_subqueries.py`` modules with a single DFS walk, reducing redundant
tree traversals and keeping the extraction order consistent.

The public entry point is :func:`extract_all`, which returns a 7-tuple
of metadata consumed by :attr:`Parser.columns` and friends.
"""

from typing import Dict, List, Union

from sqlglot import exp

from sql_metadata.utils import UniqueList

# ---------------------------------------------------------------------------
# Column name helpers
# ---------------------------------------------------------------------------


def _resolve_table_alias(col_table: str, aliases: Dict[str, str]) -> str:
    """Replace a table alias with the real table name if one is mapped.

    :param col_table: Table qualifier on a column (may be an alias).
    :type col_table: str
    :param aliases: Table alias → real name mapping.
    :type aliases: Dict[str, str]
    :returns: The real table name, or *col_table* unchanged if not aliased.
    :rtype: str
    """
    return aliases.get(col_table, col_table)


def _column_full_name(col: exp.Column, aliases: Dict[str, str]) -> str:
    """Build a fully-qualified column name with the table alias resolved.

    Assembles ``catalog.db.table.column`` from the ``exp.Column`` node,
    resolving the table part through *aliases*.  Strips trailing ``#``
    characters that MSSQL template delimiters leave on column names.

    :param col: sqlglot Column AST node.
    :type col: exp.Column
    :param aliases: Table alias → real name mapping.
    :type aliases: Dict[str, str]
    :returns: Dot-joined column name (e.g. ``"users.id"``).
    :rtype: str
    """
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
            parts.append(db.name if isinstance(db, exp.Expression) else db)
        parts.append(resolved)
        parts.append(name)
        return ".".join(parts)
    return name


def _is_star_inside_function(star: exp.Star) -> bool:
    """Determine whether a ``*`` node is inside a function call.

    ``COUNT(*)`` should **not** emit a ``*`` column — only bare
    ``SELECT *`` should.  This helper walks up the parent chain looking
    for ``exp.Func`` or ``exp.Anonymous`` (user-defined function) nodes
    before hitting a clause boundary (``Select``, ``Where``, etc.).

    :param star: sqlglot Star AST node.
    :type star: exp.Star
    :returns: ``True`` if the star is an argument to a function.
    :rtype: bool
    """
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

    The ``"expressions"`` key appears under both ``SELECT`` and ``UPDATE``
    nodes.  This helper disambiguates them.

    :param parent_type: The type of the parent AST node.
    :type parent_type: type
    :returns: ``"update"``, ``"select"``, or ``""`` for other parents.
    :rtype: str
    """
    if parent_type is exp.Update:
        return "update"
    if parent_type is exp.Select:
        return "select"
    return ""


def _classify_clause(key: str, parent_type: type) -> str:
    """Map an ``arg_types`` key and parent node type to a ``columns_dict`` section.

    During the DFS walk each child is reached via a specific ``arg_types``
    key (``"where"``, ``"expressions"``, ``"on"``, etc.).  This function
    translates that key into the user-facing section name used in
    :attr:`Parser.columns_dict` (e.g. ``"where"``, ``"select"``,
    ``"join"``).

    :param key: The ``arg_types`` key through which the child was reached.
    :type key: str
    :param parent_type: The type of the parent AST node.
    :type parent_type: type
    :returns: Section name string, or ``""`` if the key does not map to a
        known section.
    :rtype: str
    """
    if key == "expressions":
        return _classify_expressions_clause(parent_type)
    if key in _JOIN_KEYS:
        return "join"
    return _CLAUSE_MAP.get(key, "")


# ---------------------------------------------------------------------------
# Collector — accumulates results during AST walk
# ---------------------------------------------------------------------------


class _Collector:
    """Mutable accumulator for metadata gathered during the AST walk.

    Instantiated once per :func:`extract_all` call and passed through
    every recursive :func:`_walk` invocation.  Using a dedicated object
    (rather than returning tuples from each recursive call) avoids
    allocating intermediate containers and makes the walk functions
    simpler.

    :param table_aliases: Pre-computed table alias → real name mapping
        from :func:`extract_table_aliases`.
    :type table_aliases: Dict[str, str]
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
        """Initialise empty collection containers.

        :param table_aliases: Table alias → real name mapping.
        :type table_aliases: Dict[str, str]
        """
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
        """Record a column name, filing it into the appropriate section.

        :param name: Column name (possibly table-qualified, e.g. ``"t.id"``).
        :type name: str
        :param clause: Section name (``"select"``, ``"where"``, etc.) or
            ``""`` if the clause is unknown.
        :type clause: str
        :returns: Nothing.
        :rtype: None
        """
        self.columns.append(name)
        if clause:
            self.columns_dict.setdefault(clause, UniqueList()).append(name)

    def add_alias(self, name: str, target, clause: str) -> None:
        """Record a column alias and its target expression.

        :param name: The alias name (e.g. ``"total"``).
        :type name: str
        :param target: The column(s) the alias refers to — a single string,
            a list of strings, or ``None`` if not resolvable.
        :type target: Optional[Union[str, list]]
        :param clause: Section name for the alias.
        :type clause: str
        :returns: Nothing.
        :rtype: None
        """
        self.alias_names.append(name)
        if clause:
            self.alias_dict.setdefault(clause, UniqueList()).append(name)
        if target is not None:
            self.alias_map[name] = target


# ---------------------------------------------------------------------------
# AST walk — arg_types-order DFS
# ---------------------------------------------------------------------------


#: arg_types keys to skip during the walk (no column references).
_SKIP_KEYS = frozenset({"conflict", "returning", "alternative"})


def _handle_identifier_node(node: exp.Identifier, c: _Collector, clause: str) -> None:
    """Handle an ``Identifier`` in a USING clause (not inside a ``Column``).

    Only adds the identifier as a column when the current clause is
    ``"join"`` and the identifier is not part of a Column, Table,
    TableAlias, or CTE node.

    :param node: Identifier AST node.
    :type node: exp.Identifier
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current clause section name.
    :type clause: str
    """
    if not isinstance(
        node.parent,
        (exp.Column, exp.Table, exp.TableAlias, exp.CTE),
    ):
        if clause == "join":
            c.add_column(node.name, clause)


def _handle_insert_schema(node: exp.Insert, c: _Collector) -> None:
    """Extract column names from the ``Schema`` of an ``INSERT`` statement.

    :param node: Insert AST node.
    :type node: exp.Insert
    :param c: Shared collector.
    :type c: _Collector
    """
    schema = node.find(exp.Schema)
    if schema and schema.expressions:
        for col_id in schema.expressions:
            name = col_id.name if hasattr(col_id, "name") else str(col_id)
            c.add_column(name, "insert")


def _handle_join_using(child, c: _Collector) -> None:
    """Extract column identifiers from a ``JOIN USING`` clause.

    :param child: The ``using`` child value (typically a list).
    :param c: Shared collector.
    :type c: _Collector
    """
    if isinstance(child, list):
        for item in child:
            if hasattr(item, "name"):
                c.add_column(item.name, "join")


def _process_child_key(
    node: exp.Expression,
    key: str,
    child,
    c: _Collector,
    clause: str,
    depth: int,
) -> bool:
    """Handle a single ``arg_types`` child during the walk.

    Dispatches special cases for SELECT expressions, INSERT schema
    columns, and JOIN USING identifiers.  Returns ``True`` if the
    child was fully handled (caller should ``continue``), ``False``
    for default recursive walk behaviour.

    :param node: Parent AST node.
    :type node: exp.Expression
    :param key: The ``arg_types`` key for this child.
    :type key: str
    :param child: The child value (expression or list).
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current clause section name.
    :type clause: str
    :param depth: Current recursion depth.
    :type depth: int
    :returns: ``True`` if handled, ``False`` otherwise.
    :rtype: bool
    """
    if key == "expressions" and isinstance(node, exp.Select):
        _handle_select_exprs(child, c, clause, depth)
        return True
    if isinstance(node, exp.Insert) and key == "this":
        _handle_insert_schema(node, c)
        return True
    if key == "using" and isinstance(node, exp.Join):
        _handle_join_using(child, c)
        return True
    return False


def _handle_star_node(node: exp.Star, c: _Collector, clause: str) -> None:
    """Handle a standalone ``Star`` node (not inside a ``Column`` or function).

    :param node: Star AST node.
    :type node: exp.Star
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current clause section name.
    :type clause: str
    """
    if not isinstance(node.parent, exp.Column) and not _is_star_inside_function(node):
        c.add_column("*", clause)


def _dispatch_leaf_node(node, c: _Collector, clause: str, depth: int) -> bool:
    """Dispatch leaf-like AST nodes to their specialised handlers.

    Returns ``True`` if the node was fully handled and the walk should
    not recurse into children.  Returns ``False`` if the walk should
    continue into children (e.g. for ``Subquery`` nodes where only the
    alias is recorded).

    :param node: Current AST node.
    :type node: exp.Expression
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current clause section name.
    :type clause: str
    :param depth: Current recursion depth.
    :type depth: int
    :returns: ``True`` if handled (stop recursion), ``False`` to continue.
    :rtype: bool
    """
    if isinstance(node, (exp.Values, exp.Star, exp.ColumnDef, exp.Identifier)):
        if isinstance(node, exp.Star):
            _handle_star_node(node, c, clause)
        elif isinstance(node, exp.ColumnDef):
            c.add_column(node.name, clause)
        elif isinstance(node, exp.Identifier):
            _handle_identifier_node(node, c, clause)
        return True
    if isinstance(node, exp.CTE):
        _handle_cte(node, c, depth)
        return True
    if isinstance(node, exp.Column):
        _handle_column(node, c, clause)
        return True
    if isinstance(node, exp.Subquery) and node.alias:
        c.subquery_items.append((depth, node.alias))
    return False


def _recurse_child(child, c: _Collector, clause: str, depth: int) -> None:
    """Recursively walk a child value (single expression or list).

    :param child: A child expression or list of expressions.
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current clause section name.
    :type clause: str
    :param depth: Current recursion depth.
    :type depth: int
    """
    if isinstance(child, list):
        for item in child:
            if isinstance(item, exp.Expression):
                _walk(item, c, clause, depth + 1)
    elif isinstance(child, exp.Expression):
        _walk(child, c, clause, depth + 1)


def _walk_children(node, c: _Collector, clause: str, depth: int) -> None:
    """Recurse into the children of *node* in ``arg_types`` key order.

    Skips keys in :data:`_SKIP_KEYS` and delegates special cases to
    :func:`_process_child_key` before falling through to the default
    recursive walk.

    :param node: Parent AST node with ``arg_types``.
    :type node: exp.Expression
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current clause section name.
    :type clause: str
    :param depth: Current recursion depth.
    :type depth: int
    """
    for key in node.arg_types:
        if key in _SKIP_KEYS:
            continue
        child = node.args.get(key)
        if child is None:
            continue

        new_clause = _classify_clause(key, type(node)) or clause

        if not _process_child_key(node, key, child, c, new_clause, depth):
            _recurse_child(child, c, new_clause, depth)


def _walk(node, c: _Collector, clause: str = "", depth: int = 0) -> None:
    """Depth-first walk of the AST in ``arg_types`` key order.

    Dispatches to specialised handlers for ``Column``, ``Star``, ``CTE``,
    ``Subquery``, ``ColumnDef``, and ``Identifier`` (USING clause) nodes.
    For all other node types it recurses into children using the
    ``arg_types`` ordering, which mirrors the SQL text order.

    :param node: Current AST node (or ``None``).
    :type node: Optional[exp.Expression]
    :param c: Shared collector accumulating extraction results.
    :type c: _Collector
    :param clause: Current ``columns_dict`` section name, inherited from
        the parent unless overridden by :func:`_classify_clause`.
    :type clause: str
    :param depth: Recursion depth, used to sort subqueries (inner first).
    :type depth: int
    :returns: Nothing — results are accumulated in *c*.
    :rtype: None
    """
    if node is None:
        return

    if _dispatch_leaf_node(node, c, clause, depth):
        return

    if hasattr(node, "arg_types"):
        _walk_children(node, c, clause, depth)


# ---------------------------------------------------------------------------
# Node handlers
# ---------------------------------------------------------------------------


def _handle_column(col: exp.Column, c: _Collector, clause: str) -> None:
    """Handle a ``Column`` AST node during the walk.

    Special cases:

    * **Star columns** (``table.*``) — emitted with the table prefix.
    * **CTE alias references** — when a column's table qualifier matches a
      known CTE name and the column name matches a CTE column-definition
      alias, it is recorded as an alias reference rather than a column.
    * **Bare alias references** — columns without a table qualifier whose
      name matches a previously seen alias (e.g. ``ORDER BY alias_name``)
      are filed into ``alias_dict`` instead of ``columns``.

    :param col: sqlglot Column node.
    :type col: exp.Column
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current ``columns_dict`` section name.
    :type clause: str
    :returns: Nothing.
    :rtype: None
    """
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


def _handle_select_exprs(exprs, c: _Collector, clause: str, depth: int) -> None:
    """Handle the ``expressions`` list of a ``SELECT`` clause.

    Dispatches each expression to the appropriate handler:

    * ``Alias`` → :func:`_handle_alias`
    * ``Star``  → record ``*`` column
    * ``Column`` → :func:`_handle_column`
    * Anything else (functions, CASE, sub-expressions) → extract columns
      via :func:`_flat_columns`.

    :param exprs: List of expressions from ``Select.args["expressions"]``.
    :type exprs: list
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current section name (typically ``"select"``).
    :type clause: str
    :param depth: Current recursion depth.
    :type depth: int
    :returns: Nothing.
    :rtype: None
    """
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
    """Handle an ``Alias`` node inside a ``SELECT`` expression list.

    Extracts the inner columns that the alias refers to, records them as
    columns, and registers the alias itself.  For subquery aliases the
    inner ``SELECT``'s immediate expressions are used as the alias target
    (not the deeply-nested columns).

    Self-aliases (``SELECT col AS col``) are detected and **not** recorded
    as aliases to avoid polluting :attr:`Parser.columns_aliases`.

    :param alias_node: sqlglot Alias AST node.
    :type alias_node: exp.Alias
    :param c: Shared collector.
    :type c: _Collector
    :param clause: Current section name.
    :type clause: str
    :param depth: Current recursion depth.
    :type depth: int
    :returns: Nothing.
    :rtype: None
    """
    alias_name = alias_node.alias
    inner = alias_node.this

    # For subqueries inside aliases, walk to collect nested aliases
    # but only use the immediate SELECT columns for the alias target
    select = inner.find(exp.Select)
    if select:
        _walk(inner, c, clause, depth + 1)
        target_cols = _flat_columns_select_only(select, c.ta)
        target = (
            target_cols[0]
            if len(target_cols) == 1
            else (target_cols if target_cols else None)
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
    """Handle a ``CTE`` (Common Table Expression) AST node.

    Records the CTE name, then either:

    * **With column definitions** (``WITH cte(c1, c2) AS (...)``): extracts
      body columns, builds alias mappings from CTE column names to body
      columns, and registers the CTE column names as aliases.
    * **Without column definitions**: recursively walks the CTE body via
      :func:`_walk`.

    :param cte: sqlglot CTE AST node.
    :type cte: exp.CTE
    :param c: Shared collector.
    :type c: _Collector
    :param depth: Current recursion depth.
    :type depth: int
    :returns: Nothing.
    :rtype: None
    """
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
    elif body and isinstance(body, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        # CTE without column defs — walk query-like bodies
        _walk(body, c, "", depth + 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _flat_columns_select_only(select: exp.Select, aliases: Dict[str, str]) -> list:
    """Extract column/alias names from a ``SELECT``'s immediate expressions.

    Unlike :func:`_flat_columns`, this does **not** recurse into
    sub-expressions — it only looks at the top-level expression list.
    Used by :func:`_handle_alias` to determine the alias target for
    subquery aliases.

    :param select: sqlglot Select AST node.
    :type select: exp.Select
    :param aliases: Table alias → real name mapping.
    :type aliases: Dict[str, str]
    :returns: List of column or alias names.
    :rtype: list
    """
    cols = []
    for expr in select.expressions or []:
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


# Functions whose first argument is a date-part unit keyword, not a column.
_DATE_PART_FUNCTIONS = frozenset({
    "dateadd", "datediff", "datepart", "datename", "date_add", "date_sub",
    "date_diff", "date_trunc", "timestampadd", "timestampdiff",
})


def _is_date_part_unit(node: exp.Column) -> bool:
    """Return True if *node* is the first arg of a date-part function."""
    parent = node.parent
    if isinstance(parent, exp.Anonymous) and parent.this.lower() in _DATE_PART_FUNCTIONS:
        exprs = parent.expressions
        return len(exprs) > 0 and exprs[0] is node
    return False


def _collect_column_from_dfs_node(
    child: exp.Expression, aliases: Dict[str, str], seen_stars: set
) -> Union[str, None]:
    """Extract a column name from a single DFS node.

    Handles ``Column`` nodes (including table-qualified stars like
    ``t.*``) and standalone ``Star`` nodes.  Returns ``None`` if the
    node does not represent a column reference.

    :param child: A DFS-visited AST node.
    :type child: exp.Expression
    :param aliases: Table alias → real name mapping.
    :type aliases: Dict[str, str]
    :param seen_stars: Mutable set of ``id()`` values for ``Star`` nodes
        already accounted for inside ``Column`` nodes.
    :type seen_stars: set
    :returns: Column name string, or ``None`` to skip.
    :rtype: Union[str, None]
    """
    if isinstance(child, exp.Column):
        if _is_date_part_unit(child):
            return None
        star = child.find(exp.Star)
        if star:
            seen_stars.add(id(star))
            table = child.table
            if table:
                table = _resolve_table_alias(table, aliases)
                return f"{table}.*"
            return "*"
        return _column_full_name(child, aliases)
    if isinstance(child, exp.Star):
        if id(child) not in seen_stars and not isinstance(child.parent, exp.Column):
            if not _is_star_inside_function(child):
                return "*"
    return None


def _flat_columns(node: exp.Expression, aliases: Dict[str, str]) -> list:
    """Extract all column names from an expression subtree via DFS.

    Traverses the subtree rooted at *node* and collects every ``Column``
    and standalone ``Star`` node.  Stars inside function calls (e.g.
    ``COUNT(*)``) are excluded via :func:`_is_star_inside_function`.

    :param node: Root of the expression subtree to scan.
    :type node: exp.Expression
    :param aliases: Table alias → real name mapping.
    :type aliases: Dict[str, str]
    :returns: List of column name strings (may contain duplicates).
    :rtype: list
    """
    cols = []
    if node is None:
        return cols
    seen_stars = set()
    for child in _dfs(node):
        name = _collect_column_from_dfs_node(child, aliases, seen_stars)
        if name is not None:
            cols.append(name)
    return cols


def _dfs(node: exp.Expression):
    """Yield *node* and all its descendants in depth-first order.

    A simple recursive generator used by :func:`_flat_columns` to
    traverse expression subtrees without the overhead of sqlglot's
    built-in ``walk()`` (which also yields parent and key metadata).

    :param node: Root expression node.
    :type node: exp.Expression
    :yields: Each expression node in DFS pre-order.
    :rtype: Generator[exp.Expression]
    """
    yield node
    for child in node.iter_expressions():
        yield from _dfs(child)


# ---------------------------------------------------------------------------
# CTE / Subquery name extraction (also used standalone)
# ---------------------------------------------------------------------------


def extract_cte_names(ast: exp.Expression, cte_name_map: Dict = None) -> List[str]:
    """Extract CTE (Common Table Expression) names from the AST.

    Iterates over all ``exp.CTE`` nodes and collects their alias names.
    If a CTE name was normalised by :func:`_normalize_cte_names` (i.e. a
    dotted name was replaced with a placeholder), the original qualified
    name is restored via *cte_name_map*.

    Called by :attr:`Parser.with_names` and seeded at the start of
    :func:`extract_all`.

    :param ast: Root AST node (may be ``None``).
    :type ast: Optional[exp.Expression]
    :param cte_name_map: Placeholder → original qualified name mapping.
    :type cte_name_map: Optional[Dict]
    :returns: Ordered list of CTE names.
    :rtype: List[str]
    """
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
    """Extract aliased subquery names from the AST in post-order.

    Post-order traversal ensures that inner (deeper) subquery aliases
    appear before outer ones, which is the order needed for correct
    column resolution in :meth:`Parser._resolve_sub_queries`.

    Called by :attr:`Parser.subqueries_names`.

    :param ast: Root AST node (may be ``None``).
    :type ast: Optional[exp.Expression]
    :returns: Ordered list of subquery alias names (inner first).
    :rtype: List[str]
    """
    if ast is None:
        return []
    names = UniqueList()
    _collect_subqueries_postorder(ast, names)
    return names


def _collect_subqueries_postorder(node: exp.Expression, out: list) -> None:
    """Recursively collect subquery aliases in post-order.

    Children are visited before the parent so that innermost subqueries
    appear first in *out*.

    :param node: Current AST node.
    :type node: exp.Expression
    :param out: Mutable list to which alias names are appended.
    :type out: list
    :returns: Nothing — modifies *out* in place.
    :rtype: None
    """
    for child in node.iter_expressions():
        _collect_subqueries_postorder(child, out)
    if isinstance(node, exp.Subquery) and node.alias:
        out.append(node.alias)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _build_reverse_cte_map(cte_name_map: Dict) -> Dict[str, str]:
    """Build a reverse mapping from placeholder CTE names to originals.

    Handles ``__DOT__`` placeholder replacement used to normalise
    qualified CTE names for sqlglot parsing.

    :param cte_name_map: Placeholder → original qualified name mapping.
    :type cte_name_map: Dict
    :returns: Combined reverse mapping.
    :rtype: Dict[str, str]
    """
    reverse_map = {v.replace(".", "__DOT__"): v for v in cte_name_map.values()}
    reverse_map.update(cte_name_map)
    return reverse_map


def _seed_cte_names(
    ast: exp.Expression, c: _Collector, reverse_map: Dict[str, str]
) -> None:
    """Pre-populate CTE names in the collector for alias detection.

    :param ast: Root AST node.
    :type ast: exp.Expression
    :param c: Shared collector to seed.
    :type c: _Collector
    :param reverse_map: Placeholder → original CTE name mapping.
    :type reverse_map: Dict[str, str]
    """
    for cte in ast.find_all(exp.CTE):
        alias = cte.alias
        if alias:
            c.cte_names.append(reverse_map.get(alias, alias))


def _build_subquery_names(c: _Collector) -> "UniqueList":
    """Sort subquery items by depth (innermost first) and build a names list.

    :param c: Collector with accumulated subquery items.
    :type c: _Collector
    :returns: Ordered unique list of subquery alias names.
    :rtype: UniqueList
    """
    c.subquery_items.sort(key=lambda x: -x[0])
    names = UniqueList()
    for _, name in c.subquery_items:
        names.append(name)
    return names


def extract_all(
    ast: exp.Expression,
    table_aliases: Dict[str, str],
    cte_name_map: Dict = None,
) -> tuple:
    """Extract all column metadata from the AST in a single pass.

    Performs a full :func:`_walk` over the AST and returns a 7-tuple of
    extraction results consumed by :attr:`Parser.columns` and related
    properties.  CTE names are seeded before the walk so that
    :func:`_handle_column` can detect CTE alias references.

    For ``CREATE TABLE`` statements without a ``SELECT`` (pure DDL), only
    ``ColumnDef`` nodes are collected — no walk is needed.

    :param ast: Root AST node (may be ``None``).
    :type ast: Optional[exp.Expression]
    :param table_aliases: Table alias → real name mapping.
    :type table_aliases: Dict[str, str]
    :param cte_name_map: Placeholder → original qualified CTE name mapping.
    :type cte_name_map: Optional[Dict]
    :returns: A 7-tuple of ``(columns, columns_dict, alias_names,
        alias_dict, alias_map, cte_names, subquery_names)``.
    :rtype: tuple
    """
    if ast is None:
        return [], {}, [], None, {}, [], []

    cte_name_map = cte_name_map or {}
    c = _Collector(table_aliases)
    reverse_map = _build_reverse_cte_map(cte_name_map)

    _seed_cte_names(ast, c, reverse_map)

    # Handle CREATE TABLE with column defs (no SELECT)
    if isinstance(ast, exp.Create) and not ast.find(exp.Select):
        for col_def in ast.find_all(exp.ColumnDef):
            c.add_column(col_def.name, "")
        return _result(c)

    # Reset cte_names — walk will re-collect them in order
    c.cte_names = UniqueList()
    _walk(ast, c)

    # Restore qualified CTE names
    final_cte = UniqueList()
    for name in c.cte_names:
        final_cte.append(reverse_map.get(name, name))

    alias_dict = c.alias_dict if c.alias_dict else None
    return (
        c.columns,
        c.columns_dict,
        c.alias_names,
        alias_dict,
        c.alias_map,
        final_cte,
        _build_subquery_names(c),
    )


def _result(c: _Collector) -> tuple:
    """Build the standard 7-tuple result from a :class:`_Collector`.

    Shared by :func:`extract_all` for the early-return ``CREATE TABLE``
    path and the normal walk path.

    :param c: Populated collector.
    :type c: _Collector
    :returns: Same 7-tuple as :func:`extract_all`.
    :rtype: tuple
    """
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
