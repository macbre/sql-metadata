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
from typing import Any

from sqlglot import exp

from sql_metadata.exceptions import InvalidQueryDefinition
from sql_metadata.utils import UniqueList, last_segment

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionResult:
    """Immutable container for column extraction results.

    Returned by :meth:`ColumnExtractor.extract` and consumed by
    :class:`Parser` to populate its column/alias/CTE properties.
    Each field corresponds to a public ``Parser`` property.
    """

    columns: UniqueList
    columns_dict: dict[str, UniqueList]
    alias_names: UniqueList
    alias_dict: dict[str, UniqueList]
    alias_map: dict[str, str | list[str]]
    cte_names: UniqueList
    subquery_names: UniqueList
    output_columns: list[str]


# ---------------------------------------------------------------------------
# Clause classification (pure functions, no state)
# ---------------------------------------------------------------------------


#: Simple key → clause-name lookup for most ``arg_types`` keys.
_CLAUSE_MAP: dict[str, str] = {
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


def _dfs(node: exp.Expression) -> Any:
    """Yield *node* and all its descendants in depth-first order.

    :param node: Root expression node.
    :yields: Each expression node in DFS pre-order.
    """
    yield node
    for child in node.iter_expressions():
        yield from _dfs(child)


#: Functions whose first argument is a date-part unit keyword, not a column.
_DATE_PART_FUNCTIONS = frozenset(
    {
        "dateadd",
        "datediff",
        "datepart",
        "datename",
        "date_add",
        "date_sub",
        "date_diff",
        "date_trunc",
        "timestampadd",
        "timestampdiff",
    }
)


def _is_date_part_unit(node: exp.Column) -> bool:
    """Return ``True`` if *node* is the date-part unit argument of a function.

    Functions like ``DATEADD``, ``DATEDIFF``, and ``DATE_TRUNC`` accept a
    date-part keyword (``DAY``, ``MONTH``, …) as their first argument.
    sqlglot parses these keywords as ``exp.Column`` nodes, but they are not
    real columns and must be skipped during extraction.

    :param node: A column AST node to inspect.
    :type node: exp.Column
    :rtype: bool
    """
    parent = node.parent
    if (
        isinstance(parent, exp.Anonymous)
        and parent.this.lower() in _DATE_PART_FUNCTIONS
    ):
        exprs = parent.expressions
        return len(exprs) > 0 and exprs[0] is node
    return False


# ---------------------------------------------------------------------------
# Collector — accumulates results during AST walk
# ---------------------------------------------------------------------------


class _Collector:
    """Mutable accumulator for metadata gathered during the AST walk.

    :param table_aliases: Pre-computed table alias → real name mapping.
    """

    __slots__ = (
        "columns",
        "columns_dict",
        "alias_names",
        "alias_dict",
        "alias_map",
        "cte_names",
        "cte_alias_names",
        "subquery_items",
        "output_columns",
    )

    def __init__(self) -> None:
        self.columns = UniqueList()
        self.columns_dict: dict[str, UniqueList] = {}
        self.alias_names = UniqueList()
        self.alias_dict: dict[str, UniqueList] = {}
        self.alias_map: dict[str, str | list[str]] = {}
        self.cte_names = UniqueList()
        self.cte_alias_names: set[str] = set()
        self.subquery_items: list[tuple[int, str]] = []
        self.output_columns: list[str] = []

    def add_column(self, name: str, clause: str) -> None:
        """Record a column name, filing it into the appropriate clause section.

        :param name: The column name to record.
        :type name: str
        :param clause: The SQL clause section (e.g. ``"select"``, ``"where"``).
        :type clause: str
        """
        self.columns.append(name)
        if clause:
            self.columns_dict.setdefault(clause, UniqueList()).append(name)

    def add_alias(self, name: str, target: Any, clause: str) -> None:
        """Record a column alias and its target expression.

        :param name: The alias name.
        :type name: str
        :param target: The source column name or expression the alias refers
            to, or ``None`` if not determinable.
        :type target: Any
        :param clause: The SQL clause section where the alias was defined.
        :type clause: str
        """
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

    Walks the AST in ``arg_types``-key order (which mirrors the left-to-right
    SQL text order) and collects all metadata into an internal
    :class:`_Collector`.  Call :meth:`extract` to run the walk and return an
    :class:`ExtractionResult`.

    The class is designed around a single public entry point
    (:meth:`extract`), which triggers a recursive depth-first traversal of
    the sqlglot AST.  Specialised handler methods process leaf-like nodes
    (columns, aliases, CTEs, subqueries) while the walk engine manages
    clause classification and child iteration.

    :param ast: Root sqlglot AST node (e.g. ``Select``, ``Insert``,
        ``Create``).
    :param table_aliases: Pre-computed mapping of table alias names to
        their real (resolved) table names.
    :param cte_name_map: Optional mapping of placeholder CTE names
        (produced by :class:`SqlCleaner`) back to the original qualified
        CTE names.
    """

    def __init__(
        self,
        ast: exp.Expression,
        table_aliases: dict[str, str],
        cte_name_map: dict[str, str] | None = None,
    ):
        self._ast = ast
        self._table_aliases = table_aliases
        self._cte_name_map = cte_name_map or {}
        self._collector = _Collector()
        self._cte_restore_map = self._cte_name_map

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def extract(self) -> ExtractionResult:
        """Run the full extraction walk and return an immutable result.

        Orchestrates the three-phase extraction process:

        1. **Seed** — pre-populate CTE names so downstream handlers can
           recognise CTE column-alias references.
        2. **Walk** — depth-first traversal of the AST, dispatching each
           node to the appropriate handler.
        3. **Finalise** — restore qualified CTE names, sort subquery
           names, and package everything into an :class:`ExtractionResult`.

        For ``CREATE TABLE`` statements without a ``SELECT`` body (pure
        DDL), only ``ColumnDef`` nodes are collected during the walk.

        Example SQL::

            SELECT a, b FROM t WHERE a > 1

        :returns: An :class:`ExtractionResult` containing columns,
            aliases, CTE names, subquery names, and output columns.
        """
        c = self._collector

        self._seed_cte_names()

        # Reset cte_names — walk will re-collect them in text order
        c.cte_names = UniqueList()
        self._walk(self._ast)

        # Restore qualified CTE names (reverse placeholder mapping)
        final_cte = UniqueList()
        for name in c.cte_names:
            final_cte.append(self._cte_restore_map.get(name, name))

        alias_dict = c.alias_dict
        return ExtractionResult(
            columns=c.columns,
            columns_dict=c.columns_dict,
            alias_names=c.alias_names,
            alias_dict=alias_dict,
            alias_map=c.alias_map,
            cte_names=final_cte,
            subquery_names=self._build_subquery_names(),
            output_columns=c.output_columns,
        )

    # -------------------------------------------------------------------
    # Setup helpers
    # -------------------------------------------------------------------

    def _seed_cte_names(self) -> None:
        """Pre-populate CTE names in the collector before the main walk.

        Scans the AST for all ``CTE`` nodes and records their alias
        names.  This allows :meth:`_handle_column` to recognize
        references like ``cte_name.col`` as CTE column-alias references
        rather than regular columns.

        Example SQL::

            WITH sales AS (SELECT id FROM orders) SELECT sales.id FROM sales

        The seed step records ``"sales"`` so that ``sales.id`` in the
        outer SELECT can be identified as a CTE-qualified reference.
        """
        for cte in self._ast.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                self._collector.cte_names.append(
                    self._cte_restore_map.get(alias, alias)
                )

    def _build_subquery_names(self) -> UniqueList:
        """Sort collected subquery items by depth and return their names.

        Subqueries are collected during the walk with their nesting
        depth.  This method sorts them innermost-first (descending depth)
        and returns a :class:`UniqueList` of alias names in that order.

        Example SQL::

            SELECT (SELECT 1) AS a, (SELECT 2) AS b FROM t

        :returns: A :class:`UniqueList` of subquery alias names, ordered
            from innermost to outermost.
        """
        c = self._collector
        c.subquery_items.sort(key=lambda x: -x[0])
        names = UniqueList()
        for _, name in c.subquery_items:
            names.append(name)
        return names

    # -------------------------------------------------------------------
    # DFS walk engine
    # -------------------------------------------------------------------

    def _walk(
        self, node: exp.Expression, clause: str = "", depth: int = 0
    ) -> None:
        """Perform a depth-first walk of the AST in ``arg_types`` key order.

        This is the core recursive method.  For each node it first
        attempts leaf dispatch via :meth:`_dispatch_leaf`.  If the node
        is not a leaf, it iterates the node's ``arg_types`` keys in
        declaration order (which mirrors SQL text order) and recurses
        into each populated child.

        :param node: The current AST node to process.
        :param clause: The current SQL clause context (e.g. ``"select"``,
            ``"where"``).  Propagated to child nodes and used to file
            columns into ``columns_dict`` sections.
        :param depth: Current nesting depth, used to sort subqueries by
            depth (innermost first).
        """
        assert node is not None

        if self._dispatch_leaf(node, clause, depth):
            return

        if hasattr(node, "arg_types"):
            self._walk_children(node, clause, depth)

    def _walk_children(self, node: exp.Expression, clause: str, depth: int) -> None:
        """Iterate and recurse into children of *node* in ``arg_types`` key order.

        For each child key, determines the SQL clause context (e.g.
        ``"where"`` → ``where``, ``"on"`` → ``join``) via
        :func:`_classify_clause`.  Special-case keys (SELECT expressions,
        INSERT schema, JOIN USING) are routed to dedicated handlers via
        :meth:`_process_child_key`; all others get the default recursive
        walk via :meth:`_recurse_child`.

        :param node: Parent AST node whose children are being iterated.
        :param clause: Inherited clause context from the parent.
        :param depth: Current nesting depth.
        """
        for key in node.arg_types:
            if key in _SKIP_KEYS:
                continue
            child = node.args.get(key)
            if child is None:
                continue

            new_clause = _classify_clause(key, type(node)) or clause

            if not self._process_child_key(node, key, child, new_clause, depth):
                self._recurse_child(child, new_clause, depth)

    def _dispatch_leaf(self, node: exp.Expression, clause: str, depth: int) -> bool:
        """Dispatch leaf-like AST nodes to their specialised handlers.

        Checks if *node* is a terminal or semi-terminal node type that
        should be handled directly rather than recursed into.  Each
        branch delegates to the appropriate handler and returns ``True``
        to stop further recursion, or ``False`` to let the walk continue.

        :param node: The AST node to inspect.
        :param clause: Current clause context.
        :param depth: Current nesting depth.
        :returns: ``True`` if the node was handled (caller should stop
            recursion), ``False`` to continue the walk.
        """
        if self._is_literal_values_without_subquery(node):
            # e.g. INSERT INTO t VALUES (1, 2) — skip literal value lists
            return True
        if isinstance(node, (exp.Star, exp.ColumnDef, exp.Identifier)):
            if isinstance(node, exp.ColumnDef):
                # e.g. CREATE TABLE t (col INT) — collect ColumnDef names
                self._collector.add_column(node.name, clause)
            # Star and Identifier are terminal — no further recursion
            return True
        if isinstance(node, exp.CTE):
            # e.g. WITH cte AS (SELECT ...) — delegate to CTE handler
            self._handle_cte(node, depth)
            return True
        if isinstance(node, exp.Column):
            # e.g. SELECT t.col FROM t — delegate to column handler
            self._handle_column(node, clause)
            return True
        if isinstance(node, exp.Subquery) and node.alias:
            # e.g. SELECT (SELECT 1) AS sub — record named subquery
            self._collector.subquery_items.append((depth, node.alias))
        return False

    def _process_child_key(
        self, node: exp.Expression, key: str, child: Any, clause: str, depth: int
    ) -> bool:
        """Route special ``arg_types`` keys to dedicated handlers.

        Intercepts three specific key/parent combinations that need
        custom processing instead of the default recursive walk:

        - ``"expressions"`` on a ``SELECT`` — column list with aliases
        - ``"this"`` on an ``INSERT`` — schema with target column names
        - ``"using"`` on a ``JOIN`` — shared column identifiers

        Example SQL::

            SELECT a, b AS c FROM t JOIN t2 USING (id)

        :param node: Parent AST node.
        :param key: The ``arg_types`` key for the child.
        :param child: The child node or list of nodes.
        :param clause: Current clause context.
        :param depth: Current nesting depth.
        :returns: ``True`` if handled by a specialised handler,
            ``False`` for default recursive walk.
        """
        if key == "expressions" and isinstance(node, exp.Select):
            # e.g. SELECT a, b, c — handle the SELECT expression list
            self._handle_select_exprs(child, clause, depth)
            return True
        if isinstance(node, exp.Insert) and key == "this":
            # e.g. INSERT INTO t (col1, col2) — extract schema columns
            self._handle_insert_schema(node)
            return True
        if key == "using" and isinstance(node, exp.Join):
            # e.g. JOIN t2 USING (id) — extract shared join columns
            self._handle_join_using(child)
            return True
        return False

    def _recurse_child(self, child: Any, clause: str, depth: int) -> None:
        """Recursively walk a child value, handling both single nodes and lists.

        This is the default recursion path for ``arg_types`` children
        that are not intercepted by :meth:`_process_child_key`.

        :param child: A single :class:`~sqlglot.expressions.Expression`
            or a list of expressions.
        :param clause: Current clause context to propagate.
        :param depth: Current nesting depth (incremented for children).
        """
        if isinstance(child, list):
            # e.g. GROUP BY a, b — child is a list of Column expressions
            for item in child:
                if isinstance(item, exp.Expression):
                    self._walk(item, clause, depth + 1)
        elif isinstance(child, exp.Expression):
            # e.g. WHERE a > 1 — child is a single expression tree
            self._walk(child, clause, depth + 1)

    # -------------------------------------------------------------------
    # Node handlers
    # -------------------------------------------------------------------

    def _handle_select_exprs(
        self, exprs: list[exp.Expression], clause: str, depth: int
    ) -> None:
        """Process the expression list of a SELECT clause.

        Iterates each expression in the SELECT list, dispatching to
        the appropriate handler based on node type.  Also builds the
        ``output_columns`` list which records the projected column
        names in their original SELECT order.

        Example SQL::

            SELECT a, b AS alias, *, COALESCE(c, d) FROM t

        :param exprs: List of expression nodes from ``SELECT.expressions``.
        :param clause: Current clause context (typically ``"select"``).
        :param depth: Current nesting depth.
        """
        assert isinstance(exprs, list)
        out = self._collector.output_columns

        for expr in exprs:
            if isinstance(expr, exp.Alias):
                # e.g. SELECT price * qty AS total
                self._handle_alias(expr, clause, depth)
                out.append(expr.alias)
            elif isinstance(expr, exp.Star):
                # e.g. SELECT *
                self._collector.add_column("*", clause)
                out.append("*")
            elif isinstance(expr, exp.Column):
                # e.g. SELECT t.col_name
                self._handle_column(expr, clause)
                out.append(self._column_full_name(expr))
            else:
                # e.g. SELECT COALESCE(a, b) — function/expression without alias
                cols = self._flat_columns(expr)
                for col in cols:
                    self._collector.add_column(col, clause)
                out.append(cols[0] if len(cols) == 1 else str(expr))

    def _handle_alias(self, alias_node: exp.Alias, clause: str, depth: int) -> None:
        """Process an ``Alias`` node from a SELECT expression list.

        Handles three cases:

        1. **Subquery alias** — the alias wraps a subquery (contains a
           ``SELECT``).  The subquery body is walked recursively, and
           the alias target is derived from the subquery's own SELECT
           columns.
        2. **Expression alias with columns** — the inner expression
           contains one or more column references (e.g. ``a + b AS
           total``).  Columns are recorded and the alias is mapped to
           its source column(s).
        3. **Expression alias without columns** — a literal or star
           expression (e.g. ``COUNT(*) AS cnt``).  The alias is
           recorded with a ``"*"`` or ``None`` target.

        Example SQL::

            SELECT (SELECT id FROM t) AS sub, a + b AS total, 1 AS one

        :param alias_node: The ``Alias`` AST node.
        :param clause: Current clause context.
        :param depth: Current nesting depth.
        """
        c = self._collector
        alias_name = alias_node.alias
        inner = alias_node.this

        select = inner.find(exp.Select)
        if select:
            # Case 1: alias wraps a subquery — e.g. SELECT (SELECT id FROM t) AS sub
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
            # Case 2: inner expression has column references
            # e.g. SELECT a + b AS total — record columns a, b
            for col in inner_cols:
                c.add_column(col, clause)

            unique_inner = UniqueList(inner_cols)
            is_self_alias = self._is_self_alias(alias_name, unique_inner)
            is_direct = isinstance(inner, exp.Column)

            if is_direct and is_self_alias:
                pass  # e.g. SELECT col AS col — trivial self-alias, skip
            else:
                target = None
                if not is_self_alias:
                    # e.g. SELECT a + b AS total → target = ["a", "b"]
                    target = unique_inner[0] if len(unique_inner) == 1 else unique_inner
                c.add_alias(alias_name, target, clause)
        else:
            # Case 3: no column references — e.g. SELECT COUNT(*) AS cnt
            target = None
            if inner.find(exp.Star):
                # e.g. SELECT * AS all_cols — star target
                target = "*"
            c.add_alias(alias_name, target, clause)

    def _handle_cte(self, cte: exp.CTE, depth: int) -> None:
        """Process a CTE (Common Table Expression) AST node.

        Records the CTE alias as a CTE name.  If the CTE declares
        explicit column aliases (e.g. ``cte(x, y) AS (...)``), maps
        each alias to its corresponding column from the CTE body.
        Otherwise, walks the CTE body recursively to extract its
        columns normally.

        Example SQL::

            WITH cte(x, y) AS (SELECT a, b FROM t) SELECT x FROM cte

        :param cte: The ``CTE`` AST node.
        :param depth: Current nesting depth.
        :raises InvalidQueryDefinition: If the CTE has no alias (invalid SQL).
        """
        c = self._collector
        alias = cte.alias
        if not alias:
            raise InvalidQueryDefinition(
                "All CTEs require an alias, not a valid SQL"
            )

        c.cte_names.append(alias)

        body = cte.this

        if self._has_cte_explicit_column_definitions(cte):
            # e.g. WITH stats(total, avg) AS (SELECT SUM(x), AVG(x) FROM t)
            table_alias = cte.args.get("alias")
            assert table_alias is not None
            body_cols = self._flat_columns(body)
            real_cols = [x for x in body_cols if x != "*"]
            cte_col_names = [col.name for col in table_alias.columns]

            for col in body_cols:
                c.add_column(col, "select")

            for i, cte_col in enumerate(cte_col_names):
                if i < len(real_cols):
                    # Map CTE alias to body column by position
                    target = real_cols[i]
                elif "*" in body_cols:
                    # Body uses SELECT * — map alias to "*"
                    target = "*"
                else:
                    # More aliases than body columns — no target
                    target = None
                c.add_alias(cte_col, target, "select")
                c.cte_alias_names.add(cte_col)
        elif self._is_cte_with_query_body(body):
            # CTE without column aliases — e.g. WITH cte AS (SELECT a ...)
            self._walk(body, "", depth + 1)

    def _handle_insert_schema(self, node: exp.Insert) -> None:
        """Extract target column names from the Schema of an INSERT statement.

        Looks for the ``Schema`` node inside the INSERT AST and records
        each column identifier as an ``"insert"``-clause column.

        Example SQL::

            INSERT INTO users (name, email) VALUES ('a', 'b')

        :param node: The ``Insert`` AST node.
        """
        schema = node.find(exp.Schema)
        if schema and schema.expressions:
            for col_id in schema.expressions:
                name = col_id.name if hasattr(col_id, "name") else str(col_id)
                self._collector.add_column(name, "insert")

    def _handle_join_using(self, child: Any) -> None:
        """Extract column identifiers from a ``JOIN ... USING`` clause.

        Iterates the identifier list and records each as a
        ``"join"``-clause column.

        Example SQL::

            SELECT * FROM orders JOIN customers USING (customer_id)

        :param child: The USING clause child — a list of identifier
            nodes.
        """
        if isinstance(child, list):
            # e.g. USING (id, name) — child is a list of Identifier nodes
            for item in child:
                if hasattr(item, "name"):
                    self._collector.add_column(item.name, "join")

    def _handle_column(self, col: exp.Column, clause: str) -> None:
        """Process a ``Column`` AST node during the walk.

        Handles several column forms:

        - **Table-qualified star** — ``t.*`` is recorded as
          ``"resolved_table.*"``.
        - **CTE column-alias reference** — ``cte.col`` where ``col``
          is a known CTE alias is filed into ``alias_dict`` instead of
          ``columns``.
        - **Bare alias reference** — a bare name matching a known alias
          (e.g. in ``ORDER BY alias``) is filed into ``alias_dict``.
        - **Regular column** — everything else is recorded via the
          fully-qualified name.

        Example SQL::

            SELECT t.id, t.*, alias_col FROM t ORDER BY alias_col

        :param col: The ``Column`` AST node.
        :param clause: Current clause context.
        """
        c = self._collector

        star = col.find(exp.Star)
        if star:
            # e.g. SELECT t.* — table-qualified star
            table = col.table
            if table:
                table = self._resolve_table_alias(table)
                c.add_column(f"{table}.*", clause)
            return

        if self._is_cte_column_alias_reference(col):
            # e.g. SELECT cte.x — CTE column alias reference
            c.alias_dict.setdefault(clause, UniqueList()).append(col.name)
            return

        full = self._column_full_name(col)

        unqualified = col.name
        if self._is_unqualified_alias_reference(col):
            # e.g. ORDER BY alias_name — name matches a known alias
            c.alias_dict.setdefault(clause, UniqueList()).append(unqualified)
            return

        # e.g. SELECT t.col — regular column, no alias match
        c.add_column(full, clause)

    # -------------------------------------------------------------------
    # Column name resolution
    # -------------------------------------------------------------------

    def _resolve_table_alias(self, col_table: str) -> str:
        """Replace a table alias with the real table name if mapped.

        Looks up *col_table* in the pre-computed ``table_aliases`` dict.
        If found, returns the resolved real table name; otherwise
        returns the input unchanged.

        Example::

            # Given table_aliases = {"t": "users"}
            _resolve_table_alias("t")  # → "users"

        :param col_table: A table name or alias string.
        :returns: The resolved table name, or *col_table* if no mapping
            exists.
        """
        return self._table_aliases.get(col_table, col_table)

    def _column_full_name(self, col: exp.Column) -> str:
        """Build a dot-separated fully-qualified column name.

        Resolves the table alias portion (if present) and assembles
        the name from up to four parts: ``catalog.db.table.column``.
        Trailing ``#`` characters are stripped from the column name
        (used by some dialects for temp-table markers).

        Example SQL::

            SELECT catalog.schema.t.col FROM t

        :param col: A ``Column`` AST node.
        :returns: The fully-qualified column name string
            (e.g. ``"users.name"``).
        """
        name = col.name.rstrip("#")
        table = col.table
        db = col.args.get("db")
        catalog = col.args.get("catalog")

        if table:
            # e.g. SELECT t.col — table-qualified column
            resolved = self._resolve_table_alias(table)
            parts = []
            if catalog:
                # e.g. SELECT catalog.schema.t.col — has catalog prefix
                parts.append(
                    catalog.name if isinstance(catalog, exp.Expression) else catalog
                )
            if db:
                # e.g. SELECT schema.t.col — has db/schema prefix
                parts.append(db.name if isinstance(db, exp.Expression) else db)
            parts.append(resolved)
            parts.append(name)
            return ".".join(parts)
        # e.g. SELECT col — bare column name without table qualifier
        return name

    @staticmethod
    def _is_star_inside_function(star: exp.Star) -> bool:
        """Check whether a ``*`` node sits inside a function call.

        Uses sqlglot's ``find_ancestor`` to walk the parent chain and
        look for ``Func`` (built-in functions) or ``Anonymous``
        (user-defined function) nodes.  A star inside a function like
        ``COUNT(*)`` should not be recorded as a standalone column.

        Example SQL::

            SELECT COUNT(*) FROM t

        :param star: A ``Star`` AST node.
        :returns: ``True`` if the star is inside a function call.
        """
        return star.find_ancestor(exp.Func, exp.Anonymous) is not None

    # -------------------------------------------------------------------
    # Predicate helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _is_literal_values_without_subquery(
        node: exp.Expression,
    ) -> bool:
        """Check whether *node* is a VALUES clause with only literal values.

        Returns ``True`` for plain ``VALUES (1, 2), (3, 4)`` rows and
        ``False`` when the VALUES clause contains a subquery
        (``VALUES (SELECT ...)``).  Literal value lists are skipped
        during the walk because they contain no column references.

        Example SQL::

            INSERT INTO t VALUES (1, 2)          -- True
            INSERT INTO t VALUES (SELECT x ...)  -- False

        :param node: An AST node to test.
        :returns: ``True`` if the node is a literal-only VALUES clause.
        """
        return isinstance(node, exp.Values) and not node.find(
            exp.Select
        )

    def _is_cte_column_alias_reference(
        self, col: exp.Column
    ) -> bool:
        """Check whether *col* references a known CTE column alias.

        Returns ``True`` when the column is table-qualified with a CTE
        name and the column name matches one of the CTE's declared
        column aliases (recorded during CTE processing).

        Example SQL::

            WITH cte AS (...) SELECT cte.x  -- True when x is a CTE alias

        :param col: A ``Column`` AST node.
        :returns: ``True`` if this is a CTE column-alias reference.
        """
        c = self._collector
        return bool(
            col.table
            and col.table in c.cte_names
            and col.name in c.cte_alias_names
        )

    def _is_unqualified_alias_reference(
        self, col: exp.Column
    ) -> bool:
        """Check whether *col* is an unqualified reference to a known alias.

        Returns ``True`` when the column has no table qualifier and its
        name matches a previously recorded column alias.  This typically
        occurs in ``ORDER BY``, ``GROUP BY``, or ``HAVING`` clauses
        that reference a SELECT alias by name.

        Example SQL::

            SELECT a AS x ... ORDER BY x  -- True (x has no table qualifier)

        :param col: A ``Column`` AST node.
        :returns: ``True`` if this is an unqualified alias reference.
        """
        c = self._collector
        return not col.table and col.name in c.alias_names

    @staticmethod
    def _is_self_alias(
        alias_name: str, unique_inner: UniqueList
    ) -> bool:
        """Check whether an alias maps back to itself.

        Returns ``True`` when the alias name is identical to the single
        source column (either exactly or by last segment for
        table-qualified columns).  Self-aliases like
        ``SELECT col AS col`` are not recorded as meaningful aliases.

        Example SQL::

            SELECT col AS col      -- True (exact match)
            SELECT t.col AS col    -- True (last_segment match)
            SELECT a + b AS total  -- False

        :param alias_name: The alias string.
        :param unique_inner: Deduplicated list of source column names.
        :returns: ``True`` if the alias is a trivial self-reference.
        """
        return len(unique_inner) == 1 and (
            unique_inner[0] == alias_name
            or last_segment(unique_inner[0]) == alias_name
        )

    @staticmethod
    def _is_standalone_star(
        child: exp.Star, seen_stars: set[int]
    ) -> bool:
        """Check whether a star node is standalone (not consumed by a Column).

        Returns ``True`` when the star has not already been accounted
        for by a parent ``Column`` node (e.g. ``t.*``) and is not
        directly nested inside a ``Column``.  Stars inside functions
        like ``COUNT(*)`` are filtered separately by
        :meth:`_is_star_inside_function`.

        Example SQL::

            SELECT * FROM t    -- True
            SELECT t.* FROM t  -- False (consumed by Column parent)

        :param child: A ``Star`` AST node.
        :param seen_stars: Set of ``id()`` values for stars already
            consumed by a parent ``Column`` node.
        :returns: ``True`` if this is a standalone star.
        """
        return id(child) not in seen_stars and not isinstance(
            child.parent, exp.Column
        )

    @staticmethod
    def _has_cte_explicit_column_definitions(
        cte: exp.CTE,
    ) -> bool:
        """Check whether a CTE declares explicit column aliases.

        Returns ``True`` when the CTE has a column definition list in
        its signature (e.g. ``cte(x, y)``) and the CTE body is a
        ``SELECT`` statement.

        Example SQL::

            WITH stats(total, avg) AS (SELECT SUM(x), AVG(x) FROM t)  -- True
            WITH cte AS (SELECT a FROM t)                              -- False

        :param cte: A ``CTE`` AST node.
        :returns: ``True`` if the CTE has explicit column definitions.
        """
        table_alias = cte.args.get("alias")
        return bool(
            table_alias
            and table_alias.columns
            and cte.this
            and isinstance(cte.this, exp.Select)
        )

    @staticmethod
    def _is_cte_with_query_body(
        body: exp.Expression,
    ) -> bool:
        """Check whether a CTE body is a walkable query statement.

        Returns ``True`` for standard SQL query bodies (SELECT, UNION,
        INTERSECT, EXCEPT) and ``False`` for scalar expression bodies
        used by some dialects (e.g. ClickHouse's
        ``WITH '2019-08-01' AS ts``  where the body is a Literal,
        or ``WITH 1 + 2 AS val`` where the body is an Add).

        :param body: The ``this`` child of a CTE node.
        :returns: ``True`` if the body is a query that should be walked.
        """
        return isinstance(
            body, (exp.Select, exp.Union, exp.Intersect, exp.Except)
        )

    # -------------------------------------------------------------------
    # Flat column extraction
    # -------------------------------------------------------------------

    def _flat_columns_select_only(self, select: exp.Select) -> list[str]:
        """Extract column/alias names from a SELECT's immediate expressions.

        Unlike :meth:`_flat_columns`, this does not recurse into the
        full AST subtree — it only inspects the top-level expressions
        of a SELECT clause.  Used by :meth:`_handle_alias` to determine
        the alias target for subquery aliases.

        Example SQL::

            SELECT a, b AS alias, * FROM t

        :param select: A ``Select`` AST node.
        :returns: A list of column name / alias name strings in SELECT
            order.
        """
        cols = []
        for expr in select.expressions or []:
            if isinstance(expr, exp.Alias):
                # e.g. SELECT b AS alias — use the alias name
                cols.append(expr.alias)
            elif isinstance(expr, exp.Column):
                # e.g. SELECT a — use the fully-qualified column name
                cols.append(self._column_full_name(expr))
            elif isinstance(expr, exp.Star):
                # e.g. SELECT * — literal star
                cols.append("*")
            else:
                # e.g. SELECT COALESCE(a, b) — extract columns from expression
                for col_name in self._flat_columns(expr):
                    cols.append(col_name)
        return cols

    def _flat_columns(self, node: exp.Expression) -> list[str]:
        """Extract all column names from an expression subtree via DFS.

        Performs a full depth-first traversal of *node* using
        :func:`_dfs` and collects every ``Column`` and standalone
        ``Star`` reference found.  Tracks already-seen star nodes to
        avoid double-counting table-qualified stars (e.g. ``t.*``
        produces both a ``Column`` and a nested ``Star``).

        Example SQL::

            COALESCE(t.a, b, c)

        :param node: Root expression node to scan.
        :returns: A list of column name strings in DFS encounter order.
        """
        assert node is not None
        cols = []
        seen_stars: set[int] = set()
        for child in _dfs(node):
            name = self._collect_column_from_node(child, seen_stars)
            if name is not None:
                cols.append(name)
        return cols

    def _collect_column_from_node(
        self, child: exp.Expression, seen_stars: set[int]
    ) -> str | None:
        """Extract a column name from a single DFS-visited node.

        Called by :meth:`_flat_columns` for each node in the traversal.
        Handles ``Column`` nodes (resolving table aliases and skipping
        date-part unit keywords) and standalone ``Star`` nodes (skipping
        stars inside functions like ``COUNT(*)``).

        Example SQL::

            DATEDIFF(day, start_date, end_date)

        In this example, ``day`` is a date-part unit keyword and should
        be skipped, while ``start_date`` and ``end_date`` are real
        columns.

        :param child: A single AST node from the DFS traversal.
        :param seen_stars: Set of ``id()`` values for ``Star`` nodes
            already consumed by a parent ``Column`` (e.g. ``t.*``).
        :returns: The column name string, or ``None`` if the node is
            not a column reference.
        """
        if isinstance(child, exp.Column):
            # e.g. SELECT t.col, DATEDIFF(day, a, b)
            if _is_date_part_unit(child):
                # e.g. DATEDIFF(day, ...) — "day" is a unit keyword, not a column
                return None
            star = child.find(exp.Star)
            if star:
                # e.g. SELECT t.* — table-qualified star within a Column node
                seen_stars.add(id(star))
                table = child.table
                if table:
                    table = self._resolve_table_alias(table)
                    return f"{table}.*"
            return self._column_full_name(child)  # e.g. SELECT t.col
        if isinstance(child, exp.Star):
            # e.g. SELECT * — standalone star (not inside a Column node)
            if self._is_standalone_star(child, seen_stars):
                if not self._is_star_inside_function(child):
                    # e.g. SELECT * FROM t — standalone star, not COUNT(*)
                    return "*"
        return None
