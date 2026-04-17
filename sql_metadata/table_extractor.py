"""Extract tables and table aliases from a sqlglot AST.

The :class:`TableExtractor` class walks the AST for ``exp.Table`` nodes,
builds fully-qualified table names (optionally preserving ``[bracket]``
notation for TSQL), and sorts results by each table identifier's
character position from sqlglot's tokenizer (``Identifier.meta['start']``),
so the output order matches left-to-right reading order without any
regex scan of the raw SQL.  CTE names are excluded from the result so
that only *real* tables are reported.
"""

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sql_metadata.utils import UniqueList

# ---------------------------------------------------------------------------
# Pure static helpers (no instance state needed)
# ---------------------------------------------------------------------------


def _assemble_dotted_name(
    catalog: str, db: str, name: str, *, preserve_empty: bool = False
) -> str:
    """Assemble a dot-joined table name from catalog, db, and name parts.

    When *preserve_empty* is ``True``, empty segments are kept so that
    double-dot notation (e.g. ``server..table``) is preserved.

    .. code-block:: sql

       -- preserve_empty=False (default)
       SELECT * FROM mydb.dbo.users   -- → "mydb.dbo.users"
       -- preserve_empty=True
       SELECT * FROM server..users    -- → "server..users"

    :param catalog: Catalog / server segment (may be empty).
    :param db: Database / schema segment (may be empty).
    :param name: Table name segment.
    :param preserve_empty: Keep empty segments for double-dot notation.
    :returns: Dot-joined name string.
    """
    return ".".join(
        part for part in [catalog, db, name] if part or preserve_empty
    )


def _ident_str(node: exp.Identifier) -> str:
    """Return an identifier string, wrapping it in ``[brackets]`` if quoted.

    TSQL uses square brackets for quoting — this helper preserves that
    notation so the output matches the original SQL style.

    .. code-block:: sql

       SELECT * FROM [dbo].[Users]  -- → "[dbo]", "[Users]"
       SELECT * FROM dbo.Users      -- → "dbo", "Users"

    :param node: An ``exp.Identifier`` AST node.
    :returns: The identifier text, optionally bracket-wrapped.
    """
    return f"[{node.name}]" if node.quoted else node.name


def _collect_node_parts(node: object, parts: list[str]) -> None:
    """Append identifier strings from *node* into *parts*.

    Handles both simple ``exp.Identifier`` nodes and ``exp.Dot`` nodes
    that contain two identifiers (e.g. ``schema.table``).

    :param node: An AST node — either ``exp.Identifier`` or ``exp.Dot``.
    :param parts: Accumulator list to append identifier strings into.
    """
    if isinstance(node, exp.Identifier):
        # e.g. SELECT * FROM [Users] — single identifier
        parts.append(_ident_str(node))
    elif isinstance(node, exp.Dot):
        # e.g. SELECT * FROM [dbo].[Users] — dotted pair
        for sub in [node.this, node.expression]:
            if isinstance(sub, exp.Identifier):
                parts.append(_ident_str(sub))


def _bracketed_full_name(table: exp.Table) -> str:
    """Build a table name preserving ``[bracket]`` notation from AST nodes.

    Walks the ``catalog``, ``db``, and ``this`` args of an ``exp.Table``
    node, collecting bracket-preserved identifier parts.

    .. code-block:: sql

       SELECT * FROM [mydb].[dbo].[Users]  -- → "[mydb].[dbo].[Users]"
       SELECT * FROM [Users]               -- → "[Users]"

    :param table: An ``exp.Table`` AST node.
    :returns: Dot-joined bracket-preserved name, or ``""`` if no parts found.
    """
    parts: list[str] = []
    for key in ["catalog", "db", "this"]:
        node = table.args.get(key)
        if node is not None:
            _collect_node_parts(node, parts)
    return ".".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# TableExtractor class
# ---------------------------------------------------------------------------


class TableExtractor:
    """Extract table names and aliases from a sqlglot AST.

    Collects ``exp.Table`` nodes from the AST, builds fully-qualified
    names (with bracket preservation for TSQL), filters out CTE names,
    and sorts by each table identifier's character position from
    sqlglot's tokenizer (``Identifier.meta['start']``).

    :param ast: Root AST node produced by sqlglot.
    :param cte_names: Set of CTE names to exclude from the result.
    :param dialect: The dialect used to parse the AST.
    """

    def __init__(
        self,
        ast: exp.Expression,
        cte_names: set[str] | None = None,
        dialect: DialectType = None,
    ):
        self._ast = ast
        self._cte_names = cte_names or set()

        from sql_metadata.dialect_parser import BracketedTableDialect

        self._bracket_mode = isinstance(dialect, type) and issubclass(
            dialect, BracketedTableDialect
        )
        self._cached_table_nodes: list[exp.Table] | None = None

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def extract(self) -> list[str]:
        """Extract table names, excluding CTE definitions.

        For ``CREATE TABLE`` statements, the target table is always placed
        first in the result regardless of its position in the SQL text.
        All other tables are sorted by the character position of their
        first identifier token (from sqlglot's ``Identifier.meta``),
        giving left-to-right reading order.

        .. code-block:: sql

           SELECT * FROM users JOIN orders ON ...  -- → ["users", "orders"]
           CREATE TABLE new_t AS SELECT * FROM src -- → ["new_t", "src"]

        :returns: Ordered list of unique table names.
        """
        create_target = None
        if isinstance(self._ast, exp.Create):
            # e.g. CREATE TABLE t AS SELECT ... — extract target first
            create_target = self._extract_create_target()

        tables_with_pos: list[tuple[str, int]] = []
        for node in self._table_nodes():
            name = self._table_full_name(node)
            if name and name not in self._cte_names:
                tables_with_pos.append((name, self._table_start_position(node)))
        tables_with_pos.sort(key=lambda pair: pair[1])
        sorted_names = [name for name, _ in tables_with_pos]
        return UniqueList(
            [create_target, *sorted_names] if create_target else sorted_names
        )

    def extract_aliases(self, tables: list[str]) -> dict[str, str]:
        """Extract table alias mappings from the AST.

        Walks all ``exp.Table`` nodes and maps each alias back to its
        fully-qualified table name, but only if the table appears in the
        provided *tables* list.

        .. code-block:: sql

           SELECT u.id FROM users u  -- → {"u": "users"}

        :param tables: List of known table names (from :meth:`extract`).
        :returns: Mapping of ``{alias: table_name}``.
        """
        aliases = {}
        for table in self._table_nodes():
            alias = table.alias
            if not alias:
                # e.g. SELECT * FROM users — no alias, skip
                continue
            full_name = self._table_full_name(table)
            if full_name in tables:
                aliases[alias] = full_name

        return aliases

    # -------------------------------------------------------------------
    # Collection helpers
    # -------------------------------------------------------------------

    def _extract_create_target(self) -> str | None:
        """Extract the target table name from a ``CREATE TABLE`` statement.

        The ``CREATE`` node's ``this`` arg may be a ``Table`` directly or a
        ``Schema`` wrapping one — both cases are handled.

        .. code-block:: sql

           CREATE TABLE my_table (id INT)              -- → "my_table"
           CREATE TABLE my_table AS SELECT * FROM src  -- → "my_table"

        :returns: Target table name, or ``None`` if it cannot be determined.
        """
        target = self._ast.this
        target_table = (
            # e.g. CREATE TABLE t (col INT) — target.this is Schema, find Table inside
            target.find(exp.Table) if not isinstance(target, exp.Table)
            # e.g. CREATE TABLE t AS SELECT ... — target.this is Table directly
            else target
        )
        name = self._table_full_name(target_table)
        return name or None

    def _table_nodes(self) -> list[exp.Table]:
        """Return all ``exp.Table`` nodes from the AST (cached).

        Uses ``find_all(exp.Table)`` which performs a DFS traversal, finding
        tables in subqueries, CTEs, and joins.  Results are cached so
        repeated calls (from :meth:`extract_aliases`, :meth:`_collect_all`)
        don't re-walk the tree.

        :returns: List of ``exp.Table`` AST nodes.
        """
        if self._cached_table_nodes is None:
            self._cached_table_nodes = list(self._ast.find_all(exp.Table))
        return self._cached_table_nodes

    # -------------------------------------------------------------------
    # Table name construction
    # -------------------------------------------------------------------

    def _table_full_name(self, table: exp.Table) -> str:
        """Build a fully-qualified table name from an ``exp.Table`` node.

        In bracket mode (TSQL), delegates to :func:`_bracketed_full_name` to
        preserve ``[square bracket]`` quoting.  Otherwise, assembles a
        dot-joined name from catalog, db, and name parts.  Double-dot
        notation (``server..table``) is detected from the AST itself —
        sqlglot parses the empty segment as ``db=''`` (a string), whereas
        an absent segment is ``db=None``.

        .. code-block:: sql

           SELECT * FROM mydb.dbo.users  -- → "mydb.dbo.users"
           SELECT * FROM [dbo].[Users]   -- (TSQL) → "[dbo].[Users]"
           SELECT * FROM server..users   -- → "server..users"

        :param table: An ``exp.Table`` AST node.
        :returns: Fully-qualified table name string.
        """
        name = table.name

        if self._bracket_mode:
            # e.g. SELECT * FROM [dbo].[Users] — preserve bracket notation
            bracketed = _bracketed_full_name(table)
            if bracketed:
                return bracketed

        # e.g. SELECT * FROM server..table — sqlglot records the empty
        # middle segment as db="" (string), whereas a missing db slot is None.
        has_double_dot = table.args.get("db") == ""
        return _assemble_dotted_name(
            table.catalog, table.db, name, preserve_empty=has_double_dot
        )

    # -------------------------------------------------------------------
    # Position detection
    # -------------------------------------------------------------------

    @staticmethod
    def _table_start_position(table: exp.Table) -> int:
        """Return the earliest identifier start position for *table*.

        sqlglot's tokenizer attaches ``meta['start']`` (0-based character
        offset in the raw SQL) to every ``exp.Identifier`` it produces.
        A qualified name like ``catalog.db.name`` has three Identifier
        children inside ``exp.Table.args``; we take the minimum so the
        whole reference sorts by its leftmost character.

        Tables without any positioned identifier (e.g. AST nodes built
        programmatically rather than parsed) sort to the front via ``0``.

        :param table: An ``exp.Table`` AST node.
        :returns: Character position (0-based) of the first identifier.
        """
        positions = [
            n.meta["start"]
            for key in ("catalog", "db", "this")
            if isinstance((n := table.args.get(key)), exp.Identifier)
            and "start" in n.meta
        ]
        return min(positions) if positions else 0
