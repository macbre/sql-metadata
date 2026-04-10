"""Extract tables and table aliases from a sqlglot AST.

The :class:`TableExtractor` class walks the AST for ``exp.Table`` and
``exp.Lateral`` nodes, builds fully-qualified table names (optionally
preserving ``[bracket]`` notation for TSQL), and sorts results by their
first occurrence in the raw SQL so the output order matches left-to-right
reading order.  CTE names are excluded from the result so that only *real*
tables are reported.
"""

import functools
import re

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


def _ends_with_table_keyword(before: str) -> bool:
    """Check whether *before* ends with a table-introducing keyword.

    Used to determine if a table name appears right after ``FROM``,
    ``JOIN``, ``TABLE``, ``INTO``, or ``UPDATE``.

    :param before: Upper-cased SQL text preceding the candidate table name.
    :returns: ``True`` if the text ends with a table keyword.
    """
    return any(before.endswith(kw) for kw in _TABLE_CONTEXT_KEYWORDS)


def _is_in_comma_list_after_keyword(before: str) -> bool:
    """Check whether a comma-preceded name belongs to a table list.

    Looks backward for the nearest table-introducing keyword (e.g. ``FROM``)
    and verifies that no interrupting keyword (e.g. ``WHERE``, ``SELECT``)
    appears between it and the comma.  This handles multi-table ``FROM``
    clauses.

    .. code-block:: sql

       SELECT * FROM t1, t2, t3  -- t2 and t3 are in comma list after FROM

    :param before: Upper-cased SQL text preceding the comma + candidate name.
    :returns: ``True`` if the name is part of a comma-separated table list.
    """
    best_kw_pos = -1
    for kw in _TABLE_CONTEXT_KEYWORDS:
        kw_pos = before.rfind(kw)
        if kw_pos > best_kw_pos:
            best_kw_pos = kw_pos
    if best_kw_pos < 0:
        # no table keyword found at all
        return False
    between = before[best_kw_pos:]
    # e.g. FROM t1 WHERE ... , x — WHERE interrupts, so x is not a table
    return not any(ik in between for ik in _INTERRUPTING_KEYWORDS)


#: SQL keywords that introduce a table-name context.
_TABLE_CONTEXT_KEYWORDS = {"FROM", "JOIN", "TABLE", "INTO", "UPDATE"}

#: Keywords that interrupt a comma-separated table list.
_INTERRUPTING_KEYWORDS = {"SELECT", "WHERE", "ORDER", "GROUP", "HAVING", "SET"}


# ---------------------------------------------------------------------------
# TableExtractor class
# ---------------------------------------------------------------------------


class TableExtractor:
    """Extract table names and aliases from a sqlglot AST.

    Encapsulates the raw SQL string and AST needed for position-based
    table sorting, bracket-mode detection, and CTE name filtering.

    The extraction pipeline:

    1. Collect all ``exp.Table`` nodes from the AST.
    2. Build fully-qualified names (with bracket preservation for TSQL).
    3. Filter out CTE names so only real tables are reported.
    4. Sort by first occurrence in the raw SQL for left-to-right order.

    :param ast: Root AST node produced by sqlglot.
    :param raw_sql: Original SQL string, used for position-based sorting.
    :param cte_names: Set of CTE names to exclude from the result.
    :param dialect: The dialect used to parse the AST.
    """

    def __init__(
        self,
        ast: exp.Expression,
        raw_sql: str = "",
        cte_names: set[str] | None = None,
        dialect: DialectType = None,
    ):
        self._ast = ast
        self._raw_sql = raw_sql
        self._upper_sql = raw_sql.upper()
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
        All other tables are sorted by their first occurrence in the raw
        SQL (left-to-right reading order).

        .. code-block:: sql

           SELECT * FROM users JOIN orders ON ...  -- → ["users", "orders"]
           CREATE TABLE new_t AS SELECT * FROM src -- → ["new_t", "src"]

        :returns: Ordered list of unique table names.
        """
        create_target = None
        if isinstance(self._ast, exp.Create):
            # e.g. CREATE TABLE t AS SELECT ... — extract target first
            create_target = self._extract_create_target()

        collected = self._collect_all()
        collected_sorted = sorted(collected, key=lambda t: self._first_position(t))
        return UniqueList(
            [create_target, *collected_sorted] if create_target
            else collected_sorted
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

    def _collect_all(self) -> UniqueList:
        """Collect table names from all ``exp.Table`` AST nodes.

        Iterates over every ``exp.Table`` node, builds the full name, and
        filters out CTE names so that only real tables are collected.

        .. code-block:: sql

           WITH cte AS (SELECT 1) SELECT * FROM cte, real_table
           -- cte is filtered out → collects only "real_table"

        :returns: :class:`UniqueList` of table names (unsorted).
        """
        collected = UniqueList()
        for table in self._table_nodes():
            full_name = self._table_full_name(table)
            if full_name and full_name not in self._cte_names:
                # e.g. FROM users — real table, collect it
                collected.append(full_name)
            # else: e.g. FROM cte_name — CTE reference, skip
        return collected

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
        notation (``server..table``) is detected from the raw SQL.

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

        # e.g. SELECT * FROM server..table — detect double-dot in raw SQL
        has_double_dot = bool(name and f"..{name}" in self._raw_sql)
        return _assemble_dotted_name(
            table.catalog, table.db, name, preserve_empty=has_double_dot
        )

    # -------------------------------------------------------------------
    # Position detection
    # -------------------------------------------------------------------

    def _first_position(self, name: str) -> int:
        """Find the first occurrence of a table name in a table context.

        Position sorting ensures the output order matches the left-to-right
        reading order of the SQL.  First tries to find the name after a
        table-introducing keyword (``FROM``, ``JOIN``, etc.); if not found,
        falls back to any whole-word occurrence; if still not found, returns
        the SQL length (pushing unknown names to the end).

        .. code-block:: sql

           SELECT * FROM b JOIN a ON ...  -- a at pos ~22, b at pos ~14 → [b, a]

        :param name: Table name to locate.
        :returns: Character position (0-based), or ``len(sql)`` if not found.
        """
        name_upper = name.upper()

        # try 1: find after a table keyword (FROM, JOIN, etc.)
        pos = self._find_word_in_table_context(name_upper)
        if pos >= 0:
            return pos

        # try 2: find as a bare word anywhere in the SQL
        pos = self._find_word(name_upper)
        return pos if pos >= 0 else len(self._raw_sql)

    def _find_word_in_table_context(self, name_upper: str) -> int:
        """Find a table name that appears after a table-introducing keyword.

        Scans all whole-word occurrences of *name_upper* and returns the
        position of the first one that is directly preceded by a table
        keyword (``FROM``, ``JOIN``, etc.) or is part of a comma-separated
        table list following such a keyword.

        .. code-block:: sql

           SELECT t.id FROM users t   -- "users" preceded by FROM → match
           SELECT * FROM t1, t2       -- "t2" preceded by comma after FROM → match
           SELECT users FROM other    -- "users" in SELECT list → no match here

        :param name_upper: Upper-cased table name to search for.
        :returns: Position of the match, or ``-1`` if not found in table context.
        """
        for match in self._word_pattern(name_upper).finditer(self._upper_sql):
            pos: int = int(match.start())
            before = self._upper_sql[:pos].rstrip()
            if _ends_with_table_keyword(before):
                # e.g. FROM users — directly after table keyword
                return pos
            if before.endswith(",") and _is_in_comma_list_after_keyword(before):
                # e.g. FROM t1, t2 — part of comma-separated list
                return pos
        return -1

    def _find_word(self, name_upper: str, start: int = 0) -> int:
        """Find *name_upper* as a whole word in the upper-cased SQL.

        Uses a cached regex pattern that respects word boundaries and
        handles optionally-quoted segments for dotted names.

        :param name_upper: Upper-cased name to search for.
        :param start: Position to start searching from.
        :returns: Position of the match, or ``-1`` if not found.
        """
        match = self._word_pattern(name_upper).search(self._upper_sql, start)
        return int(match.start()) if match else -1

    # Optional quote wrappers — cover backticks, single/double quotes, and brackets
    _OPT_OPEN_QUOTE = r"""[`"'\[]?"""
    _OPT_CLOSE_QUOTE = r"""[`"'\]]?"""

    @staticmethod
    @functools.lru_cache(maxsize=512)
    def _word_pattern(name_upper: str) -> re.Pattern[str]:
        """Build a regex matching *name_upper* as a whole word (cached).

        For qualified names (containing dots), each segment may be optionally
        wrapped in backticks, single/double quotes, or brackets — so the
        pattern for ``SCHEMA.TABLE`` also matches ``"SCHEMA"."TABLE"``,
        ``[SCHEMA].[TABLE]``, or ```SCHEMA`.`TABLE```.

        The pattern is compiled once and cached via ``lru_cache`` for
        reuse across calls and instances.

        .. code-block:: sql

           SELECT * FROM schema.table       -- matched by SCHEMA.TABLE
           SELECT * FROM "schema"."table"   -- also matched
           SELECT * FROM [schema].[table]   -- also matched

        :param name_upper: Upper-cased table name (may contain dots).
        :returns: Compiled regex pattern with word-boundary assertions.
        """
        oq = TableExtractor._OPT_OPEN_QUOTE
        cq = TableExtractor._OPT_CLOSE_QUOTE
        segments = name_upper.split(".")
        inner = r"\.".join(
            oq + re.escape(seg) + cq for seg in segments
        )
        return re.compile(r"(?<![A-Za-z0-9_])" + inner + r"(?![A-Za-z0-9_])")
