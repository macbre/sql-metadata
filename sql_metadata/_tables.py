"""Extract tables and table aliases from a sqlglot AST.

Walks the AST for ``exp.Table`` and ``exp.Lateral`` nodes, builds
fully-qualified table names (optionally preserving ``[bracket]``
notation for TSQL), and sorts results by their first occurrence
in the raw SQL so the output order matches left-to-right reading
order.  CTE names are excluded from the result so that only *real*
tables are reported.
"""

from typing import Dict, List, Set

from sqlglot import exp

from sql_metadata.utils import UniqueList


def _assemble_dotted_name(catalog: str, db, name: str) -> str:
    """Assemble a dot-joined table name from catalog, db, and name parts.

    Handles the special case where *db* is an empty string but *catalog*
    is present (producing ``catalog..name``-style output via an empty
    middle part).

    :param catalog: Catalog / server part (may be falsy).
    :type catalog: str
    :param db: Database / schema part (``None``, ``""``, or a string).
    :param name: Table name part.
    :type name: str
    :returns: Dot-joined table name.
    :rtype: str
    """
    parts = []
    if catalog:
        parts.append(catalog)
    if db is not None:
        if db == "" and catalog:
            parts.append("")
        elif db:
            parts.append(db)
    if name:
        parts.append(name)
    return ".".join(parts)


def _table_full_name(
    table: exp.Table, raw_sql: str = "", bracket_mode: bool = False
) -> str:
    """Build a fully-qualified table name from an ``exp.Table`` AST node.

    Assembles ``catalog.db.table`` from the node's parts.  Special-cases:

    * **Bracket mode** — when the query was parsed with
      :class:`_BracketedTableDialect`, delegates to
      :func:`_bracketed_full_name` to preserve ``[square bracket]``
      quoting in the output.
    * **Double-dot notation** — detects ``..table`` or ``catalog..table``
      patterns in the raw SQL and reproduces them (used by some MSSQL
      and Redshift queries).

    :param table: sqlglot Table node.
    :type table: exp.Table
    :param raw_sql: Original SQL string, used for double-dot detection.
    :type raw_sql: str
    :param bracket_mode: If ``True``, preserve ``[bracket]`` quoting.
    :type bracket_mode: bool
    :returns: Dot-joined table name (e.g. ``"schema.table"``).
    :rtype: str
    """
    name = table.name

    if bracket_mode:
        bracketed = _bracketed_full_name(table)
        if bracketed:
            return bracketed

    if raw_sql and name and f"..{name}" in raw_sql:
        catalog = table.catalog
        return f"{catalog}..{name}" if catalog else f"..{name}"

    return _assemble_dotted_name(table.catalog, table.db, name)


def _ident_str(node: exp.Identifier) -> str:
    """Return an identifier string, wrapping it in ``[brackets]`` if quoted.

    sqlglot marks identifiers parsed inside square brackets as ``quoted``;
    this helper re-applies the brackets so the output matches the original
    SQL notation.

    :param node: sqlglot Identifier node.
    :type node: exp.Identifier
    :returns: Identifier text, optionally wrapped in brackets.
    :rtype: str
    """
    return f"[{node.name}]" if node.quoted else node.name


def _collect_node_parts(node, parts: list) -> None:
    """Append identifier strings from *node* into *parts*.

    Handles both simple ``exp.Identifier`` nodes and ``exp.Dot`` nodes
    (used for 4-part names like ``server.db.schema.table``).

    :param node: An AST node — ``Identifier``, ``Dot``, or empty string.
    :type node: exp.Expression or str
    :param parts: Mutable list to which strings are appended.
    :type parts: list
    :returns: Nothing — modifies *parts* in place.
    :rtype: None
    """
    if isinstance(node, exp.Identifier):
        parts.append(_ident_str(node))
    elif isinstance(node, exp.Dot):
        # 4-part names: Dot(schema, table)
        for sub in [node.this, node.expression]:
            if isinstance(sub, exp.Identifier):
                parts.append(_ident_str(sub))
    elif node == "":
        parts.append("")


def _bracketed_full_name(table: exp.Table) -> str:
    """Build a table name preserving ``[bracket]`` notation from AST nodes.

    Iterates over the ``catalog``, ``db``, and ``this`` arguments of the
    Table node, collecting bracketed identifier parts via
    :func:`_collect_node_parts`.

    :param table: sqlglot Table node parsed with TSQL dialect.
    :type table: exp.Table
    :returns: Dot-joined name with brackets preserved, or ``""`` if empty.
    :rtype: str
    """
    parts = []
    for key in ["catalog", "db", "this"]:
        node = table.args.get(key)
        if node is not None:
            _collect_node_parts(node, parts)
    return ".".join(parts) if parts else ""


def _is_word_char(c: str) -> bool:
    """Check whether *c* is an alphanumeric character or underscore.

    Used by :func:`_find_word` to enforce whole-word matching when
    locating table names in raw SQL.

    :param c: A single character.
    :type c: str
    :returns: ``True`` if *c* is ``[a-zA-Z0-9_]``.
    :rtype: bool
    """
    return c.isalnum() or c == "_"


def _find_word(name_upper: str, upper_sql: str, start: int = 0) -> int:
    """Find *name_upper* as a whole word in *upper_sql*.

    Performs a case-insensitive search (both arguments are expected to be
    upper-cased) and verifies that the match is not a substring of a
    larger identifier by checking adjacent characters.

    :param name_upper: Upper-cased table name to find.
    :type name_upper: str
    :param upper_sql: Upper-cased SQL string to search within.
    :type upper_sql: str
    :param start: Index to start searching from.
    :type start: int
    :returns: Index of the match, or ``-1`` if not found.
    :rtype: int
    """
    pos = start
    while True:
        pos = upper_sql.find(name_upper, pos)
        if pos < 0:
            return -1
        before_ok = pos == 0 or not _is_word_char(upper_sql[pos - 1])
        after_pos = pos + len(name_upper)
        after_ok = after_pos >= len(upper_sql) or not _is_word_char(
            upper_sql[after_pos]
        )
        if before_ok and after_ok:
            return pos
        pos += 1


#: SQL keywords that introduce a table-name context.  Used by
#: :func:`_find_word_in_table_context` to confirm that a name occurrence
#: is indeed in a table position (after FROM, JOIN, etc.).
_TABLE_CONTEXT_KEYWORDS = {"FROM", "JOIN", "TABLE", "INTO", "UPDATE"}


def _first_position(name: str, raw_sql: str) -> int:
    """Find the first occurrence of a table name in a table context.

    Tries :func:`_find_word_in_table_context` first with the full name,
    then with just the last dotted component (for ``schema.table`` where
    only ``table`` appears after ``FROM``), and finally falls back to an
    unrestricted whole-word search.

    :param name: Table name to locate.
    :type name: str
    :param raw_sql: Original SQL string.
    :type raw_sql: str
    :returns: Character index of the first occurrence, or ``len(raw_sql)``
        if not found (pushes unknown tables to the end of the sort).
    :rtype: int
    """
    upper = raw_sql.upper()
    name_upper = name.upper()

    # Search for name after a table context keyword (FROM, JOIN, TABLE, etc.)
    pos = _find_word_in_table_context(name_upper, upper)
    if pos >= 0:
        return pos

    # Try last component only (for schema.table, find just table)
    last_part = name_upper.split(".")[-1]
    pos = _find_word_in_table_context(last_part, upper)
    if pos >= 0:
        return pos

    # Fallback: find anywhere (for unusual contexts)
    pos = _find_word(name_upper, upper)
    return pos if pos >= 0 else len(raw_sql)


#: Keywords that *interrupt* a comma-separated table list (e.g.
#: ``FROM a, b WHERE ...`` — ``WHERE`` interrupts the FROM context).
_INTERRUPTING_KEYWORDS = {"SELECT", "WHERE", "ORDER", "GROUP", "HAVING", "SET"}


def _ends_with_table_keyword(before: str) -> bool:
    """Check whether *before* ends with a table-introducing keyword.

    :param before: Upper-cased, right-stripped SQL text preceding the name.
    :type before: str
    :returns: ``True`` if a keyword like ``FROM``, ``JOIN``, etc. is found.
    :rtype: bool
    """
    return any(before.endswith(kw) for kw in _TABLE_CONTEXT_KEYWORDS)


def _is_in_comma_list_after_keyword(before: str) -> bool:
    """Check whether a comma-preceded name belongs to a table list.

    Looks for the most recent table-context keyword before the trailing
    comma and verifies that no interrupting keyword (``SELECT``,
    ``WHERE``, etc.) appears between that keyword and the comma.

    :param before: Upper-cased, right-stripped SQL text preceding the
        name, already known to end with ``","``.
    :type before: str
    :returns: ``True`` if the name is part of a table list.
    :rtype: bool
    """
    best_kw_pos = -1
    for kw in _TABLE_CONTEXT_KEYWORDS:
        kw_pos = before.rfind(kw)
        if kw_pos > best_kw_pos:
            best_kw_pos = kw_pos
    if best_kw_pos < 0:
        return False
    between = before[best_kw_pos:]
    return not any(ik in between for ik in _INTERRUPTING_KEYWORDS)


def _find_word_in_table_context(name_upper: str, upper_sql: str) -> int:
    """Find a table name that appears after a table-introducing keyword.

    Checks each whole-word occurrence of *name_upper* to see whether it
    is immediately preceded by a keyword from :data:`_TABLE_CONTEXT_KEYWORDS`
    or is part of a comma-separated list following such a keyword (with no
    interrupting keyword in between).

    :param name_upper: Upper-cased table name to find.
    :type name_upper: str
    :param upper_sql: Upper-cased SQL string.
    :type upper_sql: str
    :returns: Index of the first table-context occurrence, or ``-1``.
    :rtype: int
    """
    pos = 0
    while True:
        pos = _find_word(name_upper, upper_sql, pos)
        if pos < 0:
            return -1
        before = upper_sql[:pos].rstrip()
        if _ends_with_table_keyword(before):
            return pos
        if before.endswith(",") and _is_in_comma_list_after_keyword(before):
            return pos
        pos += 1


def _extract_create_target(
    ast: exp.Expression, raw_sql: str, cte_names: Set[str], bracket_mode: bool
) -> str:
    """Extract the target table name from a ``CREATE TABLE`` statement.

    :param ast: A ``Create`` AST node.
    :type ast: exp.Expression
    :param raw_sql: Original SQL string.
    :type raw_sql: str
    :param cte_names: CTE names to exclude.
    :type cte_names: Set[str]
    :param bracket_mode: Whether bracket quoting is active.
    :type bracket_mode: bool
    :returns: Target table name, or ``None`` if not found.
    :rtype: Optional[str]
    """
    target = ast.this
    if not target:
        return None
    target_table = (
        target.find(exp.Table) if not isinstance(target, exp.Table) else target
    )
    if not target_table:
        return None
    name = _table_full_name(target_table, raw_sql, bracket_mode)
    if name and name not in cte_names:
        return name
    return None


def _collect_lateral_aliases(ast: exp.Expression, cte_names: Set[str]) -> List[str]:
    """Collect alias names from ``LATERAL VIEW`` clauses in the AST.

    :param ast: Root AST node.
    :type ast: exp.Expression
    :param cte_names: CTE names to exclude.
    :type cte_names: Set[str]
    :returns: List of lateral alias names not in *cte_names*.
    :rtype: List[str]
    """
    names = []
    for lateral in ast.find_all(exp.Lateral):
        alias = lateral.args.get("alias")
        if alias and alias.this:
            name = alias.this.name if hasattr(alias.this, "name") else str(alias.this)
            if name and name not in cte_names:
                names.append(name)
    return names


def _collect_all_tables(
    ast: exp.Expression, raw_sql: str, cte_names: Set[str], bracket_mode: bool
) -> "UniqueList":
    """Collect table names from ``Table`` and ``Lateral`` AST nodes.

    Filters out CTE names and returns an unsorted list.

    :param ast: Root AST node.
    :type ast: exp.Expression
    :param raw_sql: Original SQL string.
    :type raw_sql: str
    :param cte_names: CTE names to exclude.
    :type cte_names: Set[str]
    :param bracket_mode: Whether bracket quoting is active.
    :type bracket_mode: bool
    :returns: Unsorted list of unique table names.
    :rtype: UniqueList
    """
    collected = UniqueList()
    for table in ast.find_all(exp.Table):
        full_name = _table_full_name(table, raw_sql, bracket_mode)
        if full_name and full_name not in cte_names:
            collected.append(full_name)
    for name in _collect_lateral_aliases(ast, cte_names):
        collected.append(name)
    return collected


def _place_tables_in_order(create_target: str, collected_sorted: list) -> "UniqueList":
    """Build the final table list with optional CREATE target first.

    :param create_target: Target table name for CREATE, or ``None``.
    :type create_target: Optional[str]
    :param collected_sorted: Position-sorted table names.
    :type collected_sorted: list
    :returns: Ordered unique list of table names.
    :rtype: UniqueList
    """
    tables = UniqueList()
    if create_target:
        tables.append(create_target)
        for t in collected_sorted:
            if t != create_target:
                tables.append(t)
    else:
        for t in collected_sorted:
            tables.append(t)
    return tables


def extract_tables(
    ast: exp.Expression,
    raw_sql: str = "",
    cte_names: Set[str] = None,
    dialect=None,
) -> List[str]:
    """Extract table names from *ast*, excluding CTE definitions.

    Collects all ``exp.Table`` nodes (and ``exp.Lateral`` aliases for
    Hive ``LATERAL VIEW`` clauses), filters out names that match known
    CTE names, and sorts the results by their first occurrence in
    *raw_sql* so the output order matches left-to-right reading order.

    For ``CREATE TABLE`` statements the target table is always placed
    first regardless of its position in the SQL.

    Called by :attr:`Parser.tables`.

    :param ast: Root AST node.
    :type ast: exp.Expression
    :param raw_sql: Original SQL string, used for position-based sorting.
    :type raw_sql: str
    :param cte_names: Set of CTE names to exclude from the result.
    :type cte_names: Optional[Set[str]]
    :param dialect: The dialect used to parse the AST, checked to enable
        bracket-mode table name construction.
    :type dialect: Optional[Union[str, type]]
    :returns: Ordered list of unique table names.
    :rtype: List[str]
    """
    if ast is None:
        return []

    from sql_metadata._ast import _BracketedTableDialect

    cte_names = cte_names or set()
    bracket_mode = isinstance(dialect, type) and issubclass(
        dialect, _BracketedTableDialect
    )

    if isinstance(ast, exp.Command):
        return _extract_tables_from_command(raw_sql)

    create_target = None
    if isinstance(ast, exp.Create):
        create_target = _extract_create_target(ast, raw_sql, cte_names, bracket_mode)

    collected = _collect_all_tables(ast, raw_sql, cte_names, bracket_mode)
    collected_sorted = sorted(collected, key=lambda t: _first_position(t, raw_sql))
    return _place_tables_in_order(create_target, collected_sorted)


def _extract_tables_from_command(raw_sql: str) -> List[str]:
    """Extract table names from queries that sqlglot parsed as ``Command``.

    Handles ``ALTER TABLE ... APPEND FROM ...`` and similar statements
    where sqlglot does not produce a structured AST.  Falls back to
    regex matching against the raw SQL.

    :param raw_sql: Original SQL string.
    :type raw_sql: str
    :returns: List of table names found.
    :rtype: List[str]
    """
    import re

    tables = UniqueList()

    # ALTER TABLE table APPEND FROM table
    match = re.search(
        r"ALTER\s+TABLE\s+(\S+)",
        raw_sql,
        re.IGNORECASE,
    )
    if match:
        tables.append(match.group(1).strip("`").strip('"'))
    # Also check for FROM in ALTER TABLE
    from_match = re.search(
        r"\bFROM\s+(\S+)",
        raw_sql,
        re.IGNORECASE,
    )
    if from_match:
        tables.append(from_match.group(1).strip("`").strip('"'))

    return tables


def extract_table_aliases(
    ast: exp.Expression,
    tables: List[str],
) -> Dict[str, str]:
    """Extract table alias mappings from the AST.

    Iterates over all ``exp.Table`` nodes that have an alias and whose
    full name appears in the known *tables* list.  Returns a dictionary
    mapping each alias to its resolved table name.

    Called by :attr:`Parser.tables_aliases`.

    :param ast: Root AST node.
    :type ast: exp.Expression
    :param tables: List of known table names (from :func:`extract_tables`).
    :type tables: List[str]
    :returns: Mapping of ``{alias: table_name}``.
    :rtype: Dict[str, str]
    """
    if ast is None:
        return {}

    aliases = {}
    for table in ast.find_all(exp.Table):
        alias = table.alias
        if not alias:
            continue
        full_name = _table_full_name(table)
        if full_name in tables:
            aliases[alias] = full_name

    return aliases
