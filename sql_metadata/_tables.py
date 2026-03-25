"""
Module to extract tables and table aliases from sqlglot AST.
"""

from typing import Dict, List, Set

from sqlglot import exp

from sql_metadata.utils import UniqueList


def _table_full_name(table: exp.Table, raw_sql: str = "") -> str:
    """Build fully-qualified table name from a Table node."""
    parts = []
    catalog = table.catalog
    db = table.db
    name = table.name

    # Handle MSSQL bracket notation
    if raw_sql and "[" in raw_sql:
        # Try to find the bracketed version in raw SQL
        bracketed = _find_bracketed_table(table, raw_sql)
        if bracketed:
            return bracketed

    # Check for double-dot notation in raw SQL (e.g., ..table or db..table)
    if raw_sql and name and f"..{name}" in raw_sql:
        if catalog:
            return f"{catalog}..{name}"
        return f"..{name}"

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


def _find_bracketed_table(table: exp.Table, raw_sql: str) -> str:
    """Find the original bracketed table name from raw SQL."""
    import re

    name = table.name
    db = table.db or ""
    catalog = table.catalog or ""

    # Try to find the original bracketed name in SQL
    # Build possible patterns
    parts = []
    for part in [catalog, db, name]:
        if part:
            # Try bracketed first, then plain
            if f"[{part}]" in raw_sql:
                parts.append(f"[{part}]")
            else:
                parts.append(part)
        elif part == "" and parts:
            # Empty schema (db..table)
            parts.append("")

    candidate = ".".join(parts)
    if candidate in raw_sql:
        return candidate

    # Also try with dbo schema for MSSQL 4-part names
    if catalog and db and name:
        pattern = re.compile(
            r"\[?" + re.escape(catalog) + r"\]?\.\[?" + re.escape(db)
            + r"\]?\.\[?\w*\]?\.\[?" + re.escape(name) + r"\]?"
        )
        match = pattern.search(raw_sql)
        if match:
            return match.group(0)

    return ""


def _is_word_char(c: str) -> bool:
    return c.isalnum() or c == "_"


def _find_word(name_upper: str, upper_sql: str, start: int = 0) -> int:
    """Find name as a whole word in SQL (not as a substring of another identifier)."""
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


_TABLE_CONTEXT_KEYWORDS = {"FROM", "JOIN", "TABLE", "INTO", "UPDATE"}


def _first_position(name: str, raw_sql: str) -> int:
    """Find first occurrence of table name in a FROM/JOIN/TABLE context in raw SQL."""
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


_INTERRUPTING_KEYWORDS = {"SELECT", "WHERE", "ORDER", "GROUP", "HAVING", "SET"}


def _find_word_in_table_context(name_upper: str, upper_sql: str) -> int:
    """Find table name after FROM/JOIN/TABLE keywords (including comma-separated)."""
    pos = 0
    while True:
        pos = _find_word(name_upper, upper_sql, pos)
        if pos < 0:
            return -1
        before = upper_sql[:pos].rstrip()
        # Direct keyword before the name
        for kw in _TABLE_CONTEXT_KEYWORDS:
            if before.endswith(kw):
                return pos
        # Comma-separated: check if there's a FROM/JOIN before the comma
        # without an interrupting keyword (SELECT, WHERE, etc.) in between
        if before.endswith(","):
            # Find the most recent table context keyword
            best_kw_pos = -1
            for kw in _TABLE_CONTEXT_KEYWORDS:
                kw_pos = before.rfind(kw)
                if kw_pos > best_kw_pos:
                    best_kw_pos = kw_pos
            if best_kw_pos >= 0:
                between = before[best_kw_pos:]
                if not any(
                    ik in between for ik in _INTERRUPTING_KEYWORDS
                ):
                    return pos
        pos += 1


def extract_tables(
    ast: exp.Expression,
    raw_sql: str = "",
    cte_names: Set[str] = None,
) -> List[str]:
    """
    Extract table names from AST, excluding CTE names.
    Tables are sorted by their first occurrence in the raw SQL (left-to-right).
    """
    if ast is None:
        return []

    cte_names = cte_names or set()
    tables = UniqueList()

    # Handle REPLACE INTO parsed as Command
    if isinstance(ast, exp.Command):
        return _extract_tables_from_command(raw_sql)

    create_target = None
    # For CREATE TABLE, extract the target table first
    if isinstance(ast, exp.Create):
        target = ast.this
        if target:
            target_table = (
                target.find(exp.Table)
                if not isinstance(target, exp.Table)
                else target
            )
            if target_table:
                name = _table_full_name(target_table, raw_sql)
                if name and name not in cte_names:
                    create_target = name

    # Collect all tables from AST (including LATERAL VIEW aliases)
    collected = UniqueList()
    for table in ast.find_all(exp.Table):
        full_name = _table_full_name(table, raw_sql)
        if not full_name or full_name in cte_names:
            continue
        collected.append(full_name)
    for lateral in ast.find_all(exp.Lateral):
        alias = lateral.args.get("alias")
        if alias and alias.this:
            name = alias.this.name if hasattr(alias.this, "name") else str(alias.this)
            if name and name not in cte_names:
                collected.append(name)

    # Sort by position in raw SQL (left-to-right order)
    collected_sorted = sorted(collected, key=lambda t: _first_position(t, raw_sql))

    # For CREATE TABLE, target goes first
    if create_target:
        tables.append(create_target)
        for t in collected_sorted:
            if t != create_target:
                tables.append(t)
    else:
        for t in collected_sorted:
            tables.append(t)

    return tables


def _extract_tables_from_command(raw_sql: str) -> List[str]:
    """Extract tables from Command-parsed queries via regex."""
    import re

    tables = UniqueList()

    # REPLACE/INSERT INTO table
    match = re.search(
        r"(?:REPLACE|INSERT)\s+(?:IGNORE\s+)?INTO\s+(\S+)",
        raw_sql,
        re.IGNORECASE,
    )
    if match:
        table = match.group(1).strip("`").strip('"').strip("'").rstrip("(")
        tables.append(table)
        return tables

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
    """
    Extract table alias mapping {alias: table_name}.
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
