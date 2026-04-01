"""Extract tables and table aliases from a sqlglot AST.

The :class:`TableExtractor` class walks the AST for ``exp.Table`` and
``exp.Lateral`` nodes, builds fully-qualified table names (optionally
preserving ``[bracket]`` notation for TSQL), and sorts results by their
first occurrence in the raw SQL so the output order matches left-to-right
reading order.  CTE names are excluded from the result so that only *real*
tables are reported.
"""

import re
from typing import Dict, List, Optional, Set

from sqlglot import exp

from sql_metadata.utils import UniqueList, last_segment

# ---------------------------------------------------------------------------
# Pure static helpers (no instance state needed)
# ---------------------------------------------------------------------------


def _assemble_dotted_name(catalog: str, db: object, name: str) -> str:
    """Assemble a dot-joined table name from catalog, db, and name parts."""
    parts: list[str] = []
    if catalog:
        parts.append(catalog)
    if db is not None:
        db_str = str(db)
        # TODO: revisit if catalog..table bypasses shortcut
        if db_str == "" and catalog:  # pragma: no cover
            parts.append("")
        elif db_str:
            parts.append(db_str)
    if name:
        parts.append(name)
    return ".".join(parts)


def _ident_str(node: exp.Identifier) -> str:
    """Return an identifier string, wrapping it in ``[brackets]`` if quoted."""
    return f"[{node.name}]" if node.quoted else node.name


def _collect_node_parts(node: object, parts: list[str]) -> None:
    """Append identifier strings from *node* into *parts*."""
    if isinstance(node, exp.Identifier):
        parts.append(_ident_str(node))
    elif isinstance(node, exp.Dot):
        for sub in [node.this, node.expression]:
            if isinstance(sub, exp.Identifier):
                parts.append(_ident_str(sub))


def _bracketed_full_name(table: exp.Table) -> str:
    """Build a table name preserving ``[bracket]`` notation from AST nodes."""
    parts: list[str] = []
    for key in ["catalog", "db", "this"]:
        node = table.args.get(key)
        if node is not None:
            _collect_node_parts(node, parts)
    return ".".join(parts) if parts else ""


def _ends_with_table_keyword(before: str) -> bool:
    """Check whether *before* ends with a table-introducing keyword."""
    return any(before.endswith(kw) for kw in _TABLE_CONTEXT_KEYWORDS)


def _is_in_comma_list_after_keyword(before: str) -> bool:
    """Check whether a comma-preceded name belongs to a table list."""
    best_kw_pos = -1
    for kw in _TABLE_CONTEXT_KEYWORDS:
        kw_pos = before.rfind(kw)
        if kw_pos > best_kw_pos:
            best_kw_pos = kw_pos
    if best_kw_pos < 0:
        return False
    between = before[best_kw_pos:]
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

    :param ast: Root AST node.
    :param raw_sql: Original SQL string, used for position-based sorting.
    :param cte_names: Set of CTE names to exclude from the result.
    :param dialect: The dialect used to parse the AST.
    """

    def __init__(
        self,
        ast: Optional[exp.Expression],
        raw_sql: str = "",
        cte_names: Optional[Set[str]] = None,
        dialect: object = None,
    ):
        self._ast = ast
        self._raw_sql = raw_sql
        self._upper_sql = raw_sql.upper()
        self._cte_names = cte_names or set()

        from sql_metadata.dialect_parser import BracketedTableDialect

        self._bracket_mode = isinstance(dialect, type) and issubclass(
            dialect, BracketedTableDialect
        )
        self._cached_table_nodes: Optional[List[exp.Table]] = None

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def extract(self) -> List[str]:
        """Extract table names, excluding CTE definitions.

        Sorts results by first occurrence in raw SQL (left-to-right order).
        For ``CREATE TABLE`` statements the target table is always first.
        """
        if self._ast is None:  # pragma: no cover — Parser always provides an AST
            return []

        if isinstance(self._ast, exp.Command):
            return self._extract_tables_from_command()

        create_target = None
        if isinstance(self._ast, exp.Create):
            create_target = self._extract_create_target()

        collected = self._collect_all()
        collected_sorted = sorted(collected, key=lambda t: self._first_position(t))
        return self._place_tables_in_order(create_target, collected_sorted)

    def _table_nodes(self) -> List[exp.Table]:
        """Return all ``exp.Table`` nodes from the AST (cached)."""
        if self._cached_table_nodes is None:
            assert self._ast is not None
            self._cached_table_nodes = list(self._ast.find_all(exp.Table))
        return self._cached_table_nodes

    def extract_aliases(self, tables: List[str]) -> Dict[str, str]:
        """Extract table alias mappings from the AST.

        :param tables: List of known table names.
        :returns: Mapping of ``{alias: table_name}``.
        """
        if self._ast is None:  # pragma: no cover — Parser always provides an AST
            return {}

        aliases = {}
        for table in self._table_nodes():
            alias = table.alias
            if not alias:
                continue
            full_name = self._table_full_name(table)
            if full_name in tables:
                aliases[alias] = full_name

        return aliases

    # -------------------------------------------------------------------
    # Table name construction
    # -------------------------------------------------------------------

    def _table_full_name(self, table: exp.Table) -> str:
        """Build a fully-qualified table name from an ``exp.Table`` node."""
        name = table.name

        if self._bracket_mode:
            bracketed = _bracketed_full_name(table)
            if bracketed:
                return bracketed

        if self._raw_sql and name and f"..{name}" in self._raw_sql:
            catalog = table.catalog
            return f"{catalog}..{name}" if catalog else f"..{name}"

        return _assemble_dotted_name(table.catalog, table.db, name)

    # -------------------------------------------------------------------
    # Position detection
    # -------------------------------------------------------------------

    def _first_position(self, name: str) -> int:
        """Find the first occurrence of a table name in a table context."""
        name_upper = name.upper()

        pos = self._find_word_in_table_context(name_upper)
        if pos >= 0:
            return pos

        last_part = last_segment(name_upper)
        pos = self._find_word_in_table_context(last_part)
        # TODO: revisit if qualified table names stop being found by full name above
        if pos >= 0:  # pragma: no cover
            return pos

        pos = self._find_word(name_upper)
        return pos if pos >= 0 else len(self._raw_sql)

    _pattern_cache: Dict[str, re.Pattern[str]] = {}

    @staticmethod
    def _word_pattern(name_upper: str) -> re.Pattern[str]:
        """Build a regex matching *name_upper* as a whole word (cached)."""
        pat = TableExtractor._pattern_cache.get(name_upper)
        if pat is None:
            escaped = re.escape(name_upper)
            pat = re.compile(r"(?<![A-Za-z0-9_])" + escaped + r"(?![A-Za-z0-9_])")
            TableExtractor._pattern_cache[name_upper] = pat
        return pat

    def _find_word(self, name_upper: str, start: int = 0) -> int:
        """Find *name_upper* as a whole word in the upper-cased SQL."""
        match = self._word_pattern(name_upper).search(self._upper_sql, start)
        return int(match.start()) if match else -1

    def _find_word_in_table_context(self, name_upper: str) -> int:
        """Find a table name that appears after a table-introducing keyword."""
        for match in self._word_pattern(name_upper).finditer(self._upper_sql):
            pos: int = int(match.start())
            before = self._upper_sql[:pos].rstrip()
            if _ends_with_table_keyword(before):
                return pos
            if before.endswith(",") and _is_in_comma_list_after_keyword(before):
                return pos
        return -1

    # -------------------------------------------------------------------
    # Collection helpers
    # -------------------------------------------------------------------

    def _extract_create_target(self) -> Optional[str]:
        """Extract the target table name from a CREATE TABLE statement."""
        assert self._ast is not None
        target = self._ast.this
        # TODO: revisit if sqlglot produces CREATE without .this target
        if not target:  # pragma: no cover
            return None
        target_table = (
            target.find(exp.Table) if not isinstance(target, exp.Table) else target
        )
        # TODO: revisit if sqlglot produces CREATE target without a Table node
        if not target_table:  # pragma: no cover
            return None
        name = self._table_full_name(target_table)
        if name and name not in self._cte_names:
            return name
        # TODO: revisit if CTE-named CREATE targets become possible
        return None  # pragma: no cover

    def _collect_all(self) -> UniqueList:
        """Collect table names from Table AST nodes."""
        assert self._ast is not None
        collected = UniqueList()
        for table in self._table_nodes():
            full_name = self._table_full_name(table)
            if full_name and full_name not in self._cte_names:
                collected.append(full_name)
        return collected

    @staticmethod
    def _place_tables_in_order(
        create_target: Optional[str], collected_sorted: list
    ) -> UniqueList:
        """Build the final table list with optional CREATE target first."""
        tables = UniqueList()
        if create_target:
            tables.append(create_target)
        for t in collected_sorted:
            tables.append(t)
        return tables

    def _extract_tables_from_command(self) -> List[str]:
        """Extract table names from queries parsed as Command (regex fallback)."""
        import re

        tables = UniqueList()

        match = re.search(
            r"ALTER\s+TABLE\s+(\S+)",
            self._raw_sql,
            re.IGNORECASE,
        )
        if match:
            tables.append(match.group(1).strip("`").strip('"'))
        from_match = re.search(
            r"\bFROM\s+(\S+)",
            self._raw_sql,
            re.IGNORECASE,
        )
        if from_match:
            tables.append(from_match.group(1).strip("`").strip('"'))

        return tables
