"""
Module wrapping sqlglot.parse() to produce an AST from SQL strings.
"""

import re

import sqlglot
from sqlglot import Dialect
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError
from sqlglot.tokens import Tokenizer

from sql_metadata._comments import strip_comments_for_parsing as _strip_comments


class _HashVarDialect(Dialect):
    """Dialect that treats #WORD as identifiers (MSSQL variables)."""

    class Tokenizer(Tokenizer):
        SINGLE_TOKENS = {**Tokenizer.SINGLE_TOKENS}
        SINGLE_TOKENS.pop("#", None)
        VAR_SINGLE_TOKENS = {*Tokenizer.VAR_SINGLE_TOKENS, "#"}


def _strip_outer_parens(sql: str) -> str:
    """Strip redundant outer parentheses from SQL."""
    stripped = sql.strip()
    while stripped.startswith("(") and stripped.endswith(")"):
        # Verify these parens are balanced (not part of inner expression)
        depth = 0
        balanced = True
        for i, char in enumerate(stripped):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            if depth == 0 and i < len(stripped) - 1:
                balanced = False
                break
        if balanced:
            stripped = stripped[1:-1].strip()
        else:
            break
    return stripped


def _normalize_cte_names(sql: str) -> tuple:
    """
    Replace qualified CTE names (e.g., db.cte_name) with simple placeholders.
    Returns (modified_sql, {placeholder: original_name}).
    """
    name_map = {}
    # Find WITH ... AS patterns with qualified names
    pattern = re.compile(
        r"(\bWITH\s+|,\s*)(\w+\.\w+)(\s+AS\s*\()",
        re.IGNORECASE,
    )

    def replacer(match):
        prefix = match.group(1)
        qualified_name = match.group(2)
        suffix = match.group(3)
        # Create a placeholder with double underscores
        placeholder = qualified_name.replace(".", "__DOT__")
        name_map[placeholder] = qualified_name
        return f"{prefix}{placeholder}{suffix}"

    modified = pattern.sub(replacer, sql)

    # Also replace references to qualified CTE names in FROM/JOIN clauses
    for placeholder, original in name_map.items():
        # Replace references but not the definition (already replaced)
        # Use word boundary to avoid partial matches
        modified = re.sub(
            r"\b" + re.escape(original) + r"\b",
            placeholder,
            modified,
        )

    return modified, name_map


class ASTParser:
    """
    Wraps sqlglot.parse() with error handling.
    """

    def __init__(self, sql: str) -> None:
        self._raw_sql = sql
        self._ast = None
        self._dialect = None
        self._parsed = False
        self._cte_name_map = {}  # placeholder → original qualified name

    @property
    def ast(self) -> exp.Expression:
        if self._parsed:
            return self._ast
        self._parsed = True
        self._ast = self._parse(self._raw_sql)
        return self._ast

    @property
    def cte_name_map(self) -> dict:
        """Map of placeholder names to original qualified CTE names."""
        # Ensure parsing has happened
        _ = self.ast
        return self._cte_name_map

    def _parse(self, sql: str) -> exp.Expression:
        if not sql or not sql.strip():
            return None

        # Strip comments for parsing (sqlglot handles most, but not # comments)
        clean_sql = _strip_comments(sql)
        if not clean_sql.strip():
            return None

        # Normalize qualified CTE names (e.g., database1.tableFromWith → placeholder)
        clean_sql, self._cte_name_map = _normalize_cte_names(clean_sql)

        # Strip DB2 isolation level clause
        clean_sql = re.sub(
            r"\bwith\s+(ur|cs|rs|rr)\s*$", "", clean_sql, flags=re.IGNORECASE
        ).strip()

        # Detect malformed WITH...AS(...)  AS (extra AS after CTE body)
        if re.match(r"\s*WITH\b", clean_sql, re.IGNORECASE):
            _MAIN_KW = r"(?:SELECT|INSERT|UPDATE|DELETE)"
            # Pattern: ) AS <keyword> or ) AS <word> <keyword>
            if re.search(
                r"\)\s+AS\s+" + _MAIN_KW + r"\b", clean_sql, re.IGNORECASE
            ) or re.search(
                r"\)\s+AS\s+\w+\s+" + _MAIN_KW + r"\b",
                clean_sql,
                re.IGNORECASE,
            ):
                raise ValueError("This query is wrong")

        # Strip redundant outer parentheses
        clean_sql = _strip_outer_parens(clean_sql)
        if not clean_sql.strip():
            return None

        # Determine dialect order based on SQL features
        dialects = self._detect_dialects(clean_sql)
        last_result = None
        for dialect in dialects:
            try:
                import logging

                # Capture parse errors at WARN level
                logger = logging.getLogger("sqlglot")
                old_level = logger.level
                logger.setLevel(logging.CRITICAL)
                try:
                    results = sqlglot.parse(
                        clean_sql,
                        dialect=dialect,
                        error_level=sqlglot.ErrorLevel.WARN,
                    )
                finally:
                    logger.setLevel(old_level)

                if results and results[0] is not None:
                    result = results[0]
                    # Unwrap Subquery wrapper from parenthesized queries
                    if isinstance(result, exp.Subquery) and not result.alias:
                        result = result.this

                    last_result = result

                    # Check if parse result is degraded - try next dialect
                    if dialect != dialects[-1]:
                        if (
                            isinstance(result, exp.Command)
                            and not self._is_expected_command(clean_sql)
                        ):
                            continue
                        # Check for degraded parse results
                        if self._has_parse_issues(result, clean_sql):
                            continue
                    self._dialect = dialect
                    return result
            except (ParseError, TokenError):
                if dialect is not None and dialect == dialects[-1]:
                    raise ValueError("This query is wrong")
                continue

        # Return last successful result if any
        if last_result is not None:
            return last_result
        raise ValueError("This query is wrong")

    @staticmethod
    def _is_expected_command(sql: str) -> bool:
        """Check if the SQL is expected to be parsed as a Command."""
        upper = sql.strip().upper()
        return upper.startswith("REPLACE") or upper.startswith("CREATE FUNCTION")

    @staticmethod
    def _has_parse_issues(ast: exp.Expression, sql: str = "") -> bool:
        """Check if AST has signs of failed/degraded parse."""
        _BAD_TABLE_NAMES = {"IGNORE", ""}
        for table in ast.find_all(exp.Table):
            if table.name in _BAD_TABLE_NAMES:
                return True
        # Check if a SQL keyword appears as a column name (likely wrong parse)
        _SQL_KEYWORDS = {"UNIQUE", "DISTINCT", "SELECT", "FROM", "WHERE"}
        for col in ast.find_all(exp.Column):
            if col.name.upper() in _SQL_KEYWORDS and not col.table:
                return True
        return False

    @staticmethod
    def _detect_dialects(sql: str) -> list:
        """Detect which dialects to try based on SQL features."""
        from sql_metadata._comments import _has_hash_variables

        upper = sql.upper()
        # #WORD variables (MSSQL) — use custom dialect that treats # as identifier
        if _has_hash_variables(sql):
            return [_HashVarDialect, None, "mysql"]
        if "`" in sql:
            return ["mysql", None]
        if "[" in sql:
            return ["tsql", None, "mysql"]
        if " TOP " in upper:
            return ["tsql", None, "mysql"]
        if " UNIQUE " in upper:
            return [None, "mysql", "oracle"]
        if "LATERAL VIEW" in upper:
            return ["spark", None, "mysql"]
        return [None, "mysql"]
