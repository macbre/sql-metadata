"""
Module to extract and strip comments from SQL using sqlglot tokenizer.

Uses sqlglot's tokenizer to identify comments (which are skipped during
tokenization), then extracts them from the gaps between token positions.
"""

from typing import List

from sqlglot.tokens import Tokenizer


def _choose_tokenizer(sql: str):
    """Choose tokenizer: MySQL for # comments, default otherwise."""
    if "#" in sql and not _has_hash_variables(sql):
        from sqlglot.dialects.mysql import MySQL

        return MySQL.Tokenizer()
    return Tokenizer()


def _has_hash_variables(sql: str) -> bool:
    """Check if # is used as variable/template prefix (not comment)."""
    pos = sql.find("#")
    while pos >= 0:
        end = pos + 1
        while end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
            end += 1
        if end > pos + 1:
            # #WORD# template variable
            if end < len(sql) and sql[end] == "#":
                return True
            # = #WORD or (#WORD variable reference
            before = pos - 1
            while before >= 0 and sql[before] in " \t":
                before -= 1
            if before >= 0 and sql[before] in "=(":
                return True
        pos = sql.find("#", max(end, pos + 1))
    return False


def extract_comments(sql: str) -> List[str]:
    """
    Extract all SQL comments with delimiters preserved.
    Uses sqlglot tokenizer to find gaps where comments live.
    """
    if not sql:
        return []
    try:
        tokens = list(_choose_tokenizer(sql).tokenize(sql))
    except Exception:
        return []
    comments = []
    prev_end = -1
    for tok in tokens:
        _scan_gap(sql, prev_end + 1, tok.start, comments)
        prev_end = tok.end
    _scan_gap(sql, prev_end + 1, len(sql), comments)
    return comments


def _scan_gap(sql: str, start: int, end: int, out: list) -> None:
    """Scan text between token positions for comment delimiters."""
    gap = sql[start:end]
    i = 0
    while i < len(gap):
        if gap[i : i + 2] == "/*":
            close = gap.find("*/", i + 2)
            if close >= 0:
                out.append(gap[i : close + 2])
                i = close + 2
            else:
                out.append(gap[i:])
                return
        elif gap[i : i + 2] == "--":
            nl = gap.find("\n", i)
            out.append(gap[i : nl + 1] if nl >= 0 else gap[i:])
            i = nl + 1 if nl >= 0 else len(gap)
        elif gap[i] == "#":
            nl = gap.find("\n", i)
            out.append(gap[i : nl + 1] if nl >= 0 else gap[i:])
            i = nl + 1 if nl >= 0 else len(gap)
        else:
            i += 1


def strip_comments_for_parsing(sql: str) -> str:
    """
    Strip ALL comments including # hash lines for sqlglot parsing.
    Uses MySQL tokenizer which treats # as comment delimiter,
    except for REPLACE queries where MySQL tokenizer fails.
    """
    if not sql:
        return sql or ""
    # MySQL tokenizer breaks on REPLACE INTO — use default for those
    # Skip MySQL tokenizer when # is used as variable (not comment)
    upper = sql.strip().upper()
    if (
        upper.startswith("REPLACE")
        or upper.startswith("CREATE FUNCTION")
        or _has_hash_variables(sql)
    ):
        tokenizer = Tokenizer()
    else:
        from sqlglot.dialects.mysql import MySQL

        tokenizer = MySQL.Tokenizer()
    try:
        tokens = list(tokenizer.tokenize(sql))
    except Exception:
        return sql.strip()
    if not tokens:
        return ""
    parts = [sql[tokens[0].start : tokens[0].end + 1]]
    for i in range(1, len(tokens)):
        if tokens[i].start > tokens[i - 1].end + 1:
            parts.append(" ")
        parts.append(sql[tokens[i].start : tokens[i].end + 1])
    return "".join(parts).strip()


def strip_comments(sql: str) -> str:
    """
    Remove comments and normalize whitespace using sqlglot tokenizer.
    Preserves original token spacing (no space added where none existed).
    Preserves #VAR template variables (not treated as comments).
    """
    if not sql:
        return sql or ""
    try:
        tokens = list(_choose_tokenizer(sql).tokenize(sql))
    except Exception:
        return sql.strip()
    if not tokens:
        return ""
    parts = [sql[tokens[0].start : tokens[0].end + 1]]
    for i in range(1, len(tokens)):
        if tokens[i].start > tokens[i - 1].end + 1:
            parts.append(" ")
        parts.append(sql[tokens[i].start : tokens[i].end + 1])
    return "".join(parts).strip()
