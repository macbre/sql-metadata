"""
Extract original SQL text for CTE/subquery bodies using sqlglot tokenizer.

Preserves original casing and quoting by reconstructing from token positions.
"""

from typing import Dict, List

from sqlglot.tokens import TokenType

from sql_metadata._comments import _choose_tokenizer


def _choose_body_tokenizer(sql: str):
    """Choose tokenizer for body extraction: MySQL for backticks when safe."""
    if "`" in sql:
        from sqlglot.dialects.mysql import MySQL
        return MySQL.Tokenizer()
    return _choose_tokenizer(sql)


# ---------------------------------------------------------------------------
# Token reconstruction
# ---------------------------------------------------------------------------

# SQL keywords that need a space before (
_KW_BEFORE_PAREN = {
    TokenType.WHERE, TokenType.IN, TokenType.ON, TokenType.AND, TokenType.OR,
    TokenType.NOT, TokenType.HAVING, TokenType.FROM, TokenType.JOIN,
    TokenType.VALUES, TokenType.SET, TokenType.BETWEEN, TokenType.WHEN,
    TokenType.THEN, TokenType.ELSE, TokenType.USING, TokenType.INTO,
    TokenType.TABLE, TokenType.OVER, TokenType.PARTITION_BY,
    TokenType.ORDER_BY, TokenType.GROUP_BY, TokenType.WINDOW,
    TokenType.EXISTS, TokenType.SELECT, TokenType.INNER, TokenType.OUTER,
    TokenType.LEFT, TokenType.RIGHT, TokenType.CROSS, TokenType.FULL,
    TokenType.NATURAL, TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE,
    TokenType.WITH, TokenType.RETURNING, TokenType.UNION, TokenType.LIMIT,
    TokenType.OFFSET, TokenType.DISTINCT,
}


def _no_space(prev, curr) -> bool:
    if prev.token_type == TokenType.DOT or curr.token_type == TokenType.DOT:
        return True
    if curr.token_type in (TokenType.COMMA, TokenType.SEMICOLON, TokenType.R_PAREN):
        return True
    if prev.token_type == TokenType.L_PAREN:
        return True
    if curr.token_type == TokenType.L_PAREN:
        # Space before ( after keywords, operators, and comma
        if (
            prev.token_type in _KW_BEFORE_PAREN
            or prev.token_type in (TokenType.STAR, TokenType.COMMA)
        ):
            return False
        return True
    return False


def _reconstruct(tokens, sql: str) -> str:
    """Reconstruct SQL from tokens preserving original casing and quotes."""
    if not tokens:
        return ""

    def _text(tok):
        if tok.token_type == TokenType.IDENTIFIER:
            return tok.text  # strip backticks
        return sql[tok.start: tok.end + 1]

    parts = [_text(tokens[0])]
    for i in range(1, len(tokens)):
        if not _no_space(tokens[i - 1], tokens[i]):
            parts.append(" ")
        parts.append(_text(tokens[i]))
    return "".join(parts)


# ---------------------------------------------------------------------------
# Body extraction
# ---------------------------------------------------------------------------

def extract_cte_bodies(sql: str, cte_names: List[str]) -> Dict[str, str]:  # noqa: C901
    """Extract CTE body SQL preserving original casing."""
    if not sql or not cte_names:
        return {}
    try:
        tokens = list(_choose_body_tokenizer(sql).tokenize(sql))
    except Exception:
        return {}

    name_map = {}
    for name in cte_names:
        name_map[name.split(".")[-1].upper()] = name

    results = {}
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if (
            tok.token_type in (TokenType.VAR, TokenType.IDENTIFIER)
            and tok.text.upper() in name_map
        ):
            cte_name = name_map[tok.text.upper()]
            j = i + 1
            # Skip optional column definitions: name (c1, c2) AS (...)
            if j < len(tokens) and tokens[j].token_type == TokenType.L_PAREN:
                depth = 1
                j += 1
                while j < len(tokens) and depth > 0:
                    if tokens[j].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[j].token_type == TokenType.R_PAREN:
                        depth -= 1
                    j += 1
            # Should be at AS
            if (
                j < len(tokens)
                and tokens[j].token_type == TokenType.ALIAS
                and tokens[j].text.upper() == "AS"
            ):
                j += 1
                if j < len(tokens) and tokens[j].token_type == TokenType.L_PAREN:
                    body_tokens = []
                    depth = 1
                    j += 1
                    while j < len(tokens) and depth > 0:
                        if tokens[j].token_type == TokenType.L_PAREN:
                            depth += 1
                        elif tokens[j].token_type == TokenType.R_PAREN:
                            depth -= 1
                            if depth == 0:
                                break
                        body_tokens.append(tokens[j])
                        j += 1
                    if body_tokens:
                        results[cte_name] = _reconstruct(body_tokens, sql)
                    i = j + 1
                    continue
        i += 1
    return results


def extract_subquery_bodies(  # noqa: C901
    sql: str, subquery_names: List[str]
) -> Dict[str, str]:
    """Extract subquery body SQL preserving original casing."""
    if not sql or not subquery_names:
        return {}
    try:
        tokens = list(_choose_body_tokenizer(sql).tokenize(sql))
    except Exception:
        return {}

    names_upper = {n.upper(): n for n in subquery_names}
    results = {}

    for i, tok in enumerate(tokens):
        if (
            tok.token_type in (TokenType.VAR, TokenType.IDENTIFIER)
            and tok.text.upper() in names_upper
        ):
            original_name = names_upper[tok.text.upper()]
            j = i - 1
            if j >= 0 and tokens[j].token_type == TokenType.ALIAS:
                j -= 1
            if j >= 0 and tokens[j].token_type == TokenType.R_PAREN:
                body_reversed = []
                depth = 1
                j -= 1
                while j >= 0 and depth > 0:
                    if tokens[j].token_type == TokenType.R_PAREN:
                        depth += 1
                    elif tokens[j].token_type == TokenType.L_PAREN:
                        depth -= 1
                        if depth == 0:
                            break
                    body_reversed.append(tokens[j])
                    j -= 1
                if body_reversed:
                    results[original_name] = _reconstruct(
                        list(reversed(body_reversed)), sql
                    )
    return results
