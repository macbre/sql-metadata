"""Extract original SQL text for CTE and subquery bodies.

Uses the sqlglot tokenizer for structure discovery and a pre-computed
parenthesis map for O(1) body extraction.  The key design goal is to
**preserve original casing and quoting** — sqlglot's ``exp.sql()`` method
normalises casing, so instead we reconstruct the body from the raw SQL
string using token start/end positions.

Two public entry points:

* :func:`extract_cte_bodies` — called by :attr:`Parser.with_queries`.
* :func:`extract_subquery_bodies` — called by :attr:`Parser.subqueries`.
"""

from typing import Dict, List, Optional, Tuple

from sqlglot import exp
from sqlglot.tokens import TokenType

from sql_metadata._comments import _choose_tokenizer

#: Shorthand token type aliases used throughout this module to keep the
#: body-extraction logic concise.
_VAR = TokenType.VAR
_IDENT = TokenType.IDENTIFIER
_LPAREN = TokenType.L_PAREN
_RPAREN = TokenType.R_PAREN
_ALIAS = TokenType.ALIAS


def _choose_body_tokenizer(sql: str):
    """Select a tokenizer for body extraction.

    Uses the MySQL tokenizer when backticks are present (so that
    backtick-quoted identifiers are properly tokenized), otherwise
    delegates to :func:`_choose_tokenizer` from ``_comments.py``.

    :param sql: Raw SQL string.
    :type sql: str
    :returns: An instantiated sqlglot tokenizer.
    :rtype: sqlglot.tokens.Tokenizer
    """
    if "`" in sql:
        from sqlglot.dialects.mysql import MySQL

        return MySQL.Tokenizer()
    return _choose_tokenizer(sql)


# ---------------------------------------------------------------------------
# Token reconstruction (preserves original casing and quoting)
# ---------------------------------------------------------------------------

#: Token types where a left parenthesis does **not** need a preceding
#: space (i.e. it's a keyword followed by ``(``).  All other token types
#: are assumed to be function names where the ``(`` attaches directly.
_KW_BEFORE_PAREN = {
    TokenType.WHERE,
    TokenType.IN,
    TokenType.ON,
    TokenType.AND,
    TokenType.OR,
    TokenType.NOT,
    TokenType.HAVING,
    TokenType.FROM,
    TokenType.JOIN,
    TokenType.VALUES,
    TokenType.SET,
    TokenType.BETWEEN,
    TokenType.WHEN,
    TokenType.THEN,
    TokenType.ELSE,
    TokenType.USING,
    TokenType.INTO,
    TokenType.TABLE,
    TokenType.OVER,
    TokenType.PARTITION_BY,
    TokenType.ORDER_BY,
    TokenType.GROUP_BY,
    TokenType.WINDOW,
    TokenType.EXISTS,
    TokenType.SELECT,
    TokenType.INNER,
    TokenType.OUTER,
    TokenType.LEFT,
    TokenType.RIGHT,
    TokenType.CROSS,
    TokenType.FULL,
    TokenType.NATURAL,
    TokenType.INSERT,
    TokenType.UPDATE,
    TokenType.DELETE,
    TokenType.WITH,
    TokenType.RETURNING,
    TokenType.UNION,
    TokenType.LIMIT,
    TokenType.OFFSET,
    TokenType.DISTINCT,
}


def _no_space(prev, curr) -> bool:
    """Decide whether *prev* and *curr* tokens should have no space between them.

    Encodes the spacing rules needed to reconstruct SQL from tokens:
    no space around dots, before commas/right-parens, after left-parens,
    and before a left-paren that follows a non-keyword (function call).

    :param prev: The preceding token.
    :type prev: sqlglot token
    :param curr: The current token.
    :type curr: sqlglot token
    :returns: ``True`` if no space should be inserted between them.
    :rtype: bool
    """
    if prev.token_type == TokenType.DOT or curr.token_type == TokenType.DOT:
        return True
    if curr.token_type in (TokenType.COMMA, TokenType.SEMICOLON, _RPAREN):
        return True
    if prev.token_type == _LPAREN:
        return True
    if curr.token_type == _LPAREN:
        if prev.token_type in _KW_BEFORE_PAREN or prev.token_type in (
            TokenType.STAR,
            TokenType.COMMA,
        ):
            return False
        return True
    return False


def _reconstruct(tokens, sql: str) -> str:
    """Reconstruct SQL from a slice of tokens, preserving original casing.

    For each token the original text is extracted from *sql* using the
    token's ``start`` and ``end`` positions.  Spacing between tokens is
    determined by :func:`_no_space`.

    :param tokens: Slice of sqlglot tokens to reconstruct.
    :type tokens: list
    :param sql: The full original SQL string (used for positional slicing).
    :type sql: str
    :returns: Reconstructed SQL fragment.
    :rtype: str
    """
    if not tokens:
        return ""

    def _text(tok):
        """Extract the original text for a single token.

        :param tok: A sqlglot token.
        :returns: Original SQL text for this token position.
        :rtype: str
        """
        if tok.token_type == _IDENT:
            return tok.text  # strip backticks
        return sql[tok.start : tok.end + 1]

    parts = [_text(tokens[0])]
    for i in range(1, len(tokens)):
        if not _no_space(tokens[i - 1], tokens[i]):
            parts.append(" ")
        parts.append(_text(tokens[i]))
    return "".join(parts)


# ---------------------------------------------------------------------------
# Paren map: pre-compute matching parentheses in a single pass
# ---------------------------------------------------------------------------


def _build_paren_maps(
    tokens,
) -> Tuple[Dict[int, int], Dict[int, int]]:
    """Pre-compute matching parenthesis indices in O(n) time.

    Returns two dictionaries: one mapping each left-paren index to its
    matching right-paren, and the reverse.  This allows O(1) lookups
    during body extraction instead of scanning for matching parens each
    time.

    :param tokens: List of sqlglot tokens.
    :type tokens: list
    :returns: A 2-tuple of ``(l_to_r, r_to_l)`` index mappings.
    :rtype: Tuple[Dict[int, int], Dict[int, int]]
    """
    stack: list = []
    l_to_r: Dict[int, int] = {}
    r_to_l: Dict[int, int] = {}
    for i, tok in enumerate(tokens):
        if tok.token_type == _LPAREN:
            stack.append(i)
        elif tok.token_type == _RPAREN and stack:
            o = stack.pop()
            l_to_r[o] = i
            r_to_l[i] = o
    return l_to_r, r_to_l


# ---------------------------------------------------------------------------
# Body extraction
# ---------------------------------------------------------------------------


def _extract_single_cte_body(
    tokens: list, idx: int, l_to_r: Dict[int, int], raw_sql: str
) -> tuple:
    """Extract the body of a single CTE starting at the name token.

    Skips optional column definitions (using the paren map), expects
    an ``AS`` keyword, then extracts tokens between the body's
    parentheses.

    :param tokens: Full token list.
    :type tokens: list
    :param idx: Index of the CTE name token.
    :type idx: int
    :param l_to_r: Left-paren → right-paren index mapping.
    :type l_to_r: Dict[int, int]
    :param raw_sql: Original SQL string for reconstruction.
    :type raw_sql: str
    :returns: ``(body_sql, next_index)`` or ``(None, idx + 1)`` on failure.
    :rtype: tuple
    """
    j = idx + 1
    # Skip optional column definitions
    if j < len(tokens) and tokens[j].token_type == _LPAREN:
        j = l_to_r.get(j, j) + 1
    # Expect AS keyword
    if not (
        j < len(tokens)
        and tokens[j].token_type == _ALIAS
        and tokens[j].text.upper() == "AS"
    ):
        return None, idx + 1
    j += 1
    # Extract body between parens
    if j < len(tokens) and tokens[j].token_type == _LPAREN:
        close = l_to_r.get(j)
        if close is not None:
            body_tokens = tokens[j + 1 : close]
            if body_tokens:
                return _reconstruct(body_tokens, raw_sql), close + 1
    return None, idx + 1


def extract_cte_bodies(
    ast: Optional[exp.Expression],
    raw_sql: str,
    cte_names: List[str],
    cte_name_map: Optional[dict] = None,
) -> Dict[str, str]:
    """Extract CTE body SQL for each name in *cte_names*.

    Scans the token stream for each CTE name, skips optional column
    definitions (using the paren map), expects an ``AS`` keyword, and
    then extracts the tokens between the body's opening and closing
    parentheses.  The body is reconstructed via :func:`_reconstruct`
    to preserve original casing and quoting.

    Called by :attr:`Parser.with_queries`.

    :param ast: Root AST node (used only for the guard check).
    :type ast: Optional[exp.Expression]
    :param raw_sql: Original SQL string.
    :type raw_sql: str
    :param cte_names: Ordered list of CTE names to extract bodies for.
    :type cte_names: List[str]
    :param cte_name_map: Placeholder → original qualified name mapping.
    :type cte_name_map: Optional[dict]
    :returns: Mapping of ``{cte_name: body_sql}``.
    :rtype: Dict[str, str]
    """
    if not ast or not raw_sql or not cte_names:
        return {}
    try:
        tokens = list(_choose_body_tokenizer(raw_sql).tokenize(raw_sql))
    except Exception:
        return {}

    l_to_r, _ = _build_paren_maps(tokens)
    token_name_map = {n.split(".")[-1].upper(): n for n in cte_names}
    results: Dict[str, str] = {}

    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.token_type in (_VAR, _IDENT) and tok.text.upper() in token_name_map:
            cte_name = token_name_map[tok.text.upper()]
            body, next_i = _extract_single_cte_body(tokens, i, l_to_r, raw_sql)
            if body is not None:
                results[cte_name] = body
            i = next_i
        else:
            i += 1
    return results


def _extract_single_subquery_body(
    tokens: list, idx: int, r_to_l: Dict[int, int], raw_sql: str
) -> str:
    """Extract the body of a single subquery by walking backward from its alias.

    Skips an optional ``AS`` keyword, then uses the paren map to find
    the matching opening parenthesis and reconstructs the body tokens.

    :param tokens: Full token list.
    :type tokens: list
    :param idx: Index of the subquery alias name token.
    :type idx: int
    :param r_to_l: Right-paren → left-paren index mapping.
    :type r_to_l: Dict[int, int]
    :param raw_sql: Original SQL string for reconstruction.
    :type raw_sql: str
    :returns: Body SQL string, or ``None`` if extraction failed.
    :rtype: Optional[str]
    """
    j = idx - 1
    if j >= 0 and tokens[j].token_type == _ALIAS:
        j -= 1
    if j >= 0 and tokens[j].token_type == _RPAREN:
        open_idx = r_to_l.get(j)
        if open_idx is not None:
            body_tokens = tokens[open_idx + 1 : j]
            if body_tokens:
                return _reconstruct(body_tokens, raw_sql)
    return None


def extract_subquery_bodies(
    ast: Optional[exp.Expression],
    raw_sql: str,
    subquery_names: List[str],
) -> Dict[str, str]:
    """Extract subquery body SQL for each name in *subquery_names*.

    Scans the token stream for each subquery alias name, walks backward
    past an optional ``AS`` keyword, then uses the paren map to jump to
    the matching left parenthesis and extracts the body tokens between
    them.

    Called by :attr:`Parser.subqueries`.

    :param ast: Root AST node (used only for the guard check).
    :type ast: Optional[exp.Expression]
    :param raw_sql: Original SQL string.
    :type raw_sql: str
    :param subquery_names: List of subquery alias names to extract.
    :type subquery_names: List[str]
    :returns: Mapping of ``{subquery_name: body_sql}``.
    :rtype: Dict[str, str]
    """
    if not ast or not raw_sql or not subquery_names:
        return {}
    try:
        tokens = list(_choose_body_tokenizer(raw_sql).tokenize(raw_sql))
    except Exception:
        return {}

    _, r_to_l = _build_paren_maps(tokens)
    names_upper = {n.upper(): n for n in subquery_names}
    results: Dict[str, str] = {}

    for i, tok in enumerate(tokens):
        if tok.token_type in (_VAR, _IDENT) and tok.text.upper() in names_upper:
            original_name = names_upper[tok.text.upper()]
            body = _extract_single_subquery_body(tokens, i, r_to_l, raw_sql)
            if body is not None:
                results[original_name] = body
    return results
