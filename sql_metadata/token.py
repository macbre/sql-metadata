"""
SQL token module — thin wrapper around sqlglot tokens in a linked list.
"""

from typing import List, Optional

from sqlglot.tokens import TokenType

from sql_metadata._comments import _choose_tokenizer, _scan_gap
from sql_metadata.keywords_lists import RELEVANT_KEYWORDS

_KEYWORD_TYPES = frozenset({
    TokenType.SELECT, TokenType.FROM, TokenType.WHERE,
    TokenType.JOIN, TokenType.INNER, TokenType.OUTER,
    TokenType.LEFT, TokenType.RIGHT, TokenType.CROSS,
    TokenType.FULL, TokenType.NATURAL,
    TokenType.ON, TokenType.AND, TokenType.OR, TokenType.NOT,
    TokenType.IN, TokenType.IS, TokenType.ALIAS,
    TokenType.ORDER_BY, TokenType.GROUP_BY, TokenType.HAVING,
    TokenType.LIMIT, TokenType.OFFSET,
    TokenType.UNION, TokenType.ALL,
    TokenType.INSERT, TokenType.INTO, TokenType.VALUES,
    TokenType.UPDATE, TokenType.SET, TokenType.DELETE,
    TokenType.CREATE, TokenType.TABLE, TokenType.ALTER, TokenType.DROP,
    TokenType.EXISTS, TokenType.INDEX, TokenType.DISTINCT,
    TokenType.BETWEEN, TokenType.LIKE,
    TokenType.CASE, TokenType.WHEN, TokenType.THEN, TokenType.ELSE, TokenType.END,
    TokenType.NULL, TokenType.TRUE, TokenType.FALSE,
    TokenType.WITH, TokenType.REPLACE, TokenType.USING,
    TokenType.ASC, TokenType.DESC,
    TokenType.WINDOW, TokenType.OVER, TokenType.PARTITION_BY,
    TokenType.RETURNING, TokenType.UNIQUE, TokenType.TRUNCATE, TokenType.FORCE,
})


class SQLToken:
    """Token in a doubly-linked list, wrapping a sqlglot token or a comment."""

    __slots__ = (
        "value", "token_type", "position",
        "next_token", "previous_token", "last_keyword",
    )

    def __init__(
        self,
        value: str = "",
        token_type: Optional[TokenType] = None,
        position: int = -1,
        last_keyword: Optional[str] = None,
    ):
        self.value = value
        self.token_type = token_type
        self.position = position
        self.last_keyword = last_keyword
        self.next_token: Optional["SQLToken"] = None
        self.previous_token: Optional["SQLToken"] = None

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:  # pragma: no cover
        return f"SQLToken({self.value!r}, {self.token_type})"

    def __bool__(self) -> bool:
        return self.value != ""

    # ---- derived properties ----

    @property
    def normalized(self) -> str:
        return self.value.translate(str.maketrans("", "", " \n\t\r")).upper()

    @property
    def is_keyword(self) -> bool:
        return self.token_type in _KEYWORD_TYPES

    @property
    def is_name(self) -> bool:
        return self.token_type == TokenType.VAR

    @property
    def is_wildcard(self) -> bool:
        return self.token_type == TokenType.STAR

    @property
    def is_comment(self) -> bool:
        return self.token_type is None and self.value != ""

    @property
    def is_dot(self) -> bool:
        return self.token_type == TokenType.DOT

    @property
    def is_punctuation(self) -> bool:
        return self.token_type in (
            TokenType.COMMA, TokenType.SEMICOLON, TokenType.COLON,
        )

    @property
    def is_as_keyword(self) -> bool:
        return self.token_type == TokenType.ALIAS

    @property
    def is_left_parenthesis(self) -> bool:
        return self.token_type == TokenType.L_PAREN

    @property
    def is_right_parenthesis(self) -> bool:
        return self.token_type == TokenType.R_PAREN

    @property
    def is_integer(self) -> bool:
        return self.token_type == TokenType.NUMBER and "." not in self.value

    @property
    def is_float(self) -> bool:
        return self.token_type == TokenType.NUMBER and "." in self.value

    @property
    def next_token_not_comment(self) -> Optional["SQLToken"]:
        tok = self.next_token
        while tok and tok.is_comment:
            tok = tok.next_token
        return tok

    @property
    def previous_token_not_comment(self) -> Optional["SQLToken"]:
        tok = self.previous_token
        while tok and tok.is_comment:
            tok = tok.previous_token
        return tok


# Singleton for empty/missing token references
EmptyToken = SQLToken()


# ---------------------------------------------------------------------------
# Tokenizer — builds linked list from SQL string
# ---------------------------------------------------------------------------

def tokenize(sql: str) -> List[SQLToken]:  # noqa: C901
    """Tokenize SQL into a linked list of SQLToken objects."""
    if not sql or not sql.strip():
        return []

    try:
        sg_tokens = list(_choose_tokenizer(sql).tokenize(sql))
    except Exception:
        return []

    # Collect tokens and comments in position order
    items: list = []
    prev_end = -1
    for sg_tok in sg_tokens:
        comments: list = []
        _scan_gap(sql, prev_end + 1, sg_tok.start, comments)
        for text in comments:
            pos = sql.find(text, prev_end + 1)
            if pos >= 0:
                items.append((pos, None, text))  # comment: token_type=None
        val = sg_tok.text.strip("`").strip('"')
        items.append((sg_tok.start, sg_tok.token_type, val))
        prev_end = sg_tok.end

    # Trailing comments
    comments = []
    _scan_gap(sql, prev_end + 1, len(sql), comments)
    for text in comments:
        pos = sql.find(text, prev_end + 1)
        if pos >= 0:
            items.append((pos, None, text))
    items.sort(key=lambda x: x[0])

    # Build linked list
    tokens: List[SQLToken] = []
    last_kw: Optional[str] = None
    for _pos, tt, text in items:
        tok = SQLToken(
            value=text, token_type=tt,
            position=len(tokens), last_keyword=last_kw,
        )
        if tt in _KEYWORD_TYPES:
            norm = tok.normalized
            if norm in RELEVANT_KEYWORDS:
                last_kw = norm
        tokens.append(tok)

    for i in range(1, len(tokens)):
        tokens[i].previous_token = tokens[i - 1]
        tokens[i - 1].next_token = tokens[i]

    return tokens
