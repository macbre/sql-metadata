"""Extract and strip SQL comments using the sqlglot tokenizer.

sqlglot's tokenizer skips comments during tokenization, which means
comments live in the *gaps* between consecutive token positions.  This
module exploits that property: it tokenizes the SQL, then scans each gap
for comment delimiters (``--``, ``/* */``, ``#``).

Two public entry points exist:

* :func:`extract_comments` — returns the raw comment texts (delimiters
  included) for inspection or logging.
* :func:`strip_comments` — returns the SQL with all comments removed and
  whitespace normalised, used by :class:`Parser` for the ``without_comments``
  property.

A third, internal variant :func:`strip_comments_for_parsing` is consumed
by :mod:`_ast` before handing SQL to ``sqlglot.parse()``; it always uses
the MySQL tokenizer so that ``#``-style comments are reliably stripped.
"""

import re
from typing import List

from sqlglot.tokens import Tokenizer


def _choose_tokenizer(sql: str) -> Tokenizer:
    """Select the appropriate sqlglot tokenizer for *sql*.

    The default sqlglot tokenizer does **not** treat ``#`` as a comment
    delimiter, but MySQL does.  When ``#`` appears in the SQL and is used
    as a comment (not as a variable/template prefix), we switch to the
    MySQL tokenizer so that ``#``-style comments are properly skipped.

    :param sql: Raw SQL string to inspect.
    :type sql: str
    :returns: An instantiated tokenizer (MySQL or default).
    :rtype: sqlglot.tokens.Tokenizer
    """
    if "#" in sql and not _has_hash_variables(sql):
        from sqlglot.dialects.mysql import MySQL

        return MySQL.Tokenizer()
    return Tokenizer()


def _has_hash_variables(sql: str) -> bool:
    """Determine whether ``#`` characters in *sql* are variable references.

    MSSQL uses ``#table`` for temporary tables and some template engines
    use ``#VAR#`` placeholders.  This function distinguishes those from
    MySQL-style ``# comment`` lines so that :func:`_choose_tokenizer`
    picks the right dialect.

    Heuristics (checked via regex):

    * ``#WORD#`` — bracketed template variable.
    * ``= #WORD`` or ``(#WORD`` — assignment / parameter context.

    :param sql: Raw SQL string.
    :type sql: str
    :returns: ``True`` if at least one ``#`` looks like a variable prefix.
    :rtype: bool
    """
    # #WORD# template variable (e.g. #VAR#)
    if re.search(r"#\w+#", sql):
        return True
    # = #WORD or (#WORD with optional whitespace before #
    if re.search(r"[=(]\s*#\w", sql):
        return True
    return False


def extract_comments(sql: str) -> List[str]:
    """Return all comments found in *sql*, with delimiters preserved.

    Tokenizes the SQL, then scans every gap between consecutive token
    positions for comment delimiters.  Returned strings include the
    opening delimiter (``--``, ``/*``, ``#``) and, for block comments,
    the closing ``*/``.

    Called by :attr:`Parser.comments`.

    :param sql: Raw SQL string.
    :type sql: str
    :returns: List of comment strings in source order.
    :rtype: List[str]
    """
    if not sql:
        return []
    try:
        tokens = list(_choose_tokenizer(sql).tokenize(sql))
    except Exception:
        return []
    comments: list[str] = []
    prev_end = -1
    for tok in tokens:
        _scan_gap(sql, prev_end + 1, tok.start, comments)
        prev_end = tok.end
    _scan_gap(sql, prev_end + 1, len(sql), comments)
    return comments


#: Matches all three SQL comment styles in a single pass:
#: ``/* ... */`` (block, possibly unterminated), ``-- ...``, and ``# ...``.
_COMMENT_RE = re.compile(r"/\*.*?\*/|/\*.*$|--[^\n]*\n?|#[^\n]*\n?", re.DOTALL)


def _scan_gap(sql: str, start: int, end: int, out: list) -> None:
    """Scan a slice of *sql* for comment delimiters and append matches.

    :param sql: The full SQL string (not just the gap).
    :param start: Start index of the gap to scan.
    :param end: End index (exclusive) of the gap.
    :param out: Mutable list to which discovered comment strings are appended.
    """
    out.extend(_COMMENT_RE.findall(sql[start:end]))


def _reconstruct_from_tokens(sql: str, tokens: list) -> str:
    """Rebuild SQL from token spans, collapsing gaps to single spaces."""
    if not tokens:
        return ""
    parts = [sql[tokens[0].start : tokens[0].end + 1]]
    for i in range(1, len(tokens)):
        if tokens[i].start > tokens[i - 1].end + 1:
            parts.append(" ")
        parts.append(sql[tokens[i].start : tokens[i].end + 1])
    return "".join(parts).strip()


def strip_comments_for_parsing(sql: str) -> str:
    """Strip **all** comments — including ``#`` lines — for sqlglot parsing.

    Unlike :func:`strip_comments`, this always uses the MySQL tokenizer
    (which treats ``#`` as a comment delimiter) so that hash-style
    comments are removed before ``sqlglot.parse()`` sees the SQL.  The
    only exceptions are ``CREATE FUNCTION`` bodies (which may contain
    ``#`` in procedural code) and MSSQL ``#temp`` table references.

    Called exclusively by :meth:`ASTParser._parse` in ``_ast.py``.

    :param sql: Raw SQL string.
    :type sql: str
    :returns: SQL with all comments removed and whitespace collapsed.
    :rtype: str
    """
    if not sql:
        return sql or ""
    # Skip MySQL tokenizer when # is used as variable (not comment)
    upper = sql.strip().upper()
    if upper.startswith("CREATE FUNCTION") or _has_hash_variables(sql):
        tokenizer = Tokenizer()
    else:
        from sqlglot.dialects.mysql import MySQL

        tokenizer = MySQL.Tokenizer()
    try:
        tokens = list(tokenizer.tokenize(sql))
    except Exception:
        return sql.strip()
    return _reconstruct_from_tokens(sql, tokens)


def strip_comments(sql: str) -> str:
    """Remove comments and normalise whitespace, preserving ``#VAR`` references.

    Reconstructs the SQL from its token spans, inserting a single space
    wherever a gap (comment or extra whitespace) existed between two
    tokens.  Uses :func:`_choose_tokenizer` so that ``#VAR`` template
    variables in MSSQL queries are kept intact.

    Called by :attr:`Parser.without_comments` and
    :attr:`Generalizator.without_comments`.

    :param sql: Raw SQL string.
    :type sql: str
    :returns: SQL with comments removed and whitespace normalised.
    :rtype: str
    """
    if not sql:
        return sql or ""
    try:
        tokens = list(_choose_tokenizer(sql).tokenize(sql))
    except Exception:
        return sql.strip()
    return _reconstruct_from_tokens(sql, tokens)
