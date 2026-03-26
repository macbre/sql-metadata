"""Extract the query type from a sqlglot AST root node.

Maps the top-level ``sqlglot.exp.Expression`` subclass to a
:class:`QueryType` enum value.  Handles edge cases like parenthesised
queries (``exp.Paren`` / ``exp.Subquery`` wrappers), set operations
(``UNION`` / ``INTERSECT`` / ``EXCEPT`` → ``SELECT``), and opaque
``exp.Command`` nodes produced by sqlglot for statements it does not
fully parse (e.g. ``ALTER TABLE APPEND``, ``CREATE FUNCTION``).
"""

import logging

from sqlglot import exp

from sql_metadata.keywords_lists import QueryType

#: Module-level logger.  An error is logged (and ``ValueError`` raised)
#: when the query type is not recognised.
logger = logging.getLogger(__name__)


#: Direct AST type → QueryType mapping for simple cases.
_SIMPLE_TYPE_MAP = {
    exp.Select: QueryType.SELECT,
    exp.Union: QueryType.SELECT,
    exp.Intersect: QueryType.SELECT,
    exp.Except: QueryType.SELECT,
    exp.Insert: QueryType.INSERT,
    exp.Update: QueryType.UPDATE,
    exp.Delete: QueryType.DELETE,
    exp.Create: QueryType.CREATE,
    exp.Alter: QueryType.ALTER,
    exp.Drop: QueryType.DROP,
    exp.TruncateTable: QueryType.TRUNCATE,
    exp.Merge: QueryType.MERGE,
}


def _unwrap_parens(ast: exp.Expression) -> exp.Expression:
    """Remove ``Paren`` and ``Subquery`` wrappers to reach the real statement.

    :param ast: The root AST node, possibly wrapped.
    :type ast: exp.Expression
    :returns: The innermost non-wrapper node.
    :rtype: exp.Expression
    """
    root = ast
    while isinstance(root, (exp.Paren, exp.Subquery)):
        root = root.this
    return root


def _resolve_command_type(root: exp.Expression) -> QueryType:
    """Determine the query type for an opaque ``Command`` node.

    sqlglot produces ``exp.Command`` for statements it does not fully
    parse (e.g. ``ALTER TABLE APPEND``, ``CREATE FUNCTION``).  This
    helper inspects the command text to map it to a known type.

    :param root: A ``Command`` AST node.
    :type root: exp.Expression
    :returns: The detected query type, or ``None`` if unrecognised.
    :rtype: Optional[QueryType]
    """
    expression_text = str(root.this).upper() if root.this else ""
    if expression_text == "ALTER":
        return QueryType.ALTER
    if expression_text == "CREATE":
        return QueryType.CREATE
    return None


def _raise_for_none_ast(raw_query: str) -> None:
    """Raise an appropriate error when the AST is ``None``.

    Distinguishes between empty input (comment-only or blank) and
    genuinely malformed SQL by stripping comments first.

    :param raw_query: The original SQL string.
    :type raw_query: str
    :raises ValueError: Always — either "empty" or "wrong".
    """
    from sql_metadata._comments import strip_comments

    stripped = strip_comments(raw_query) if raw_query else ""
    if stripped.strip():
        raise ValueError("This query is wrong")
    raise ValueError("Empty queries are not supported!")


def extract_query_type(ast: exp.Expression, raw_query: str) -> QueryType:
    """Determine the :class:`QueryType` for a parsed SQL statement.

    Called by :attr:`Parser.query_type`.  If the AST is ``None`` the
    function distinguishes between empty input (comment-only or blank)
    and genuinely malformed SQL by stripping comments first.

    :param ast: Root AST node returned by :attr:`ASTParser.ast`, or
        ``None`` if parsing produced no tree.
    :type ast: Optional[exp.Expression]
    :param raw_query: The original SQL string, used as a fallback for
        ``Command`` nodes and for error messages.
    :type raw_query: str
    :returns: The detected query type.
    :rtype: QueryType
    :raises ValueError: If the query is empty, malformed, or of an
        unsupported type.
    """
    if ast is None:
        _raise_for_none_ast(raw_query)

    root = _unwrap_parens(ast)
    node_type = type(root)

    if node_type is exp.With:
        raise ValueError("This query is wrong")

    simple = _SIMPLE_TYPE_MAP.get(node_type)
    if simple is not None:
        return simple

    if node_type is exp.Command:
        result = _resolve_command_type(root)
        if result is not None:
            return result

    shorten_query = " ".join(raw_query.split(" ")[:3])
    logger.error("Not supported query type: %s", shorten_query)
    raise ValueError("Not supported query type!")
