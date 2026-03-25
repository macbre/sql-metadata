"""
Module to extract query type from sqlglot AST.
"""

import logging

from sqlglot import exp

from sql_metadata.keywords_lists import QueryType


logger = logging.getLogger(__name__)


def extract_query_type(ast: exp.Expression, raw_query: str) -> QueryType:
    """
    Map AST root node type to QueryType enum.
    """
    if ast is None:
        # Check if the raw query has content (malformed vs empty)
        # Strip comments first — a comment-only query is empty
        from sql_metadata._comments import strip_comments

        stripped = strip_comments(raw_query) if raw_query else ""
        if stripped.strip():
            raise ValueError("This query is wrong")
        raise ValueError("Empty queries are not supported!")

    root = ast

    # Unwrap parenthesized expressions
    while isinstance(root, (exp.Paren, exp.Subquery)):
        root = root.this

    node_type = type(root)

    if node_type is exp.Select:
        return QueryType.SELECT

    if node_type in (exp.Union, exp.Intersect, exp.Except):
        return QueryType.SELECT

    # WITH without a proper SELECT body - malformed
    if node_type is exp.With:
        raise ValueError("This query is wrong")

    if node_type is exp.Insert:
        return QueryType.INSERT

    if node_type is exp.Update:
        return QueryType.UPDATE

    if node_type is exp.Delete:
        return QueryType.DELETE

    if node_type is exp.Create:
        kind = (root.args.get("kind") or "").upper()
        if kind in ("TABLE", "TEMPORARY", "FUNCTION"):
            return QueryType.CREATE
        # Default CREATE → CREATE TABLE
        return QueryType.CREATE

    if node_type is exp.Alter:
        return QueryType.ALTER

    if node_type is exp.Drop:
        return QueryType.DROP

    if node_type is exp.TruncateTable:
        return QueryType.TRUNCATE

    # Commands not fully parsed by sqlglot
    if node_type is exp.Command:
        expression_text = str(root.this).upper() if root.this else ""
        if expression_text == "ALTER":
            return QueryType.ALTER
        if expression_text == "CREATE":
            # CREATE FUNCTION ... parsed as Command
            return QueryType.CREATE

    shorten_query = " ".join(raw_query.split(" ")[:3])
    logger.error("Not supported query type: %s", shorten_query)
    raise ValueError("Not supported query type!")
