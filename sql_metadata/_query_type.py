"""Extract the query type from a sqlglot AST root node.

The :class:`QueryTypeExtractor` class maps the top-level AST node to a
:class:`QueryType` enum value, handling parenthesised wrappers, set
operations, and opaque ``Command`` nodes.
"""

import logging
from typing import Optional

from sqlglot import exp

from sql_metadata.keywords_lists import QueryType

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


class QueryTypeExtractor:
    """Determine the query type from a sqlglot AST root node.

    :param ast: Root AST node (may be ``None``).
    :param raw_query: Original SQL string (for error messages).
    """

    def __init__(
        self,
        ast: Optional[exp.Expression],
        raw_query: str,
    ):
        self._ast = ast
        self._raw_query = raw_query

    def extract(self) -> QueryType:
        """Determine the :class:`QueryType` for the parsed SQL.

        :returns: The detected query type.
        :raises ValueError: If the query is empty, malformed, or
            unsupported.
        """
        if self._ast is None:
            self._raise_for_none_ast()

        root = self._unwrap_parens(self._ast)
        node_type = type(root)

        if node_type is exp.With:
            raise ValueError("This query is wrong")

        simple = _SIMPLE_TYPE_MAP.get(node_type)
        if simple is not None:
            return simple

        if node_type is exp.Command:
            result = self._resolve_command_type(root)
            if result is not None:
                return result

        shorten_query = " ".join(self._raw_query.split(" ")[:3])
        logger.error("Not supported query type: %s", shorten_query)
        raise ValueError("Not supported query type!")

    @staticmethod
    def _unwrap_parens(ast: exp.Expression) -> exp.Expression:
        """Remove Paren and Subquery wrappers to reach the real statement."""
        if isinstance(ast, (exp.Paren, exp.Subquery)):
            return QueryTypeExtractor._unwrap_parens(ast.this)
        return ast

    @staticmethod
    def _resolve_command_type(root: exp.Expression) -> Optional[QueryType]:
        """Determine query type for an opaque Command node."""
        expression_text = str(root.this).upper() if root.this else ""
        if expression_text == "ALTER":
            return QueryType.ALTER
        if expression_text == "CREATE":
            return QueryType.CREATE
        return None

    def _raise_for_none_ast(self) -> None:
        """Raise an appropriate error when the AST is None."""
        from sql_metadata._comments import strip_comments

        stripped = (
            strip_comments(self._raw_query) if self._raw_query else ""
        )
        if stripped.strip():
            raise ValueError("This query is wrong")
        raise ValueError("Empty queries are not supported!")

