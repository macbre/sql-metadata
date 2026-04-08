"""Extract the query type from a sqlglot AST root node.

The :class:`QueryTypeExtractor` class maps the top-level AST node to a
:class:`QueryType` enum value, handling set operations (``UNION``,
``INTERSECT``, ``EXCEPT``) and opaque ``Command`` nodes that sqlglot
cannot fully parse (e.g. Hive DDL).
"""

import logging
from typing import NoReturn

from sqlglot import exp

from sql_metadata.comments import strip_comments
from sql_metadata.exceptions import InvalidQueryDefinition
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

    Maps the root AST node type to a :class:`QueryType` enum value
    using :data:`_SIMPLE_TYPE_MAP` for common statement types.  Falls
    back to command-text inspection for opaque ``exp.Command`` nodes
    that sqlglot could not fully parse.

    :param ast: Root AST node produced by :class:`DialectParser`, or
        ``None`` when the input was empty or comment-only.
    :param raw_query: Original SQL string, kept for error messages
        when the AST is ``None``.
    """

    def __init__(
        self,
        ast: exp.Expression | None,
        raw_query: str,
    ):
        self._ast = ast
        self._raw_query = raw_query

    def extract(self) -> QueryType:
        """Determine the :class:`QueryType` for the parsed SQL.

        Checks the root node type against :data:`_SIMPLE_TYPE_MAP` first.
        A bare ``exp.With`` node (CTE without a main statement) is rejected
        as invalid SQL.  ``exp.Command`` nodes are forwarded to
        :meth:`_resolve_command_type` for command-text inspection.

        :returns: The detected query type.
        :raises InvalidQueryDefinition: If the AST is ``None`` (empty or
            comment-only input), the query is a bare ``WITH`` clause, or
            the root node type is not recognised.
        """
        if self._ast is None:
            self._raise_for_none_ast()

        root = self._ast
        node_type = type(root)

        if node_type is exp.With:
            raise InvalidQueryDefinition(
                "WITH clause without a main statement is not valid SQL"
            )

        simple = _SIMPLE_TYPE_MAP.get(node_type)
        if simple is not None:
            return simple

        if node_type is exp.Command:
            result = self._resolve_command_type(root)
            if result is not None:
                return result

        shorten_query = " ".join(self._raw_query.split(" ")[:3])
        logger.error("Not supported query type: %s", shorten_query)
        raise InvalidQueryDefinition("Not supported query type!")

    @staticmethod
    def _resolve_command_type(root: exp.Expression) -> QueryType | None:
        """Extract query type from the command text of an opaque node.

        Some dialect-specific DDL (e.g. Hive
        ``CREATE FUNCTION ... USING JAR ... WITH SERDEPROPERTIES``) is
        not supported by any sqlglot dialect and degrades to
        ``exp.Command(this='CREATE', ...)``.  This method reads the
        ``this`` attribute of the command node and maps it back to the
        corresponding :class:`QueryType`, so callers still get
        ``QueryType.CREATE`` instead of an unsupported-type error.

        :param root: An ``exp.Command`` AST node.
        :returns: The resolved :class:`QueryType`, or ``None`` if the
            command text does not match any known type.
        """
        expression_text = str(root.this).upper() if root.this else ""
        if expression_text == "CREATE":
            return QueryType.CREATE
        return None

    def _raise_for_none_ast(self) -> "NoReturn":
        """Raise a descriptive error when the AST is ``None``.

        Distinguishes between truly empty input (blank or comment-only
        SQL) and SQL that has content but could not be parsed by
        sqlglot.  In the first case a "not supported" message is raised;
        in the second a "could not parse" message points the caller
        toward a syntax problem.

        :raises InvalidQueryDefinition: Always — this method never
            returns normally.
        """

        stripped = strip_comments(self._raw_query) if self._raw_query else ""
        if stripped.strip():
            raise InvalidQueryDefinition(
                "Could not parse the query — the SQL syntax appears to be invalid"
            )
        raise InvalidQueryDefinition("Empty queries are not supported!")
