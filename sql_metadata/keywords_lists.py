"""Query type enum for classifying SQL statements.

Defines the :class:`QueryType` enum used by :class:`QueryTypeExtractor`
and exported from the ``sql_metadata`` package.
"""

from enum import Enum


class QueryType(str, Enum):
    """Enumeration of SQL statement types recognised by the parser.

    Inherits from :class:`str` so that values are directly comparable to
    plain strings (``parser.query_type == "SELECT"``).  Returned by
    :attr:`Parser.query_type` and by :class:`_query_type.QueryTypeExtractor`.
    """

    INSERT = "INSERT"
    REPLACE = "REPLACE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SELECT = "SELECT"
    CREATE = "CREATE TABLE"
    ALTER = "ALTER TABLE"
    DROP = "DROP TABLE"
    TRUNCATE = "TRUNCATE TABLE"
    MERGE = "MERGE"
