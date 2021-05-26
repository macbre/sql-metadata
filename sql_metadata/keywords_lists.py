"""
Module provide lists of sql keywords that should trigger or skip
checks for tables an columns
"""

# these keywords are followed by columns reference
from enum import Enum

KEYWORDS_BEFORE_COLUMNS = {"SELECT", "WHERE", "ORDERBY", "GROUPBY", "ON", "SET"}

# normalized list of table preceding keywords
TABLE_ADJUSTMENT_KEYWORDS = {
    "FROM",
    "JOIN",
    "INNERJOIN",
    "FULLJOIN",
    "FULLOUTERJOIN",
    "LEFTJOIN",
    "RIGHTJOIN",
    "LEFTOUTERJOIN",
    "RIGHTOUTERJOIN",
    "INTO",
    "UPDATE",
    "TABLE",
}

# next statement beginning after with statement
WITH_ENDING_KEYWORDS = {"UPDATE", "SELECT", "DELETE", "REPLACE"}

# subquery preceding keywords
SUBQUERY_PRECEDING_KEYWORDS = {
    "FROM",
    "JOIN",
    "INNERJOIN",
    "FULLJOIN",
    "FULLOUTERJOIN",
    "LEFTJOIN",
    "RIGHTJOIN",
    "LEFTOUTERJOIN",
    "RIGHTOUTERJOIN",
}

# section of a query in which column can exists
# based on last normalized keyword
COLUMNS_SECTIONS = {
    "SELECT": "select",
    "WHERE": "where",
    "ORDERBY": "order_by",
    "ON": "join",
    "INTO": "insert",
    "SET": "update",
    "GROUPBY": "group_by",
}


class QueryType(str, Enum):
    """
    Types of supported queries
    """

    INSERT = "INSERT"
    REPLACE = "REPLACE"
    UPDATE = "UPDATE"
    SELECT = "SELECT"
    CREATE = "CREATE TABLE"
    ALTER = "ALTER TABLE"


# cannot fully replace with enum as with/select has the same key
SUPPORTED_QUERY_TYPES = {
    "INSERT": QueryType.INSERT,
    "REPLACE": QueryType.REPLACE,
    "UPDATE": QueryType.UPDATE,
    "SELECT": QueryType.SELECT,
    "WITH": QueryType.SELECT,
    "CREATETABLE": QueryType.CREATE,
    "ALTERTABLE": QueryType.ALTER,
}

# all the keywords we care for - rest is ignored in assigning
# the last keyword
RELEVANT_KEYWORDS = {
    *KEYWORDS_BEFORE_COLUMNS,
    *TABLE_ADJUSTMENT_KEYWORDS,
    *WITH_ENDING_KEYWORDS,
    *SUBQUERY_PRECEDING_KEYWORDS,
    "LIMIT",
    "OFFSET",
    "USING",
    "RETURNING",
    "VALUES",
    "INDEX",
    "WITH",
}
