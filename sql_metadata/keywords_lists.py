"""
Module provide lists of sql keywords that should trigger or skip
checks for tables an columns
"""

# these keywords are followed by columns reference
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

SUPPORTED_QUERY_TYPES = {
    "INSERT": "Insert",
    "REPLACE": "Replace",
    "UPDATE": "Update",
    "SELECT": "Select",
    "WITH": "Select",
    "CREATETABLE": "Create",
    "ALTERTABLE": "Alter",
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
