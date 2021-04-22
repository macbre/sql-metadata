"""
Module provide lists of sql keywords that should trigger or skip
checks for tables an columns
"""
# these keywords should not change the state of a parser
# and not "reset" previously found SELECT keyword
KEYWORDS_IGNORED = [
    "AS",
    "AND",
    "OR",
    "IN",
    "IS",
    "NULL",
    "NOT",
    "NOT NULL",
    "LIKE",
    "CASE",
    "WHEN",
    "DISTINCT",
    "UNIQUE",
]

# these function should be ignored
# and not "reset" previously found SELECT keyword
FUNCTIONS_IGNORED = [
    "COUNT",
    "MIN",
    "MAX",
    "FROM_UNIXTIME",
    "DATE_FORMAT",
    "CAST",
    "CONVERT",
]
# these keywords are followed by columns reference
KEYWORDS_BEFORE_COLUMNS = ["SELECT", "WHERE", "ORDER BY", "ON"]

# these keywords precede table names
TABLE_SYNTAX_KEYWORDS = [
    # SELECT queries
    "FROM",
    "WHERE",
    "JOIN",
    "INNERJOIN",
    "FULLJOIN",
    "FULLOUTERJOIN",
    "LEFTOUTERJOIN",
    "RIGHTOUTERJOIN",
    "LEFTJOIN",
    "RIGHTJOIN",
    "ON",
    "UNION",
    "UNIONALL",
    # INSERT queries
    "INTO",
    "VALUES",
    # UPDATE queries
    "UPDATE",
    "SET",
    # Hive queries
    "TABLE",  # INSERT TABLE
]

# normalized list of table preceding keywords
TABLE_ADJUSTMENT_KEYWORDS = [
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
]
