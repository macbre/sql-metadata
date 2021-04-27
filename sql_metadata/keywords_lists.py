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
    "YEAR",
    "MONTH",
    "YEARWEEK",
    "DAY",
    "AVG",
    "SUM",
    "IFNULL",
    "DATEDIFF",
    "DIV",
    "MID",
    "WEEKDAY",
    "NOW",
    "LAST_DAY",
    "DATE_ADD",
]
# these keywords are followed by columns reference
KEYWORDS_BEFORE_COLUMNS = ["SELECT", "WHERE", "ORDERBY", "ON", "SET"]

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

# next statement beginning after with statement
WITH_ENDING_KEYWORDS = ["UPDATE", "SELECT", "DELETE", "REPLACE"]

# subquery preceding keywords
SUBQUERY_PRECEDING_KEYWORDS = [
    "FROM",
    "JOIN",
    "INNERJOIN",
    "FULLJOIN",
    "FULLOUTERJOIN",
    "LEFTJOIN",
    "RIGHTJOIN",
    "LEFTOUTERJOIN",
    "RIGHTOUTERJOIN",
]
