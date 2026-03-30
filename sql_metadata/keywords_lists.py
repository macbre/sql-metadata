"""SQL keyword sets and enums used to classify tokens and query types.

Defines the canonical sets of normalised SQL keywords that the token-based
parser (``token.py``) and the AST-based extractors use to decide when a
token is relevant (e.g. precedes a column or table reference) and to map
query prefixes to :class:`QueryType` values.  Keyword values are stored
**without spaces** (``INNERJOIN``, ``ORDERBY``) because the tokeniser
strips whitespace before comparison.
"""

from enum import Enum

#: Normalised keywords after which the next token(s) are column references.
#: Used by the token-linked-list walker and by ``COLUMNS_SECTIONS`` to
#: decide which ``columns_dict`` section a column belongs to.
KEYWORDS_BEFORE_COLUMNS = {
    "SELECT",
    "WHERE",
    "HAVING",
    "ORDERBY",
    "GROUPBY",
    "ON",
    "SET",
    "USING",
}

#: Normalised keywords after which the next token is a **table** name.
#: Includes all JOIN variants (whitespace-stripped) as well as INTO,
#: UPDATE, TABLE, and the DDL guard ``IFNOTEXISTS``.
TABLE_ADJUSTMENT_KEYWORDS = {
    "FROM",
    "JOIN",
    "CROSSJOIN",
    "INNERJOIN",
    "FULLJOIN",
    "FULLOUTERJOIN",
    "LEFTJOIN",
    "RIGHTJOIN",
    "LEFTOUTERJOIN",
    "RIGHTOUTERJOIN",
    "NATURALJOIN",
    "INTO",
    "UPDATE",
    "TABLE",
    "IFNOTEXISTS",
}

#: Keywords that signal the end of a ``WITH`` (CTE) block and the start
#: of the main statement body.  Used by the legacy token-based WITH parser
#: and referenced in ``_ast.py`` for malformed-query detection.
WITH_ENDING_KEYWORDS = {"UPDATE", "SELECT", "DELETE", "REPLACE", "INSERT"}

#: Keywords that can appear immediately before a parenthesised subquery
#: in a FROM/JOIN position.  A subset of ``TABLE_ADJUSTMENT_KEYWORDS``
#: excluding DML-only entries (INTO, UPDATE, TABLE).
SUBQUERY_PRECEDING_KEYWORDS = {
    "FROM",
    "JOIN",
    "CROSSJOIN",
    "INNERJOIN",
    "FULLJOIN",
    "FULLOUTERJOIN",
    "LEFTJOIN",
    "RIGHTJOIN",
    "LEFTOUTERJOIN",
    "RIGHTOUTERJOIN",
    "NATURALJOIN",
}

#: Maps a normalised keyword to the ``columns_dict`` section name that
#: columns following it belong to.  For example, columns after ``SELECT``
#: go into the ``"select"`` section, columns after ``ON``/``USING`` go
#: into ``"join"``.
COLUMNS_SECTIONS = {
    "SELECT": "select",
    "WHERE": "where",
    "HAVING": "having",
    "ORDERBY": "order_by",
    "ON": "join",
    "USING": "join",
    "INTO": "insert",
    "SET": "update",
    "GROUPBY": "group_by",
    "INNERJOIN": "inner_join",
}


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


class TokenType(str, Enum):
    """Semantic classification assigned to an :class:`SQLToken` during parsing.

    These types are used by the legacy token-based extraction pipeline to
    label each token after the keyword-driven classification pass.  In the
    v3 sqlglot-based pipeline they are still referenced for backward
    compatibility in test assertions and token introspection.
    """

    COLUMN = "COLUMN"
    TABLE = "TABLE"
    COLUMN_ALIAS = "COLUMN_ALIAS"
    TABLE_ALIAS = "TABLE_ALIAS"
    WITH_NAME = "WITH_NAME"
    SUB_QUERY_NAME = "SUB_QUERY_NAME"
    PARENTHESIS = "PARENTHESIS"


#: Maps normalised query-prefix strings to :class:`QueryType` values.
#: Cannot be replaced by the enum alone because ``WITH`` maps to
#: ``SELECT`` (a CTE followed by its main query) and composite prefixes
#: like ``CREATETABLE`` need their own entries.
SUPPORTED_QUERY_TYPES = {
    "INSERT": QueryType.INSERT,
    "REPLACE": QueryType.REPLACE,
    "UPDATE": QueryType.UPDATE,
    "SELECT": QueryType.SELECT,
    "DELETE": QueryType.DELETE,
    "WITH": QueryType.SELECT,
    "CREATETABLE": QueryType.CREATE,
    "CREATETEMPORARY": QueryType.CREATE,
    "ALTERTABLE": QueryType.ALTER,
    "DROPTABLE": QueryType.DROP,
    "CREATEFUNCTION": QueryType.CREATE,
    "TRUNCATETABLE": QueryType.TRUNCATE,
}

#: Union of all keyword sets the tokeniser cares about.  Tokens whose
#: normalised value falls outside this set are **not** tracked as the
#: ``last_keyword`` on subsequent tokens, keeping the classification
#: logic focused on structurally significant positions only.
RELEVANT_KEYWORDS = {
    *KEYWORDS_BEFORE_COLUMNS,
    *TABLE_ADJUSTMENT_KEYWORDS,
    *WITH_ENDING_KEYWORDS,
    *SUBQUERY_PRECEDING_KEYWORDS,
    "LIMIT",
    "OFFSET",
    "RETURNING",
    "VALUES",
    "INDEX",
    "KEY",
    "WITH",
    "WINDOW",
}
