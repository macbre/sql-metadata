"""
This module provides SQL query parsing functions
"""
# pylint:disable=unsubscriptable-object
import re
from typing import List, Tuple, Optional, Dict

import sqlparse

from sqlparse.sql import TokenList
from sqlparse.tokens import Name, Whitespace, Wildcard, Number, Punctuation


def unique(_list: List) -> List:
    """
    Makes the list have unique items only and maintains the order

    list(set()) won't provide that

    :type _list list
    :rtype: list
    """
    ret = []

    for item in _list:
        if item not in ret:
            ret.append(item)

    return ret


def preprocess_query(query: str) -> str:
    """
    Perform initial query cleanup

    :type query str
    :rtype str
    """
    # 0. remove newlines
    query = query.replace("\n", " ")

    # 1. remove aliases
    # FROM `dimension_wikis` `dw`
    # INNER JOIN `fact_wam_scores` `fwN`
    query = re.sub(
        r"(\s(FROM|JOIN)\s`[^`]+`)\s`[^`]+`", r"\1", query, flags=re.IGNORECASE
    )

    # 2. `database`.`table` notation -> database.table
    query = re.sub(r"`([^`]+)`\.`([^`]+)`", r"\1.\2", query)

    # 2. database.table notation -> table
    # query = re.sub(r'([a-z_0-9]+)\.([a-z_0-9]+)', r'\2', query, flags=re.IGNORECASE)

    return query


def get_query_tokens(query: str) -> List[sqlparse.sql.Token]:
    """
    :type query str
    :rtype: list[sqlparse.sql.Token]
    """
    query = preprocess_query(query)
    parsed = sqlparse.parse(query)

    # handle empty queries (#12)
    if not parsed:
        return []

    tokens = TokenList(parsed[0].tokens).flatten()
    # print([(token.value, token.ttype) for token in tokens])

    return [token for token in tokens if token.ttype is not Whitespace]


def get_query_columns(query: str) -> List[str]:
    """
    :type query str
    :rtype: list[str]
    """
    columns = []
    last_keyword = None
    last_token = None

    # print(preprocess_query(query))

    # these keywords should not change the state of a parser
    # and not "reset" previously found SELECT keyword
    keywords_ignored = [
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

    # these keywords are followed by columns reference
    keywords_before_columns = ["SELECT", "WHERE", "ORDER BY", "ON"]

    # these function should be ignored
    # and not "reset" previously found SELECT keyword
    functions_ignored = [
        "COUNT",
        "MIN",
        "MAX",
        "FROM_UNIXTIME",
        "DATE_FORMAT",
        "CAST",
        "CONVERT",
    ]

    tables_aliases = get_query_table_aliases(query)

    def resolve_table_alias(_table_name: str) -> str:
        """
        Resolve aliases, e.g. SELECT bar.column FROM foo AS bar
        """
        if _table_name in tables_aliases:
            return tables_aliases[_table_name]
        return _table_name

    for token in get_query_tokens(query):
        if token.is_keyword and token.value.upper() not in keywords_ignored:
            # keep the name of the last keyword, e.g. SELECT, FROM, WHERE, (ORDER) BY
            last_keyword = token.value.upper()
            # print('keyword', last_keyword)
        elif token.ttype is Name:
            # analyze the name tokens, column names and where condition values
            if (
                last_keyword in keywords_before_columns
                and last_token.value.upper() not in ["AS"]
            ):
                if token.value.upper() not in functions_ignored:
                    if str(last_token) == ".":
                        # print('DOT', last_token, columns[-1])

                        # we have table.column notation example
                        # append column name to the last entry of columns
                        # as it is a table name in fact
                        table_name = resolve_table_alias(columns[-1])

                        columns[-1] = "{}.{}".format(table_name, token)
                    else:
                        columns.append(str(token.value))
            elif last_keyword in ["INTO"] and last_token.ttype is Punctuation:
                # INSERT INTO `foo` (col1, `col2`) VALUES (..)
                #  print(last_keyword, token, last_token)
                columns.append(str(token.value).strip("`"))
        elif token.ttype is Wildcard:
            # handle * wildcard in SELECT part, but ignore count(*)
            # print(last_keyword, last_token, token.value)
            if last_keyword == "SELECT" and last_token.value != "(":

                if str(last_token) == ".":
                    # handle SELECT foo.*
                    table_name = resolve_table_alias(columns[-1])
                    columns[-1] = "{}.{}".format(table_name, str(token))
                else:
                    columns.append(str(token.value))

        last_token = token

    return unique(columns)


def _get_token_normalized_value(token: str) -> str:
    return token.value.translate(str.maketrans("", "", " \n\t\r")).upper()


def _update_table_names(
    tables: List[str], tokens: List[sqlparse.sql.Token], index: int, last_keyword: str
) -> List[str]:
    """
    Return new table names matching database.table or database.schema.table notation

    :type tables list[str]
    :type tokens list[sqlparse.sql.Token]
    :type index int
    :type last_keyword str
    :rtype: list[str]
    """

    token = tokens[index]
    last_token = tokens[index - 1].value.upper() if index > 0 else None
    next_token = tokens[index + 1].value.upper() if index + 1 < len(tokens) else None

    if (
        last_keyword
        in [
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
        and last_token not in ["AS"]
        and token.value not in ["AS", "SELECT"]
    ):
        if last_token == "." and next_token != ".":
            # we have database.table notation example
            table_name = "{}.{}".format(tokens[index - 2], tokens[index])
            if len(tables) > 0:
                tables[-1] = table_name
            else:
                tables.append(table_name)

        schema_notation_match = (Name, ".", Name, ".", Name)
        schema_notation_tokens = (
            (
                tokens[index - 4].ttype,
                tokens[index - 3].value,
                tokens[index - 2].ttype,
                tokens[index - 1].value,
                tokens[index].ttype,
            )
            if len(tokens) > 4
            else None
        )
        if schema_notation_tokens == schema_notation_match:
            # we have database.schema.table notation example
            table_name = "{}.{}.{}".format(
                tokens[index - 4], tokens[index - 2], tokens[index]
            )
            if len(tables) > 0:
                tables[-1] = table_name
            else:
                tables.append(table_name)
        elif _get_token_normalized_value(tokens[index - 1]) not in [",", last_keyword]:
            # it's not a list of tables, e.g. SELECT * FROM foo, bar
            # hence, it can be the case of alias without AS, e.g. SELECT * FROM foo bar
            pass
        else:
            table_name = str(token.value.strip("`"))
            tables.append(table_name)

    return tables


def get_query_tables(query: str) -> List[str]:
    """
    :type query str
    :rtype: list[str]
    """
    tables = []
    last_keyword = None

    table_syntax_keywords = [
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

    # print(query, get_query_tokens(query))
    query = query.replace('"', "")
    tokens = get_query_tokens(query)

    for index, token in enumerate(tokens):
        # print([token, token.ttype, last_token, last_keyword])

        # remove whitespaces from token value and uppercase
        token_val_norm = _get_token_normalized_value(token)
        if token.is_keyword and token_val_norm in table_syntax_keywords:
            # keep the name of the last keyword, the next one can be a table name
            last_keyword = token_val_norm
            # print('keyword', last_keyword)
        elif str(token) == "(":
            # reset the last_keyword for INSERT `foo` VALUES(id, bar) ...
            last_keyword = None
        elif token.is_keyword and token_val_norm in ["FORCE", "ORDER", "GROUPBY"]:
            # reset the last_keyword for queries like:
            # "SELECT x FORCE INDEX"
            # "SELECT x ORDER BY"
            # "SELECT x FROM y GROUP BY x"
            last_keyword = None
        elif (
            token.is_keyword
            and token_val_norm == "SELECT"
            and last_keyword in ["INTO", "TABLE"]
        ):
            # reset the last_keyword for "INSERT INTO SELECT" and "INSERT TABLE SELECT" queries
            last_keyword = None
        elif token.ttype is Name or token.is_keyword:
            tables = _update_table_names(tables, tokens, index, last_keyword)

    return unique(tables)


def get_query_limit_and_offset(query: str) -> Optional[Tuple[int, int]]:
    """
    :type query str
    :rtype: (int, int)
    """
    limit = None
    offset = None
    last_keyword = None
    last_token = None

    # print(query)
    for token in get_query_tokens(query):
        # print([token, token.ttype, last_keyword])

        if token.is_keyword and token.value.upper() in ["LIMIT", "OFFSET"]:
            last_keyword = token.value.upper()
        elif token.ttype is Number.Integer:
            # print([token, last_keyword, last_token_was_integer])
            if last_keyword == "LIMIT":
                # LIMIT <limit>
                limit = int(token.value)
                last_keyword = None
            elif last_keyword == "OFFSET":
                # OFFSET <offset>
                offset = int(token.value)
                last_keyword = None
            elif last_token and last_token.ttype is Punctuation:
                # LIMIT <offset>,<limit>
                offset = limit
                limit = int(token.value)

        last_token = token

    if limit is None:
        return None

    return limit, offset or 0


def get_query_table_aliases(query: str) -> Dict[str, str]:
    """
    Returns tables aliases mapping from a given query

    E.g. SELECT a.* FROM users1 AS a JOIN users2 AS b ON a.ip_address = b.ip_address
    will give you {'a': 'users1', 'b': 'users2'}
    """
    aliases = dict()
    last_keyword_token = None
    last_table_name = None

    for token in get_query_tokens(query):
        # print(token.ttype, token, last_table_name)

        # handle "FROM foo alias" syntax (i.e, "AS" keyword is missing)
        # if last_table_name and token.ttype is Name:
        #     aliases[token.value] = last_table_name
        #     last_table_name = False

        if last_keyword_token:
            if last_keyword_token.value.upper() in ["FROM", "JOIN", "INNER JOIN"]:
                last_table_name = token.value

            elif last_table_name and last_keyword_token.value.upper() in ["AS"]:
                aliases[token.value] = last_table_name
                last_table_name = False

        last_keyword_token = token if token.is_keyword else False

    return aliases


# SQL queries normalization (#16)
def normalize_likes(sql: str) -> str:
    """
    Normalize and wrap LIKE statements

    :type sql str
    :rtype: str
    """
    sql = sql.replace("%", "")

    # LIKE '%bot'
    sql = re.sub(r"LIKE '[^\']+'", "LIKE X", sql)

    # or all_groups LIKE X or all_groups LIKE X
    matches = re.finditer(r"(or|and) [^\s]+ LIKE X", sql, flags=re.IGNORECASE)
    matches = [match.group(0) for match in matches] if matches else None

    if matches:
        for match in set(matches):
            sql = re.sub(r"(\s?" + re.escape(match) + ")+", " " + match + " ...", sql)

    return sql


def remove_comments_from_sql(sql: str) -> str:
    """
    Removes comments from SQL query

    :type sql str|None
    :rtype: str
    """
    return re.sub(r"\s?/\*.+\*/", "", sql)


def generalize_sql(sql: Optional[str]) -> Optional[str]:
    """
    Removes most variables from an SQL query and replaces them with X or N for numbers.

    Based on Mediawiki's DatabaseBase::generalizeSQL

    :type sql str|None
    :rtype: str
    """
    if sql is None:
        return None

    # multiple spaces
    sql = re.sub(r"\s{2,}", " ", sql)

    # MW comments
    # e.g. /* CategoryDataService::getMostVisited N.N.N.N */
    sql = remove_comments_from_sql(sql)

    # handle LIKE statements
    sql = normalize_likes(sql)

    sql = re.sub(r"\\\\", "", sql)
    sql = re.sub(r"\\'", "", sql)
    sql = re.sub(r'\\"', "", sql)
    sql = re.sub(r"'[^\']*'", "X", sql)
    sql = re.sub(r'"[^\"]*"', "X", sql)

    # All newlines, tabs, etc replaced by single space
    sql = re.sub(r"\s+", " ", sql)

    # All numbers => N
    sql = re.sub(r"-?[0-9]+", "N", sql)

    # WHERE foo IN ('880987','882618','708228','522330')
    sql = re.sub(
        r" (IN|VALUES)\s*\([^,]+,[^)]+\)", " \\1 (XYZ)", sql, flags=re.IGNORECASE
    )

    return sql.strip()
