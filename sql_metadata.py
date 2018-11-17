"""
This module provides SQL query parsing functions
"""
import re

import sqlparse

from sqlparse.sql import TokenList
from sqlparse.tokens import Name, Whitespace, Wildcard, Number, Punctuation


def unique(_list):
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


def preprocess_query(query):
    """
    Perform initial query cleanup

    :type query str
    :rtype str
    """
    # 1. remove aliases
    # FROM `dimension_wikis` `dw`
    # INNER JOIN `fact_wam_scores` `fwN`
    query = re.sub(r'(\s(FROM|JOIN)\s`[^`]+`)\s`[^`]+`', r'\1', query, flags=re.IGNORECASE)

    # 2. `database`.`table` notation -> database.table
    query = re.sub(r'`([^`]+)`\.`([^`]+)`', r'\1.\2', query)

    # 2. database.table notation -> table
    # query = re.sub(r'([a-z_0-9]+)\.([a-z_0-9]+)', r'\2', query, flags=re.IGNORECASE)

    return query


def get_query_tokens(query):
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


def get_query_columns(query):
    """
    :type query str
    :rtype: list[str]
    """
    columns = []
    last_keyword = None
    last_token = None

    # print(preprocess_query(query))

    keywords_ignored = ['AS', 'AND', 'OR', 'IN', 'IS', 'NOT', 'NOT NULL', 'LIKE']
    functions_ignored = ['COUNT', 'MIN', 'MAX', 'FROM_UNIXTIME', 'DATE_FORMAT']

    for token in get_query_tokens(query):
        if token.is_keyword and token.value.upper() not in keywords_ignored:
            # keep the name of the last keyword, e.g. SELECT, FROM, WHERE, (ORDER) BY
            last_keyword = token.value.upper()
            # print('keyword', last_keyword)
        elif token.ttype is Name:
            # analyze the name tokens, column names and where condition values
            if last_keyword in ['SELECT', 'WHERE', 'BY', 'ON'] \
                    and last_token.value.upper() not in ['AS']:
                # print(last_keyword, last_token, token.value)

                if token.value.upper() not in functions_ignored:
                    if str(last_token) == '.':
                        # print('DOT', last_token, columns[-1])

                        # we have table.column notation example
                        # append column name to the last entry of columns
                        # as it is a table name in fact
                        table_name = columns[-1]
                        columns[-1] = '{}.{}'.format(table_name, token)
                    else:
                        columns.append(str(token.value))
            elif last_keyword in ['INTO'] and last_token.ttype is Punctuation:
                # INSERT INTO `foo` (col1, `col2`) VALUES (..)
                #  print(last_keyword, token, last_token)
                columns.append(str(token.value).strip('`'))
        elif token.ttype is Wildcard:
            # handle * wildcard in SELECT part, but ignore count(*)
            # print(last_keyword, last_token, token.value)
            if last_keyword == 'SELECT' and last_token.value != '(':

                if str(last_token) == '.':
                    # handle SELECT foo.*
                    table_name = columns[-1]
                    columns[-1] = '{}.{}'.format(table_name, str(token))
                else:
                    columns.append(str(token.value))

        last_token = token

    return unique(columns)


def get_query_tables(query):
    """
    :type query str
    :rtype: list[str]
    """
    tables = []
    last_keyword = None
    last_token = None

    table_syntax_keywords = [
        # SELECT queries
        'FROM', 'WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'ON',
        # INSERT queries
        'INTO', 'VALUES',
        # UPDATE queries
        'UPDATE', 'SET',
    ]

    # print(query, get_query_tokens(query))

    for token in get_query_tokens(query):
        # print([token, token.ttype])
        if token.is_keyword and token.value.upper() in table_syntax_keywords:
            # keep the name of the last keyword, the next one can be a table name
            last_keyword = token.value.upper()
            # print('keyword', last_keyword)
        elif str(token) == '(':
            # reset the last_keyword for INSERT `foo` VALUES(id, bar) ...
            last_keyword = None
        elif token.is_keyword and str(token) == 'FORCE':
            # reset the last_keyword for "SELECT x FORCE INDEX" queries
            last_keyword = None
        elif token.is_keyword and str(token) == 'SELECT' and last_keyword == 'INTO':
            # reset the last_keyword for "INSERT INTO SELECT" queries
            last_keyword = None
        elif token.ttype is Name or token.is_keyword:
            # print([last_keyword, last_token, token.value])
            # analyze the name tokens, column names and where condition values
            if last_keyword in ['FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN',
                                'INTO', 'UPDATE'] \
                    and last_token not in ['AS'] \
                    and token.value not in ['AS', 'SELECT']:

                if last_token == '.':
                    # we have database.table notation example
                    # append table name to the last entry of tables
                    # as it is a database name in fact
                    database_name = tables[-1]
                    tables[-1] = '{}.{}'.format(database_name, token)
                else:
                    table_name = str(token.value.strip('`'))
                    tables.append(table_name)

        last_token = token.value.upper()

    return unique(tables)


def get_query_limit_and_offset(query):
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

        if token.is_keyword and token.value.upper() in ['LIMIT', 'OFFSET']:
            last_keyword = token.value.upper()
        elif token.ttype is Number.Integer:
            # print([token, last_keyword, last_token_was_integer])
            if last_keyword == 'LIMIT':
                # LIMIT <limit>
                limit = int(token.value)
                last_keyword = None
            elif last_keyword == 'OFFSET':
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


# SQL queries normalization (#16)
def normalize_likes(sql):
    """
    Normalize and wrap LIKE statements

    :type sql str
    :rtype: str
    """
    sql = sql.replace('%', '')

    # LIKE '%bot'
    sql = re.sub(r"LIKE '[^\']+'", 'LIKE X', sql)

    # or all_groups LIKE X or all_groups LIKE X
    matches = re.finditer(r'(or|and) [^\s]+ LIKE X', sql, flags=re.IGNORECASE)
    matches = [match.group(0) for match in matches] if matches else None

    if matches:
        for match in set(matches):
            sql = re.sub(r'(\s?' + re.escape(match) + ')+', ' ' + match + ' ...', sql)

    return sql


def remove_comments_from_sql(sql):
    """
    Removes comments from SQL query

    :type sql str|None
    :rtype: str
    """
    return re.sub(r'\s?/\*.+\*/', '', sql)


def generalize_sql(sql):
    """
    Removes most variables from an SQL query and replaces them with X or N for numbers.

    Based on Mediawiki's DatabaseBase::generalizeSQL

    :type sql str|None
    :rtype: str
    """
    if sql is None:
        return None

    # multiple spaces
    sql = re.sub(r'\s{2,}', ' ', sql)

    # MW comments
    # e.g. /* CategoryDataService::getMostVisited N.N.N.N */
    sql = remove_comments_from_sql(sql)

    # handle LIKE statements
    sql = normalize_likes(sql)

    sql = re.sub(r"\\\\", '', sql)
    sql = re.sub(r"\\'", '', sql)
    sql = re.sub(r'\\"', '', sql)
    sql = re.sub(r"'[^\']*'", 'X', sql)
    sql = re.sub(r'"[^\"]*"', 'X', sql)

    # All newlines, tabs, etc replaced by single space
    sql = re.sub(r'\s+', ' ', sql)

    # All numbers => N
    sql = re.sub(r'-?[0-9]+', 'N', sql)

    # WHERE foo IN ('880987','882618','708228','522330')
    sql = re.sub(r' (IN|VALUES)\s*\([^,]+,[^)]+\)', ' \\1 (XYZ)', sql, flags=re.IGNORECASE)

    return sql.strip()
