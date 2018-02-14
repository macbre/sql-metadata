"""
This module provides SQL query parsing functions
"""
import re

import sqlparse

from sqlparse.sql import TokenList
from sqlparse.tokens import Name, Whitespace, Wildcard, Number, Punctuation


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

    # 2. `database`.`table` notation -> table
    query = re.sub(r'`([^`]+)`\.`([^`]+)`', r'\2', query)

    # 2. `database`.`table` notation -> table
    query = re.sub(r'([a-z_0-9]+)\.([a-z_0-9]+)', r'\2', query, flags=re.IGNORECASE)

    return query


def get_query_tokens(query):
    """
    :type query str
    :rtype: list[sqlparse.sql.Token]
    """
    query = preprocess_query(query)

    tokens = TokenList(sqlparse.parse(query)[0].tokens).flatten()
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
            if last_keyword in ['SELECT', 'WHERE', 'BY'] and last_token not in ['AS']:
                # print(last_keyword, last_token, token.value)

                if token.value not in columns \
                        and token.value.upper() not in functions_ignored:
                    columns.append(str(token.value))
        elif token.ttype is Wildcard:
            # handle wildcard in SELECT part, but ignore count(*)
            # print(last_keyword, last_token, token.value)
            if last_keyword == 'SELECT' and last_token != '(':
                columns.append(str(token.value))

        last_token = token.value.upper()

    return columns


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

    for token in get_query_tokens(query):
        # print([token, token.ttype])
        if token.is_keyword and token.value.upper() in table_syntax_keywords:
            # keep the name of the last keyword, the next one can be a table name
            last_keyword = token.value.upper()
            # print('keyword', last_keyword)
        elif str(token) == '(':
            # reset the last_keyword for INSERT `foo` VALUES(id, bar) ...
            last_keyword = None
        elif token.ttype is Name or token.is_keyword:
            # print([last_keyword, last_token, token.value])
            # analyze the name tokens, column names and where condition values
            if last_keyword in ['FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN',
                                'INTO', 'UPDATE'] \
                    and last_token not in ['AS'] \
                    and token.value not in ['AS']:
                table_name = str(token.value.strip('`'))
                if table_name not in tables:
                    tables.append(table_name)

        last_token = token.value.upper()

    return tables


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
