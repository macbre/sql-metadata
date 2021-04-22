"""
This module provides SQL query parsing functions
"""
import re
from typing import Dict, List, Optional, Tuple

import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Name, Number, Punctuation, Whitespace, Wildcard

from sql_metadata.generalizator import Generalizator
from sql_metadata.keywords_lists import (
    FUNCTIONS_IGNORED,
    KEYWORDS_BEFORE_COLUMNS,
    KEYWORDS_IGNORED,
    TABLE_ADJUSTMENT_KEYWORDS,
)
from sql_metadata.token import EmptyToken, SQLToken
from sql_metadata.utils import unique, update_table_names


class Parser:
    """
    Main class to parse sql query
    """
    def __init__(self, sql: Optional[str] = None) -> None:
        self._raw_query = sql
        self.query = self._preprocess_query()

    def _preprocess_query(self) -> Optional[str]:
        """
        Perform initial query cleanup

        :type query str
        :rtype str
        """
        # 0. remove newlines
        if self._raw_query is None:
            return None
        query = self._raw_query.replace("\n", " ")
        # 1. remove quotes "
        query = query.replace('"', "")

        # 2. `database`.`table` notation -> database.table
        query = re.sub(r"`([^`]+)`\.`([^`]+)`", r"\1.\2", query)

        return query

    @property
    def tokens(self) -> List[SQLToken]:
        """
        :rtype: list[SQLToken]
        """
        parsed = sqlparse.parse(self.query)
        tokens = []
        # handle empty queries (#12)
        if not parsed:
            return tokens

        sqlparse_tokens = TokenList(parsed[0].tokens).flatten()
        non_empty_tokens = [
            token for token in sqlparse_tokens if token.ttype is not Whitespace
        ]
        last_keyword = None
        for index, tok in enumerate(non_empty_tokens):
            token = SQLToken(
                value=tok.value,
                is_keyword=tok.is_keyword,
                is_name=tok.ttype is Name,
                is_punctuation=tok.ttype is Punctuation,
                is_dot=str(tok) == ".",
                is_wildcard=tok.ttype is Wildcard,
                is_integer=tok.ttype is Number.Integer,
                is_left_parenthesis=str(tok) == "(",
                is_right_parenthesis=str(tok) == ")",
                last_keyword=last_keyword,
                next_token=EmptyToken,
                previous_token=EmptyToken,
            )
            if index > 0:
                token.previous_token = tokens[index - 1]
                tokens[index - 1].next_token = token

            if tok.is_keyword and tok.normalized not in KEYWORDS_IGNORED:
                last_keyword = tok.normalized
            tokens.append(token)

        return tokens

    @property
    def columns(self) -> List[str]:
        """
        :rtype: list[str]
        """
        columns = []
        tables_aliases = self.tables_aliases

        def resolve_table_alias(_table_name: str) -> str:
            """
            Resolve aliases, e.g. SELECT bar.column FROM foo AS bar
            """
            if _table_name in tables_aliases:
                return tables_aliases[_table_name]
            return _table_name

        for token in self.tokens:
            if token.is_name:
                # analyze the name tokens, column names and where condition values
                if (
                    token.last_keyword in KEYWORDS_BEFORE_COLUMNS
                    and token.previous_token.upper not in ["AS"]
                ):
                    if token.upper not in FUNCTIONS_IGNORED:
                        if token.previous_token.is_dot:
                            # print('DOT', last_token, columns[-1])

                            # we have table.column notation example
                            # append column name to the last entry of columns
                            # as it is a table name in fact
                            table_name = resolve_table_alias(columns[-1])

                            columns[-1] = "{}.{}".format(table_name, token.value)
                        else:
                            columns.append(str(token.value))
                elif (
                    token.last_keyword in ["INTO"]
                    and token.previous_token.is_punctuation
                ):
                    # INSERT INTO `foo` (col1, `col2`) VALUES (..)
                    #  print(last_keyword, token, last_token)
                    columns.append(str(token.value).strip("`"))
            elif token.is_wildcard:
                # handle * wildcard in SELECT part, but ignore count(*)
                # print(last_keyword, last_token, token.value)
                if (
                    token.last_keyword == "SELECT"
                    and not token.previous_token.is_left_parenthesis
                ):

                    if token.previous_token.is_dot:
                        # handle SELECT foo.*
                        table_name = resolve_table_alias(columns[-1])
                        columns[-1] = "{}.{}".format(table_name, str(token.value))
                    else:
                        columns.append(str(token.value))

        return unique(columns)

    @property
    def tables(self) -> List[str]:
        """
        :rtype: list[str]
        """
        tables = []
        with_names = self.with_names

        for index, token in enumerate(self.tokens):
            if token.is_name or token.is_keyword:
                tables = update_table_names(tables, token, index)

        return [x for x in unique(tables) if x not in with_names]

    @property
    def limit_and_offset(self) -> Optional[Tuple[int, int]]:
        """
        :type query str
        :rtype: (int, int)
        """
        limit = None
        offset = None

        # print(query)
        for token in self.tokens:
            # print([token, token.ttype, last_keyword])
            if token.is_integer:
                # print([token, last_keyword, last_token_was_integer])
                if token.last_keyword == "LIMIT" and not limit:
                    # LIMIT <limit>
                    limit = int(token.value)
                elif token.last_keyword == "OFFSET":
                    # OFFSET <offset>
                    offset = int(token.value)
                elif token.previous_token.is_punctuation:
                    # LIMIT <offset>,<limit>
                    offset = limit
                    limit = int(token.value)

        if limit is None:
            return None

        return limit, offset or 0

    @property
    def tables_aliases(self) -> Dict[str, str]:
        """
        Returns tables aliases mapping from a given query

        E.g. SELECT a.* FROM users1 AS a JOIN users2 AS b ON a.ip_address = b.ip_address
        will give you {'a': 'users1', 'b': 'users2'}
        """
        aliases = dict()
        tables = self.tables

        for token in self.tokens:
            if (
                token.last_keyword_normalized in TABLE_ADJUSTMENT_KEYWORDS
                and token.is_name
                and token.next_token.upper != "AS"
                and not token.next_token.is_dot
            ):
                if token.previous_token.upper in ["AS"]:
                    # potential <DB.<SCHEMA>.<TABLE> as <ALIAS>
                    potential_table_name = token.get_nth_previous(2).left_expanded
                else:
                    # potential <DB.<SCHEMA>.<TABLE> <ALIAS>
                    potential_table_name = token.previous_token.left_expanded

                if potential_table_name in tables:
                    aliases[token.value] = potential_table_name

        return aliases

    @property
    def with_names(self) -> List[str]:
        """
        Returns with statements aliases list from a given query

        E.g. WITH database1.tableFromWith AS (SELECT * FROM table3)
             SELECT "xxxxx" FROM database1.tableFromWith alias
             LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx")
        will give you ["database1.tableFromWith"]
        """
        with_names = []
        for token in self.tokens:
            if token.previous_token.upper == "WITH":
                in_with = True
                while in_with and token.next_token:
                    # name is first
                    if token.next_token.upper == "AS":
                        with_names.append(token.left_expanded)
                        # move to next with if exists, this with ends with
                        #  ) + , if many withs or ) + select if one
                        # need to move to next as AS can be in subqueries defining with
                        while not (
                            token.is_right_parenthesis
                            and (
                                token.next_token.is_punctuation
                                or token.next_token.normalized
                                in ["UPDATE", "SELECT", "DELETE"]
                            )
                        ):
                            token = token.next_token
                        if token.next_token.normalized in [
                            "UPDATE",
                            "SELECT",
                            "DELETE",
                        ]:
                            in_with = False
                    else:
                        token = token.next_token

        return with_names

    @property
    def remove_comments(self) -> str:
        """
        Removes comments from SQL query

        :type sql str|None
        :rtype: str
        """
        return Generalizator(self._raw_query).remove_comments

    @property
    def generalize(self) -> Optional[str]:
        """
        Removes most variables from an SQL query
        and replaces them with X or N for numbers.

        Based on Mediawiki's DatabaseBase::generalizeSQL

        :type sql str|None
        :rtype: str
        """
        return Generalizator(self._raw_query).generalize
