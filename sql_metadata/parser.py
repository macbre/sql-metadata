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
    SUBQUERY_PRECEDING_KEYWORDS,
    TABLE_ADJUSTMENT_KEYWORDS,
    WITH_ENDING_KEYWORDS,
)
from sql_metadata.token import EmptyToken, SQLToken
from sql_metadata.utils import UniqueList


class Parser:  # pylint: disable=R0902
    """
    Main class to parse sql query
    """

    def __init__(self, sql: str = "") -> None:
        self._raw_query = sql
        self._query = self._preprocess_query()

        self._tokens = None

        self._columns = None
        self._columns_dict = None

        self._tables = None
        self._table_aliases = None

        self._with_names = None
        self._subqueries = None
        self._subqueries_names = None

        self._limit_and_offset = None

        self._values = None
        self._values_dict = None

    @property
    def query(self) -> str:
        """
        Returns preprocessed query
        """
        return self._query

    @property
    def tokens(self) -> List[SQLToken]:
        """
        :rtype: list[SQLToken]
        """
        if self._tokens is not None:
            return self._tokens

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
        subquery_level = 0
        open_parenthesises = []
        for index, tok in enumerate(non_empty_tokens):
            token = SQLToken(
                value=tok.value,
                is_keyword=tok.is_keyword,
                is_name=tok.ttype is Name,
                is_punctuation=tok.ttype is Punctuation,
                is_dot=str(tok) == ".",
                is_wildcard=tok.ttype is Wildcard,
                is_integer=tok.ttype is Number.Integer,
                is_float=tok.ttype is Number.Float,
                is_left_parenthesis=str(tok) == "(",
                is_right_parenthesis=str(tok) == ")",
                position=index,
                last_keyword=last_keyword,
                next_token=EmptyToken,
                previous_token=EmptyToken,
                subquery_level=subquery_level,
            )
            if index > 0:
                token.previous_token = tokens[index - 1]
                tokens[index - 1].next_token = token

            if (
                token.is_left_parenthesis
                and token.previous_token.normalized not in SUBQUERY_PRECEDING_KEYWORDS
            ):
                token.is_nested_function_start = True
                open_parenthesises.append(token)
            elif (
                token.is_left_parenthesis
                and token.previous_token.normalized in SUBQUERY_PRECEDING_KEYWORDS
            ):
                token.is_subquery_start = True
                subquery_level += 1
                token.subquery_level = subquery_level
                open_parenthesises.append(token)
            elif token.is_right_parenthesis:
                last_open_parenthesis = open_parenthesises.pop(-1)
                if last_open_parenthesis.is_subquery_start:
                    token.is_subquery_end = True
                    subquery_level -= 1
                else:
                    token.is_nested_function_end = True

            if tok.is_keyword and tok.normalized not in KEYWORDS_IGNORED:
                last_keyword = tok.normalized
            tokens.append(token)

        self._tokens = tokens
        return tokens

    @property
    def columns(self) -> List[str]:
        """
        :rtype: list[str]
        """
        if self._columns is not None:
            return self._columns
        columns = UniqueList()
        tables_aliases = self.tables_aliases
        subqueries_names = self.subqueries_names

        for token in self.tokens:
            if token.is_name and not token.next_token.is_dot:
                # analyze the name tokens, column names and where condition values
                if (
                    token.last_keyword_normalized in KEYWORDS_BEFORE_COLUMNS
                    and token.previous_token.normalized != "AS"
                ):
                    if (
                        token.normalized not in FUNCTIONS_IGNORED
                        and not (
                            # aliases of sub-queries i.e.: select from (...) <alias>
                            token.previous_token.is_right_parenthesis
                            and token.value in subqueries_names
                        )
                        # custom functions - they are followed by the parenthesis
                        # e.g. custom_func(...
                        and not token.next_token.is_left_parenthesis
                    ):
                        column = token.table_prefixed_column(tables_aliases)
                        self._add_to_columns_subsection(
                            keyword=token.last_keyword_normalized, column=column
                        )
                        columns.append(column)
                elif (
                    token.last_keyword_normalized == "INTO"
                    and token.previous_token.is_punctuation
                ):
                    # INSERT INTO `foo` (col1, `col2`) VALUES (..)
                    column = str(token.value).strip("`")
                    self._add_to_columns_subsection(
                        keyword=token.last_keyword_normalized, column=column
                    )
                    columns.append(column)
            elif (
                token.is_wildcard
                and token.last_keyword_normalized == "SELECT"
                and not token.previous_token.is_left_parenthesis
            ):
                # handle * wildcard in SELECT part, but ignore count(*)
                column = token.table_prefixed_column(tables_aliases)
                self._add_to_columns_subsection(
                    keyword=token.last_keyword_normalized, column=column
                )
                columns.append(column)

        self._columns = columns
        return self._columns

    @property
    def columns_without_subqueries(self) -> List:
        """
        Returns columns without ones explicitly coming from sub-queries
        """
        columns = self.columns
        subqueries = self.subqueries_names
        return [column for column in columns if column.split(".")[0] not in subqueries]

    @property
    def columns_dict(self) -> Dict[str, List[str]]:
        """
        Returns dictionary of column names divided into section of the query in which
        given column is present.

        Sections consist of: select, where, order_by, join, insert and update
        """
        if self._columns_dict:
            return self._columns_dict
        _ = self.columns
        return self._columns_dict

    @property
    def tables(self) -> List[str]:
        """
        :rtype: list[str]
        """
        if self._tables is not None:
            return self._tables
        tables = UniqueList()
        with_names = self.with_names

        for token in self.tokens:
            if (
                (token.is_name or token.is_keyword)
                and token.last_keyword_normalized in TABLE_ADJUSTMENT_KEYWORDS
                and token.previous_token.normalized not in ["AS", "WITH"]
                and token.normalized not in ["AS", "SELECT"]
            ):
                if token.next_token.is_dot:
                    pass  # part of the qualified name
                elif token.previous_token.is_dot:
                    tables.append(token.left_expanded)  # full qualified name
                elif (
                    token.previous_token.normalized != token.last_keyword_normalized
                    and not token.previous_token.is_punctuation
                ) or token.previous_token.is_right_parenthesis:
                    # it's not a list of tables, e.g. SELECT * FROM foo, bar
                    # hence, it can be the case of alias without AS,
                    # e.g. SELECT * FROM foo bar
                    # or an alias of subquery (SELECT * FROM foo) bar
                    pass
                elif (
                    token.last_keyword_normalized == "INTO" and token.is_in_parenthesis
                ):
                    # we are in <columns> of INSERT INTO <TABLE> (<columns>)
                    pass
                else:
                    table_name = str(token.value.strip("`"))
                    tables.append(table_name)

        self._tables = tables - with_names
        return self._tables

    @property
    def limit_and_offset(self) -> Optional[Tuple[int, int]]:
        """
        Returns value for limit and offset if set

        :rtype: (int, int)
        """
        if self._limit_and_offset is not None:
            return self._limit_and_offset
        limit = None
        offset = None

        for token in self.tokens:
            if token.is_integer:
                if token.last_keyword_normalized == "LIMIT" and not limit:
                    # LIMIT <limit>
                    limit = int(token.value)
                elif token.last_keyword_normalized == "OFFSET":
                    # OFFSET <offset>
                    offset = int(token.value)
                elif token.previous_token.is_punctuation:
                    # LIMIT <offset>,<limit>
                    offset = limit
                    limit = int(token.value)

        if limit is None:
            return None

        self._limit_and_offset = limit, offset or 0
        return self._limit_and_offset

    @property
    def tables_aliases(self) -> Dict[str, str]:
        """
        Returns tables aliases mapping from a given query

        E.g. SELECT a.* FROM users1 AS a JOIN users2 AS b ON a.ip_address = b.ip_address
        will give you {'a': 'users1', 'b': 'users2'}
        """
        if self._table_aliases is not None:
            return self._table_aliases
        aliases = dict()
        tables = self.tables

        for token in self.tokens:
            if (
                token.last_keyword_normalized in TABLE_ADJUSTMENT_KEYWORDS
                and token.is_name
                and token.next_token.normalized != "AS"
                and not token.next_token.is_dot
            ):
                if token.previous_token.normalized == "AS":
                    # potential <DB.<SCHEMA>.<TABLE> as <ALIAS>
                    potential_table_name = token.get_nth_previous(2).left_expanded
                else:
                    # potential <DB.<SCHEMA>.<TABLE> <ALIAS>
                    potential_table_name = token.previous_token.left_expanded

                if potential_table_name in tables:
                    aliases[token.value] = potential_table_name

        self._table_aliases = aliases
        return self._table_aliases

    @property
    def with_names(self) -> List[str]:
        """
        Returns with statements aliases list from a given query

        E.g. WITH database1.tableFromWith AS (SELECT * FROM table3)
             SELECT "xxxxx" FROM database1.tableFromWith alias
             LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx")
        will return ["database1.tableFromWith"]
        """
        if self._with_names is not None:
            return self._with_names
        with_names = UniqueList()
        for token in self.tokens:
            if token.previous_token.normalized == "WITH":
                in_with = True
                while in_with and token.next_token:
                    # name is first
                    if token.next_token.normalized == "AS":
                        with_names.append(token.left_expanded)
                        # move to next with if exists, this with ends with
                        #  ) + , if many withs or ) + select if one
                        # need to move to next as AS can be in
                        # sub-queries inside with definition
                        while token.next_token and not (
                            token.is_right_parenthesis
                            and (
                                token.next_token.is_punctuation
                                or token.next_token.normalized in WITH_ENDING_KEYWORDS
                            )
                        ):
                            token = token.next_token
                        if token.next_token.normalized in WITH_ENDING_KEYWORDS:
                            in_with = False
                    else:
                        token = token.next_token

        self._with_names = with_names
        return self._with_names

    @property
    def subqueries(self) -> Dict:
        """
        Returns a dictionary with all sub-queries existing in query
        """
        if self._subqueries is not None:
            return self._subqueries
        subqueries = dict()
        token = self.tokens[0]
        while token.next_token:
            if token.previous_token.is_subquery_start:
                current_subquery = []
                current_level = token.subquery_level
                inner_token = token
                while (
                    inner_token.next_token
                    and not inner_token.next_token.subquery_level < current_level
                ):
                    current_subquery.append(inner_token)
                    inner_token = inner_token.next_token
                if inner_token.next_token.value in self.subqueries_names:
                    query_name = inner_token.next_token.value
                else:
                    query_name = inner_token.next_token.next_token.value
                subquery_text = "".join([x.stringified_token for x in current_subquery])
                subqueries[query_name] = subquery_text

            token = token.next_token

        self._subqueries = subqueries
        return self._subqueries

    @property
    def subqueries_names(self) -> List[str]:
        """
        Returns sub-queries aliases list from a given query

        e.g. SELECT COUNT(1) FROM
            (SELECT std.task_id FROM some_task_detail std WHERE std.STATUS = 1) a
             JOIN (SELECT st.task_id FROM some_task st WHERE task_type_id = 80) b
             ON a.task_id = b.task_id;
        will return ["a", "b"]
        """
        if self._subqueries_names is not None:
            return self._subqueries_names
        subqueries_names = UniqueList()
        for token in self.tokens:
            if (token.previous_token.is_subquery_end and token.normalized != "AS") or (
                token.previous_token.normalized == "AS"
                and token.get_nth_previous(2).is_subquery_end
            ):
                subqueries_names.append(str(token))

        self._subqueries_names = subqueries_names
        return self._subqueries_names

    @property
    def values(self) -> List:
        """
        Returns list of values from insert queries
        """
        if self._values:
            return self._values
        values = []
        for token in self.tokens:
            if (
                token.last_keyword_normalized == "VALUES"
                and token.is_in_parenthesis
                and token.next_token.is_punctuation
            ):
                if token.is_integer:
                    value = int(token.value)
                elif token.is_float:
                    value = float(token.value)
                else:
                    value = token.value.strip("'\"")
                values.append(value)
        self._values = values
        return self._values

    @property
    def values_dict(self) -> Dict:
        """
        Returns dictionary of column-value pairs.
        If columns are not set the auto generated column_<col_number> are added.
        """
        values = self.values
        if self._values_dict or not values:
            return self._values_dict
        columns = self.columns
        if not columns:
            columns = [f"column_{ind + 1}" for ind in range(len(values))]
        values_dict = dict(zip(columns, values))
        self._values_dict = values_dict
        return self._values_dict

    @property
    def comments(self) -> List[str]:
        """
        Return comments from SQL query

        :rtype: List[str]
        """
        return Generalizator(self._raw_query).comments

    @property
    def without_comments(self) -> str:
        """
        Removes comments from SQL query

        :rtype: str
        """
        return Generalizator(self._raw_query).without_comments

    @property
    def generalize(self) -> Optional[str]:
        """
        Removes most variables from an SQL query
        and replaces them with X or N for numbers.

        Based on Mediawiki's DatabaseBase::generalizeSQL

        :rtype: Optional[str]
        """
        return Generalizator(self._raw_query).generalize

    def _add_to_columns_subsection(self, keyword: str, column: str):
        sections = {
            "SELECT": "select",
            "WHERE": "where",
            "ORDERBY": "order_by",
            "ON": "join",
            "INTO": "insert",
            "SET": "update",
        }
        section = sections[keyword]
        self._columns_dict = self._columns_dict or dict()
        self._columns_dict.setdefault(section, UniqueList()).append(column)

    def _preprocess_query(self) -> str:
        """
        Perform initial query cleanup

        :rtype str
        """
        if self._raw_query == "":
            return ""

        # 0. remove newlines
        query = self._raw_query.replace("\n", " ")
        # 1. remove quotes "
        query = query.replace('"', "")

        # 2. `database`.`table` notation -> database.table
        query = re.sub(r"`([^`]+)`\.`([^`]+)`", r"\1.\2", query)

        return query
