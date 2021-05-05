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
    COLUMNS_SECTIONS,
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
        self._columns_aliases_names = None
        self._columns_aliases = None
        self._columns_with_tables_aliases = dict()
        self._columns_aliases_dict = None

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
        Tokenizes the query
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
                if (
                    token.previous_token.normalized in KEYWORDS_BEFORE_COLUMNS
                    or token.previous_token.normalized == ","
                ):
                    # we are in columns it's a column subquery definition
                    token.is_column_definition_start = True
                else:
                    # nested function
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
                elif last_open_parenthesis.is_column_definition_start:
                    token.is_column_definition_end = True
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
        Returns the list columns this query refers to
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
                    and token.previous_token.normalized not in ["AS", ")"]
                    and token.previous_token.table_prefixed_column(tables_aliases)
                    not in columns
                    and token.left_expanded not in self.columns_aliases_names
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
                        self._columns_with_tables_aliases[token.left_expanded] = column
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
                elif token.left_expanded in self.columns_aliases_names:
                    self._add_to_columns_aliases_subsection(
                        token.last_keyword_normalized, token.left_expanded
                    )
            elif (
                token.is_wildcard
                and token.last_keyword_normalized == "SELECT"
                and not token.previous_token.is_left_parenthesis
            ):
                # handle * wildcard in SELECT part, but ignore count(*)
                column = token.table_prefixed_column(tables_aliases)
                self._columns_with_tables_aliases[token.left_expanded] = column
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
        if not self._columns_dict:
            _ = self.columns
        if self.columns_aliases_dict:
            for key, value in self.columns_aliases_dict.items():
                for alias in value:
                    self._columns_dict.setdefault(key, UniqueList()).append(
                        self._resolve_column_alias(alias)
                    )
        return self._columns_dict

    @property
    def columns_aliases(self) -> Dict:
        """
        Returns a dictionary of column aliases with columns
        """
        if self._columns_aliases is not None:
            return self._columns_aliases
        column_aliases = dict()
        if self._columns is None:
            _ = self.columns
        aliases_of = (
            list(self._columns_with_tables_aliases.keys())
            + self.columns_aliases_names
            + ["*"]
        )
        for token in self.tokens:
            if (
                token.value in self.columns_aliases_names
                and token.value not in column_aliases
                and not token.previous_token.is_left_parenthesis
            ):
                if token.previous_token.normalized == "AS":
                    token_check = token.get_nth_previous(2)
                else:
                    token_check = token.previous_token
                if token_check.is_column_definition_end:
                    # nested subquery like select (select a as b from x) column
                    start_token = token.find_token(
                        True, value_attribute="is_column_definition_start"
                    )
                    if start_token.next_token.normalized == "SELECT":
                        # we have a subquery
                        alias_token = start_token.next_token.find_token(
                            aliases_of, direction="right"
                        )
                        alias_of = self._resolve_alias_to_column(alias_token)
                    else:
                        # chain of functions or redundant parenthesis
                        loop_token = start_token
                        aliases = []
                        while loop_token.next_token != token:
                            if loop_token.next_token.left_expanded in aliases_of:
                                alias_token = loop_token.next_token
                                aliases.append(
                                    self._resolve_alias_to_column(alias_token)
                                )
                            loop_token = loop_token.next_token
                        alias_of = aliases

                elif token_check.is_nested_function_end:
                    # it can be one function or a chain of functions
                    start_token = token.find_token(
                        [",", "SELECT"], value_attribute="normalized"
                    )
                    loop_token = start_token
                    aliases = []
                    while loop_token.next_token != token:
                        if loop_token.next_token.left_expanded in aliases_of:
                            alias_token = loop_token.next_token
                            aliases.append(self._resolve_alias_to_column(alias_token))
                        loop_token = loop_token.next_token
                    alias_of = aliases
                else:
                    alias_token = token.find_token(
                        aliases_of, value_attribute="left_expanded"
                    )
                    alias_of = self._resolve_alias_to_column(alias_token)

                # if token.value not in [alias_of, ""]:
                if isinstance(alias_of, list) and len(alias_of) == 1:
                    alias_of = alias_of[0]
                if token.value != alias_of:
                    # skip aliases of self, like sum(column) as column
                    column_aliases[token.left_expanded] = alias_of

        self._columns_aliases = column_aliases
        return self._columns_aliases

    @property
    def columns_aliases_dict(self) -> Dict[str, List[str]]:
        """
        Returns dictionary of column names divided into section of the query in which
        given column is present.

        Sections consist of: select, where, order_by, join, insert and update
        """
        if self._columns_aliases_dict:
            return self._columns_aliases_dict
        _ = self.columns_aliases_names
        return self._columns_aliases_dict

    @property
    def columns_aliases_names(self) -> List[str]:
        """
        Extract names of the column aliases used in query
        """
        if self._columns_aliases_names is not None:
            return self._columns_aliases_names
        column_aliases_names = UniqueList()
        for token in self.tokens:
            if token.is_name and not token.next_token.is_dot:
                # analyze the name tokens, column names and where condition values
                if (
                    token.last_keyword_normalized in KEYWORDS_BEFORE_COLUMNS
                    and token.previous_token.normalized in ["AS", ")"]
                    and token.value not in self.with_names + self.subqueries_names
                ):
                    alias = token.left_expanded
                    column_aliases_names.append(alias)

        self._columns_aliases_names = column_aliases_names
        return self._columns_aliases_names

    @property
    def tables(self) -> List[str]:
        """
        Return the list of tables this query refers to
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
                elif token.is_in_parenthesis and (
                    token.find_token("(").previous_token.value in with_names
                    or token.last_keyword_normalized == "INTO"
                ):
                    # we are in <columns> of INSERT INTO <TABLE> (<columns>)
                    # or columns of with statement: with (<columns>) as ...
                    pass
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
                else:
                    table_name = str(token.value.strip("`"))
                    tables.append(table_name)

        self._tables = tables - with_names
        return self._tables

    @property
    def limit_and_offset(self) -> Optional[Tuple[int, int]]:
        """
        Returns value for limit and offset if set
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
                        if token.is_right_parenthesis:
                            # inside columns of with statement
                            # like: with (col1, col2) as (subquery)
                            prev_token = token.find_token("(").previous_token
                            with_names.append(prev_token.left_expanded)
                        else:
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
        """
        return Generalizator(self._raw_query).comments

    @property
    def without_comments(self) -> str:
        """
        Removes comments from SQL query
        """
        return Generalizator(self._raw_query).without_comments

    @property
    def generalize(self) -> Optional[str]:
        """
        Removes most variables from an SQL query
        and replaces them with X or N for numbers.

        Based on Mediawiki's DatabaseBase::generalizeSQL
        """
        return Generalizator(self._raw_query).generalize

    def _add_to_columns_subsection(self, keyword: str, column: str):
        section = COLUMNS_SECTIONS[keyword]
        self._columns_dict = self._columns_dict or dict()
        self._columns_dict.setdefault(section, UniqueList()).append(column)

    def _add_to_columns_aliases_subsection(self, keyword: str, alias: str):
        section = COLUMNS_SECTIONS[keyword]
        self._columns_aliases_dict = self._columns_aliases_dict or dict()
        self._columns_aliases_dict.setdefault(section, UniqueList()).append(alias)

    def _resolve_column_alias(self, alias: str) -> str:
        """
        Returns a column name for a given alias
        """
        while alias in self.columns_aliases:
            alias = self.columns_aliases[alias]
        return alias

    def _resolve_alias_to_column(self, alias_token: SQLToken) -> str:
        if alias_token.left_expanded in self._columns_with_tables_aliases:
            alias_of = self._columns_with_tables_aliases[alias_token.left_expanded]
        else:
            alias_of = alias_token.left_expanded
        return alias_of

    def _preprocess_query(self) -> str:
        """
        Perform initial query cleanup
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
