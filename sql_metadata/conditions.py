"""
Gathers conditions used to parse tokens into specified qualifiers
"""
from typing import List, TYPE_CHECKING

import sql_metadata

if TYPE_CHECKING:
    from sql_metadata import Parser
    from sql_metadata.token import SQLToken


# Tables related
def is_potential_table_name(token: "SQLToken") -> bool:
    return (
        (token.is_name or token.is_keyword)
        and token.last_keyword_normalized
        in sql_metadata.keywords_lists.TABLE_ADJUSTMENT_KEYWORDS
        and token.previous_token.normalized not in ["AS", "WITH"]
        and token.normalized not in ["AS", "SELECT"]
    )


def is_constraint_definition_inside_create_table_clause(
    parser: "Parser", token: "SQLToken"
) -> bool:
    # handle CREATE TABLE queries (#35)
    # skip keyword that are withing parenthesis-wrapped list of column
    return (
        parser.query_type == sql_metadata.QueryType.CREATE
        and token.is_in_parenthesis
        and token.is_create_table_columns_definition
    )


def is_with_statement_nested_in_subquery(token: "SQLToken") -> bool:
    return (
        token.normalized == "WITH"
        and token.previous_token.is_left_parenthesis
        and token.get_nth_previous(2).normalized == "FROM"
    )


def is_columns_alias_of_with_query_or_column_in_insert_query(
    token: "SQLToken", with_names: List[str]
) -> bool:
    # we are in <columns> of INSERT INTO <TABLE> (<columns>)
    # or columns of with statement: with (<columns>) as ...
    return token.is_in_parenthesis and (
        token.find_nearest_token("(").previous_token.value in with_names
        or token.last_keyword_normalized == "INTO"
    )


def is_alias_of_table_or_alias_of_subquery(token: "SQLToken") -> bool:
    # it's not a list of tables, e.g. SELECT * FROM foo, bar
    # hence, it can be the case of alias without AS,
    # e.g. SELECT * FROM foo bar
    # or an alias of subquery (SELECT * FROM foo) bar
    return (
        token.previous_token.normalized != token.last_keyword_normalized
        and not token.previous_token.is_punctuation
    ) or token.previous_token.is_right_parenthesis


# Columns related
def is_a_wildcard_in_select_statement(token: "SQLToken") -> bool:
    # handle * wildcard in select part, but ignore count(*)
    return (
        token.is_wildcard
        and token.last_keyword_normalized == "SELECT"
        and not token.previous_token.is_left_parenthesis
    )


def is_sub_query_alias(token: "SQLToken", subqueries_names: List[str]) -> bool:
    # aliases of sub-queries i.e.: SELECT from (...) <alias>
    return token.previous_token.is_right_parenthesis and token.value in subqueries_names


def is_with_query_name(token: "SQLToken", with_names: List[str]) -> bool:
    # names of the with queries <name> as (subquery)
    return token.next_token.normalized == "AS" and token.value in with_names


def is_column_definition_inside_create_table(
    parser: "Parser", token: "SQLToken"
) -> bool:
    # previous token is either ( or , -> indicates the column name
    return (
        parser.query_type == sql_metadata.QueryType.CREATE
        and token.is_in_parenthesis
        and token.previous_token.is_punctuation
        and token.last_keyword_normalized == "TABLE"
    )


def is_table_definition_suffix_in_non_select_create_table(
    parser: "Parser", token: "SQLToken"
) -> bool:
    # we're in CREATE TABLE query with the columns
    # ignore annotations outside the parenthesis with the list of columns
    # e.g. ) CHARACTER SET utf8;
    return (
        parser.query_type == sql_metadata.QueryType.CREATE
        and not token.is_in_parenthesis
        and token.find_nearest_token("SELECT", value_attribute="normalized")
        is sql_metadata.token.EmptyToken
    )


def is_potential_column_name(token: "SQLToken") -> bool:
    return (
        token.last_keyword_normalized
        in sql_metadata.keywords_lists.KEYWORDS_BEFORE_COLUMNS
        and token.previous_token.normalized not in ["AS", ")"]
        and not token.is_alias_without_as
    )


def is_not_an_alias_or_is_self_alias_outside_of_subquery(
    parser: "Parser", token: "SQLToken"
) -> bool:
    return (
        token.value not in parser.columns_aliases_names
        or token.token_is_alias_of_self_not_from_subquery(
            aliases_levels=parser._column_aliases_max_subquery_level
        )
    )


def is_column_name_inside_insert_clause(token: "SQLToken") -> bool:
    # INSERT INTO `foo` (col1, `col2`) VALUES (..)
    return (
        token.last_keyword_normalized == "INTO" and token.previous_token.is_punctuation
    )


def is_sub_query_name_or_with_name_or_function_name(
    token: "SQLToken", sub_queries_names: List[str], with_names: List[str]
) -> bool:
    return (
        is_sub_query_alias(token=token, subqueries_names=sub_queries_names)
        or is_with_query_name(token=token, with_names=with_names)
        or token.next_token.is_left_parenthesis
    )
