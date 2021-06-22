"""
Module contains internal SQLToken that creates linked list
"""
from typing import Dict, List, Union

import sqlparse.sql
from sqlparse.tokens import Comment, Name, Number, Punctuation, Wildcard

from sql_metadata.keywords_lists import RELEVANT_KEYWORDS


class SQLToken:  # pylint: disable=R0902
    """
    Class representing single token and connected into linked list
    """

    def __init__(
        self,
        tok: sqlparse.sql.Token = None,
        index: int = -1,
        subquery_level: int = 0,
        last_keyword: str = None,
    ):
        self.position = index
        if tok is None:
            self._set_default_values()
        else:
            self.value = tok.value.strip("`").strip('"')
            self.is_keyword = tok.is_keyword or (
                tok.ttype.parent is Name and tok.ttype is not Name
            )
            self.is_name = tok.ttype is Name
            self.is_punctuation = tok.ttype is Punctuation
            self.is_dot = str(tok) == "."
            self.is_wildcard = tok.ttype is Wildcard
            self.is_integer = tok.ttype is Number.Integer
            self.is_float = tok.ttype is Number.Float
            self.is_comment = tok.ttype is Comment or tok.ttype.parent == Comment

            self.is_left_parenthesis = str(tok) == "("
            self.is_right_parenthesis = str(tok) == ")"
            self.last_keyword = last_keyword
            self.next_token = EmptyToken
            self.previous_token = EmptyToken
            self.subquery_level = subquery_level

        self._set_default_parenthesis_status()

    def _set_default_values(self):
        self.value = ""
        self.is_keyword = False
        self.is_name = False
        self.is_punctuation = False
        self.is_dot = False
        self.is_wildcard = False
        self.is_integer = False
        self.is_float = False
        self.is_comment = False

        self.is_left_parenthesis = False
        self.is_right_parenthesis = False
        self.last_keyword = None
        self.subquery_level = 0
        self.next_token = None
        self.previous_token = None

    def _set_default_parenthesis_status(self):
        self.is_in_nested_function = False
        self.parenthesis_level = 0
        self.is_subquery_start = False
        self.is_subquery_end = False
        self.is_with_query_start = False
        self.is_with_query_end = False
        self.is_with_columns_start = False
        self.is_with_columns_end = False
        self.is_nested_function_start = False
        self.is_nested_function_end = False
        self.is_column_definition_start = False
        self.is_column_definition_end = False
        self.is_create_table_columns_declaration_start = False
        self.is_create_table_columns_declaration_end = False

    def __str__(self):
        """
        String representation
        """
        return self.value.strip('"')

    def __repr__(self) -> str:  # pragma: no cover
        """
        Representation - useful for debugging
        """
        repr_str = ["=".join([str(k), str(v)]) for k, v in self.__dict__.items()]
        return f"SQLToken({','.join(repr_str)})"

    @property
    def normalized(self) -> str:
        """
        Property returning uppercase value without end lines and spaces
        """
        return self.value.translate(str.maketrans("", "", " \n\t\r")).upper()

    @property
    def stringified_token(self) -> str:
        """
        Returns string representation with whitespace or not - used to rebuild query
        from list of tokens
        """
        if self.previous_token:
            if (
                self.normalized in [")", ".", ","]
                or self.previous_token.normalized in ["(", "."]
                or (
                    self.is_left_parenthesis
                    and self.previous_token.normalized
                    not in RELEVANT_KEYWORDS.union({"*", ",", "IN", "NOTIN"})
                )
            ):
                return str(self)
            return f" {self}"
        return str(self)  # pragma: no cover

    @property
    def last_keyword_normalized(self) -> str:
        """
        Property returning uppercase last keyword without end lines and spaces
        """
        if self.last_keyword:
            return self.last_keyword.translate(str.maketrans("", "", " \n\t\r")).upper()
        return ""

    @property
    def left_expanded(self) -> str:
        """
        Property tries to expand value with dot notation if left token is a dot
        to capture whole groups like <SCHEMA>.<TABLE> or <DATABASE>.<SCHEMA>.<TABLE>
        """
        value = str(self)
        token = self
        while token.previous_token and token.previous_token.is_dot:
            if token.get_nth_previous(2) and token.get_nth_previous(2).is_name:
                value = f"{token.get_nth_previous(2)}." + value
            token = token.get_nth_previous(2)
        return value.strip("`")

    @property
    def is_in_parenthesis(self) -> bool:
        """
        Property checks if token is surrounded with brackets ()
        """
        return self.parenthesis_level > 0

    @property
    def is_create_table_columns_definition(self) -> bool:
        """
        Checks if given token is inside columns definition in
        create table query like: create table name (<columns def>)
        """
        open_parenthesis = self.find_nearest_token(
            True, value_attribute="is_create_table_columns_declaration_start"
        )
        if open_parenthesis is EmptyToken:
            return False
        close_parenthesis = self.find_nearest_token(
            True,
            direction="right",
            value_attribute="is_create_table_columns_declaration_end",
        )
        return (
            open_parenthesis is not EmptyToken and close_parenthesis is not EmptyToken
        )

    @property
    def is_keyword_column_name(self) -> bool:
        """
        Checks if given keyword can be a column name in SELECT query
        """
        return (
            self.is_keyword
            and self.normalized not in RELEVANT_KEYWORDS
            and self.previous_token.normalized in [",", "SELECT"]
            and self.next_token.normalized in [",", "AS"]
        )

    @property
    def is_alias_without_as(self) -> bool:
        """
        Checks if a given token is an alias without as keyword,
        like: SELECT col <alias1>, col2 <alias2> from table
        """
        return (
            self.next_token.normalized in [",", "FROM"]
            and self.previous_token.normalized not in [",", ".", "(", "SELECT"]
            and (
                self.last_keyword_normalized == "SELECT"
                or self.previous_token.is_column_definition_end
            )
            and not self.previous_token.is_comment
        )

    @property
    def is_alias_definition(self):
        """
        Returns if current token is a definition of an alias.
        Note that aliases can also be used in other queries and be a part
        of other nested columns with aliases.

        Note that this function only check if alias token is a token with
        alias definition, it's not suitable for determining IF token is an alias
        as it's more complicated and this method would match
        also i.e. sub-queries names
        """
        return (
            self.is_alias_without_as
            or self.previous_token.normalized == "AS"
            or self.is_in_with_columns
        )

    @property
    def is_alias_of_self(self) -> bool:
        """
        Checks if a given token is an alias but at the same time
        is also an alias of self, so not really an alias
        """

        end_of_column = self.find_nearest_token(
            [",", "FROM"], value_attribute="normalized", direction="right"
        )
        while end_of_column.is_in_nested_function:
            end_of_column = end_of_column.find_nearest_token(
                [",", "FROM"], value_attribute="normalized", direction="right"
            )
        return end_of_column.previous_token.normalized == self.normalized

    @property
    def is_in_with_columns(self) -> bool:
        """
        Checks if token is inside with colums part of a query
        """
        return (
            self.find_nearest_token("(").is_with_columns_start
            and self.find_nearest_token(")", direction="right").is_with_columns_end
        )

    @property
    def is_wildcard_not_operator(self):
        """
        Determines if * encountered in query is a wildcard like select <*> from aa
        or is that an operator like Select aa <*> bb as cc from dd
        """
        return self.normalized == "*" and (
            self.previous_token.value in [",", ".", "SELECT"]
            or (self.previous_token.value == "(")
            and self.next_token.value == ")"
        )

    def token_is_alias_of_self_not_from_subquery(self, aliases_levels: Dict) -> bool:
        """
        Checks if token is also an alias, but is an alias of self that is not
        coming from a subquery, that means it's a valid column
        """
        return (
            self.last_keyword_normalized == "SELECT"
            and self.is_alias_of_self
            and self.subquery_level == aliases_levels[self.value]
        )

    def table_prefixed_column(self, table_aliases: Dict) -> str:
        """
        Substitutes table alias with actual table name
        """
        value = self.left_expanded
        if "." in value:
            parts = value.split(".")
            if len(parts) > 2:  # pragma: no cover
                raise ValueError(f"Wrong columns name: {value}")
            parts[0] = table_aliases.get(parts[0], parts[0])
            value = ".".join(parts)
        return value

    def get_nth_previous(self, level: int) -> "SQLToken":
        """
        Function iterates previous tokens getting nth previous token
        """
        assert level >= 1
        if self.previous_token:
            if level > 1:
                return self.previous_token.get_nth_previous(level=level - 1)
            return self.previous_token
        return EmptyToken  # pragma: no cover

    def find_nearest_token(
        self,
        value: Union[Union[str, bool], List[Union[str, bool]]],
        direction: str = "left",
        value_attribute: str = "value",
    ) -> "SQLToken":
        """
        Returns token with given value to the left or right.
        If value is not found it returns EmptyToken.
        """
        if not isinstance(value, list):
            value = [value]
        attribute = "previous_token" if direction == "left" else "next_token"
        token = self
        while getattr(token, attribute):
            tok_value = getattr(getattr(token, attribute), value_attribute)
            if tok_value in value:
                return getattr(token, attribute)
            token = getattr(token, attribute)
        return EmptyToken


EmptyToken = SQLToken()
