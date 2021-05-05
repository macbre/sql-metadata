"""
Module contains internal SQLToken that creates linked list
"""
import dataclasses
from typing import Dict, List, Optional, Union

from sql_metadata.keywords_lists import FUNCTIONS_IGNORED


@dataclasses.dataclass
class SQLToken:  # pylint: disable=R0902
    """
    Class representing single token and connected into linked list
    """

    value: Optional[str]
    is_keyword: bool
    is_name: bool
    is_dot: bool
    is_punctuation: bool
    is_wildcard: bool
    is_integer: bool
    is_float: bool

    is_left_parenthesis: bool
    is_right_parenthesis: bool
    subquery_level: int
    position: int

    is_subquery_start: bool = False
    is_subquery_end: bool = False
    is_nested_function_start: bool = False
    is_nested_function_end: bool = False
    is_column_definition_start: bool = False
    is_column_definition_end: bool = False

    last_keyword: Optional[str] = None
    previous_token: Optional["SQLToken"] = None
    next_token: Optional["SQLToken"] = None

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

    # def __bool__(self) -> bool:
    #     """
    #     Checks if token is not an EmptyToken
    #     """
    #     return self.value != ""

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
                    and self.previous_token.normalized in FUNCTIONS_IGNORED
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
        left_parenthesis = self.find_token("(")
        right_parenthesis = self.find_token(")", direction="right")
        return left_parenthesis.value != "" and right_parenthesis.value != ""

    def table_prefixed_column(self, table_aliases: Dict) -> str:
        """
        Substitutes table alias with actual table name
        """
        value = self.left_expanded
        if "." in value:
            parts = value.split(".")
            if len(parts) > 3:  # pragma: no cover
                raise ValueError(f"Wrong columns name: {value}")
            parts[0] = table_aliases.get(parts[0], parts[0])
            value = ".".join(parts)
        return value

    def get_nth_previous(self, level: int):
        """
        Function iterates previous tokens getting nth previous token
        """
        assert level >= 1
        if self.previous_token:
            if level > 1:
                return self.previous_token.get_nth_previous(level=level - 1)
            return self.previous_token
        return None  # pragma: no cover

    def find_token(
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


EmptyToken = SQLToken(
    value="",
    is_keyword=False,
    is_name=False,
    is_punctuation=False,
    is_dot=False,
    is_wildcard=False,
    is_integer=False,
    is_float=False,
    is_left_parenthesis=False,
    is_right_parenthesis=False,
    last_keyword=None,
    subquery_level=0,
    position=-1,
)
