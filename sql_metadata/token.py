"""
Module contains internal SQLToken that creates linked list
"""
import dataclasses
from typing import Optional


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
    is_left_parenthesis: bool
    is_right_parenthesis: bool

    # and the state
    last_keyword: Optional[str] = None
    previous_token: Optional["SQLToken"] = None
    next_token: Optional["SQLToken"] = None

    def __str__(self):
        """
        String representation
        """
        return self.value.strip('"')

    def __repr__(self):  # pragma: no cover
        """
        Representation
        """
        return self.__str__()

    @property
    def upper(self) -> Optional[str]:
        """
        Property returning uppercased value
        """
        return self.value.upper() if self.value else None

    @property
    def normalized(self):
        """
        Property returning uppercased value without end lines and spaces
        """
        return self.value.translate(str.maketrans("", "", " \n\t\r")).upper()

    @property
    def last_keyword_normalized(self) -> str:
        """
        Property returning uppercased last keayword without end lines and spaces
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
        while token.previous_token.is_dot:
            if token.get_nth_previous(2) and token.get_nth_previous(2).is_name:
                value = f"{token.get_nth_previous(2)}." + value
            token = token.get_nth_previous(2)
        return value

    @property
    def is_in_parenthesis(self) -> bool:
        """
        Property checks if token is surrounded with brackets ()
        """
        token = self
        left_parenthesis = False
        right_parenthesis = False
        while token.previous_token:
            if token.previous_token.is_left_parenthesis:
                left_parenthesis = True
                break
            token = token.previous_token
        token = self
        while token.next_token:
            if token.next_token.is_right_parenthesis:
                right_parenthesis = True
                break
            token = token.next_token

        return left_parenthesis and right_parenthesis

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


EmptyToken = SQLToken(
    value="",
    is_keyword=False,
    is_name=False,
    is_punctuation=False,
    is_dot=False,
    is_wildcard=False,
    is_integer=False,
    is_left_parenthesis=False,
    is_right_parenthesis=False,
    last_keyword=None,
)
