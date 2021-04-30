"""
Module with various utils
"""
from typing import Any, List


class UniqueList(list):
    """
    List that keeps it's items unique
    """

    def append(self, item: Any) -> None:
        if item not in self:
            super().append(item)

    def __sub__(self, other) -> List:
        return [x for x in self if x not in other]
