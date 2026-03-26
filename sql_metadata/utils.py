"""Utility classes and functions shared across the sql-metadata package.

Provides ``UniqueList``, a deduplicating list used to collect columns,
tables, aliases, and CTE names while preserving insertion order, and
``flatten_list`` for normalising nested alias resolution results.
"""

from typing import Any, List, Sequence


class UniqueList(list):
    """A list subclass that silently rejects duplicate items.

    Used throughout the extraction pipeline (``_extract.py``, ``parser.py``)
    to collect columns, tables, aliases, CTE names, and subquery names while
    guaranteeing uniqueness and preserving first-insertion order.  This avoids
    the need for a separate ``set`` plus an ordered container.

    Inherits from :class:`list` so it is JSON-serialisable and supports
    indexing, but overrides :meth:`append` and :meth:`extend` to enforce the
    uniqueness invariant.
    """

    def append(self, item: Any) -> None:
        """Append *item* only if it is not already present.

        :param item: The value to append.
        :type item: Any
        :returns: Nothing.
        :rtype: None
        """
        if item not in self:
            super().append(item)

    def extend(self, items: Sequence[Any]) -> None:
        """Extend the list with *items*, skipping duplicates.

        Delegates to :meth:`append` for each element so the uniqueness
        invariant is maintained.

        :param items: Iterable of values to add.
        :type items: Sequence[Any]
        :returns: Nothing.
        :rtype: None
        """
        for item in items:
            self.append(item)

    def __sub__(self, other) -> List:
        """Return a plain list of elements in *self* that are not in *other*.

        Used by the parser to subtract known alias names or CTE names from
        a collected column list.

        :param other: Collection of items to exclude.
        :type other: list
        :returns: Filtered list (not a ``UniqueList``).
        :rtype: List
        """
        return [x for x in self if x not in other]


def flatten_list(input_list: List) -> List[str]:
    """Recursively flatten a list that may contain nested lists.

    Created to normalise the output of alias resolution in
    :meth:`Parser._resolve_nested_query`, where a single alias can map
    to either a string or a list of strings (multi-column aliases).

    :param input_list: A list whose elements are strings or nested lists.
    :type input_list: List
    :returns: A flat list of strings.
    :rtype: List[str]
    """
    result = []
    for item in input_list:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(item)
    return result
