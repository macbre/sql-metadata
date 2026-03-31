"""Utility classes and functions shared across the sql-metadata package.

Provides ``UniqueList``, a deduplicating list used to collect columns,
tables, aliases, and CTE names while preserving insertion order, and
``flatten_list`` for normalising nested alias resolution results.
"""

from typing import Any, Dict, List, Sequence


class UniqueList(list):
    """A list subclass that silently rejects duplicate items.

    Used throughout the extraction pipeline (``_extract.py``, ``parser.py``)
    to collect columns, tables, aliases, CTE names, and subquery names while
    guaranteeing uniqueness and preserving first-insertion order.  Maintains
    an internal ``set`` for O(1) membership checks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen: set = set(self)

    def append(self, item: Any) -> None:
        """Append *item* only if it is not already present (O(1) check)."""
        if item not in self._seen:
            self._seen.add(item)
            super().append(item)

    def extend(self, items: Sequence[Any]) -> None:
        """Extend the list with *items*, skipping duplicates."""
        for item in items:
            self.append(item)

    def __sub__(self, other) -> List:
        """Return a plain list of elements in *self* that are not in *other*."""
        other_set = set(other)
        return [x for x in self if x not in other_set]


def _make_reverse_cte_map(cte_name_map: Dict) -> Dict[str, str]:
    """Build reverse mapping from placeholder CTE names to originals."""
    reverse = {v.replace(".", "__DOT__"): v for v in cte_name_map.values()}
    reverse.update(cte_name_map)
    return reverse


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
