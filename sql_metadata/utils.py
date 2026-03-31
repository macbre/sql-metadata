"""Utility classes and functions shared across the sql-metadata package.

Provides ``UniqueList``, a deduplicating list used to collect columns,
tables, aliases, and CTE names while preserving insertion order, and
``flatten_list`` for normalising nested alias resolution results.
"""

from typing import Any, Dict, Iterable, List

#: Placeholder used to encode dots in qualified CTE names so that sqlglot
#: does not misinterpret ``db.cte_name`` as a table reference.
DOT_PLACEHOLDER = "__DOT__"


class UniqueList(list):
    """A list subclass that silently rejects duplicate items.

    Used throughout the extraction pipeline (``_extract.py``, ``parser.py``)
    to collect columns, tables, aliases, CTE names, and subquery names while
    guaranteeing uniqueness and preserving first-insertion order.  Maintains
    an internal ``set`` for O(1) membership checks.
    """

    def __init__(self, iterable: Any = None, **kwargs: Any) -> None:
        self._seen: set = set()
        if iterable is not None:
            super().__init__(**kwargs)
            self.extend(iterable)
        else:
            super().__init__(**kwargs)
            self._seen = set(self)

    def append(self, item: Any) -> None:
        """Append *item* only if it is not already present (O(1) check)."""
        if item not in self._seen:
            self._seen.add(item)
            super().append(item)

    def extend(self, items: Iterable[Any]) -> None:  # type: ignore[override]
        """Extend the list with *items*, skipping duplicates."""
        for item in items:
            self.append(item)

    def __contains__(self, item: Any) -> bool:
        """O(1) membership check using the internal set."""
        return item in self._seen

    def __sub__(self, other: Any) -> List:
        """Return a plain list of elements in *self* that are not in *other*."""
        other_set = set(other)
        return [x for x in self if x not in other_set]


def _make_reverse_cte_map(cte_name_map: Dict) -> Dict[str, str]:
    """Build reverse mapping from placeholder CTE names to originals."""
    reverse = {v.replace(".", DOT_PLACEHOLDER): v for v in cte_name_map.values()}
    reverse.update(cte_name_map)
    return reverse


def last_segment(name: str) -> str:
    """Return the last dot-separated segment of a qualified name."""
    return name.rsplit(".", 1)[-1]


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
