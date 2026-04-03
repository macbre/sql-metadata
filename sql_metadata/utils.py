"""Utility classes and functions shared across the sql-metadata package.

Provides ``UniqueList``, a deduplicating list used to collect columns,
tables, aliases, and CTE names while preserving insertion order, and
a ``last_segment`` helper for qualified name handling.
"""

from typing import Any, Iterable

#: Placeholder used to encode dots in qualified CTE names so that sqlglot
#: does not misinterpret ``db.cte_name`` as a table reference.
DOT_PLACEHOLDER = "__DOT__"


class UniqueList(list[str]):
    """A list subclass that silently rejects duplicate items.

    Used throughout the extraction pipeline (``_extract.py``, ``parser.py``)
    to collect columns, tables, aliases, CTE names, and subquery names while
    guaranteeing uniqueness and preserving first-insertion order.  Maintains
    an internal ``set`` for O(1) membership checks.
    """

    def __init__(self, iterable: Any = None, **kwargs: Any) -> None:
        self._seen: set[str] = set()
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

    def __sub__(self, other: Any) -> list[str]:
        """Return a plain list of elements in *self* that are not in *other*."""
        other_set = set(other)
        return [x for x in self if x not in other_set]



def last_segment(name: str) -> str:
    """Return the last dot-separated segment of a qualified name."""
    return name.rsplit(".", 1)[-1]


