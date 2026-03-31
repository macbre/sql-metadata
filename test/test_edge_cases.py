"""Edge-case tests for internals not covered by feature-specific test files."""

from sql_metadata.sql_cleaner import SqlCleaner
from sql_metadata.utils import UniqueList


def test_unique_list_subtraction():
    """UniqueList.__sub__ returns elements not present in the other list."""
    ul = UniqueList(["a", "b", "c", "d"])
    result = ul - ["b", "d"]
    assert result == ["a", "c"]


def test_unique_list_deduplicates_on_init():
    """UniqueList removes duplicates when constructed from an iterable."""
    ul = UniqueList(["x", "y", "x", "z", "y"])
    assert list(ul) == ["x", "y", "z"]


def test_clean_empty_after_paren_strip():
    """SQL that becomes empty after outer-paren stripping."""
    result = SqlCleaner.clean("(())")
    assert result.sql is None
