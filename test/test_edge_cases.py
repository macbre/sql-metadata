"""Edge-case tests for internal utilities.

These tests exercise code paths (depth guards, degenerate inputs) that
are difficult or impossible to trigger through the public Parser API.
They test internal symbols directly and may need updating if those
internals are refactored.
"""

from sql_metadata import Parser
from sql_metadata.sql_cleaner import SqlCleaner, _strip_outer_parens
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


def test_extract_comments_unterminated_block_comment():
    """Unterminated block comment causes tokenizer failure — returns []."""
    parser = Parser("/*")
    assert parser.comments == []


def test_strip_comments_unterminated_block_comment():
    """Unterminated block comment in strip_comments returns input stripped."""
    parser = Parser("/*")
    assert parser.without_comments == "/*"


def test_clean_empty_after_paren_strip():
    """SQL that becomes empty after outer-paren stripping."""
    result = SqlCleaner.clean("(())")
    assert result.sql is None


def test_strip_outer_parens_depth_guard():
    """Deeply nested parentheses hit the depth guard instead of stack overflow."""
    deep = "(" * 150 + "SELECT 1" + ")" * 150
    result = _strip_outer_parens(deep)
    # Depth guard stops at 100 — some parens remain
    assert "SELECT 1" in result
    assert result.startswith("(")
