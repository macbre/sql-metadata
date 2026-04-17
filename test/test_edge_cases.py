"""Edge-case tests exercised through the public :class:`Parser` API.

These tests cover degenerate inputs (empty SQL after paren stripping,
unterminated comments, deeply nested parentheses) by feeding them into
``Parser`` and asserting on its public properties — no internal helpers
are imported.
"""

from sql_metadata import Parser
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


def test_extract_comments_unterminated_block_comment():
    """Unterminated block comment causes tokenizer failure — returns []."""
    parser = Parser("/*")
    assert parser.comments == []


def test_strip_comments_unterminated_block_comment():
    """Unterminated block comment in strip_comments returns input stripped."""
    parser = Parser("/*")
    assert parser.without_comments == "/*"


def test_preprocess_query_unterminated_block_comment():
    """Tokenizer failure on Parser.query falls back to whitespace collapse."""
    # Exercises the TokenError branch in SqlCleaner.preprocess_query.
    assert Parser("/*").query == "/*"
    assert Parser("  /*\n  ").query == "/*"


def test_clean_empty_after_paren_strip():
    """SQL that becomes empty after outer-paren stripping."""
    result = SqlCleaner.clean("(())")
    assert result.sql is None


def test_strip_outer_parens_depth_guard():
    """Deeply nested parentheses don't stack-overflow the cleaner's recursion."""
    # 150 levels exceeds the 100-deep recursion guard in _strip_outer_parens;
    # parsing through Parser must return gracefully rather than raise
    # RecursionError.
    parser = Parser("(" * 150 + "SELECT 1" + ")" * 150)
    assert parser.columns == []
