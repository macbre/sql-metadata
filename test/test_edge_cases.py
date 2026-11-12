"""Edge-case tests exercised through the public :class:`Parser` API.

These tests cover degenerate inputs (empty SQL after paren stripping,
unterminated comments, deeply nested parentheses) by feeding them into
``Parser`` and asserting on its public properties — no internal helpers
are imported.
"""

import pytest

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


def test_strip_outer_parens_unbalanced_middle():
    """Queries that look paren-wrapped but aren't (UNION of parenthesised SELECTs)."""
    # "(SELECT ...) UNION (SELECT ...)" starts with "(" and ends with ")" but the
    # inner parens go negative — _is_wrapped must short-circuit and leave the SQL
    # intact so both SELECT branches parse.
    parser = Parser("(SELECT a FROM t1) UNION (SELECT b FROM t2)")
    assert parser.tables == ["t1", "t2"]


@pytest.mark.parametrize("depth", [1, 2])
@pytest.mark.parametrize("literal", ["')'", "'left)right'", "'it'')s'", "'('"])
@pytest.mark.parametrize(
    "template",
    [
        "UPDATE target SET value = {literal} WHERE id = 1",
        "INSERT INTO target (value) VALUES ({literal})",
        "DELETE FROM target WHERE value = {literal}",
    ],
)
def test_outer_parens_ignore_quoted_parentheses(
    depth: int, literal: str, template: str
) -> None:
    query = template.format(literal=literal)
    wrapped = "(" * depth + query + ")" * depth
    assert SqlCleaner.clean(wrapped).sql == query
    expected = Parser(query)
    actual = Parser(wrapped)
    assert actual.query_type == expected.query_type
    assert actual.tables == expected.tables
    assert actual.columns == expected.columns


def test_outer_parens_preserve_unterminated_literal() -> None:
    query = "((UPDATE target SET value = 'unterminated))"
    assert SqlCleaner.clean(query).sql == query
    with pytest.raises(ValueError):
        Parser(query).tables


@pytest.mark.parametrize("name", ["we're", 'say"hello', "a)b"])
def test_outer_parens_preserve_mysql_quoted_names(name: str) -> None:
    query = f"UPDATE `{name}` SET value = 1"
    expected = Parser(query)
    actual = Parser(f"(({query}))")
    assert actual.tables == expected.tables


@pytest.mark.parametrize("name", ["we're", 'say"hello', "a)b"])
def test_outer_parens_preserve_tsql_quoted_names(name: str) -> None:
    query = f"UPDATE [{name}] SET value = 1"
    expected = Parser(query)
    actual = Parser(f"(({query}))")
    assert actual.tables == expected.tables
