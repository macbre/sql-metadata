import pytest

from sql_metadata import Parser


@pytest.mark.parametrize("literal", ["'O''Reilly'", "'a''b''c'", "''''"])
@pytest.mark.parametrize(
    "prefix",
    ["SELECT ", "SELECT N", "SELECT n", "SELECT name FROM authors WHERE name LIKE "],
)
def test_generalize_doubled_quotes_as_one_literal(literal: str, prefix: str) -> None:
    assert Parser(prefix + literal).generalize == prefix + "X"


def test_generalize_separate_literals() -> None:
    assert Parser("SELECT 'O''Reilly', '', 'ordinary'").generalize == "SELECT X, X, X"


def test_generalize_unterminated_literal_remains_best_effort() -> None:
    assert Parser("SELECT 'unterminated").generalize == "SELECT 'unterminated"
