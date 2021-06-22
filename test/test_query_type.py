import pytest

from sql_metadata import Parser


def test_insert_query():
    queries = [
        "INSERT IGNORE /* foo */ INTO bar VALUES (1, '123', '2017-01-01');",
        "/* foo */ INSERT IGNORE INTO bar VALUES (1, '123', '2017-01-01');"
        "-- foo\nINSERT IGNORE INTO bar VALUES (1, '123', '2017-01-01');"
        "# foo\nINSERT IGNORE INTO bar VALUES (1, '123', '2017-01-01');",
    ]

    for query in queries:
        assert "INSERT" == Parser(query).query_type


def test_select_query():
    queries = [
        "SELECT /* foo */ foo FROM bar",
        "/* foo */ SELECT foo FROM bar"
        "-- foo\nSELECT foo FROM bar"
        "# foo\nSELECT foo FROM bar",
    ]

    for query in queries:
        assert "SELECT" == Parser(query).query_type


def test_unsupported_query():
    queries = [
        "FOO BAR",
        "DO SOMETHING",
    ]

    for query in queries:
        with pytest.raises(ValueError) as ex:
            _ = Parser(query).query_type

        assert "Not supported query type!" in str(ex.value)


def test_empty_query():
    queries = ["", "/* empty query */"]

    for query in queries:
        with pytest.raises(ValueError) as ex:
            _ = Parser(query).query_type

        assert "Empty queries are not supported!" in str(ex.value)
