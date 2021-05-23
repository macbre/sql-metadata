from sqlparse.tokens import Punctuation, Wildcard

from sql_metadata.compat import (
    get_query_columns,
    get_query_tables,
    get_query_limit_and_offset,
    generalize_sql,
    preprocess_query,
    get_query_tokens,
)


def test_get_query_columns():
    assert ["*"] == get_query_columns("SELECT * FROM `test_table`")
    assert ["foo", "id"] == get_query_columns(
        "SELECT foo, count(*) as bar FROM `test_table` WHERE id = 3"
    )


def test_get_query_tables():
    assert ["test_table"] == get_query_tables("SELECT * FROM `test_table`")
    assert ["test_table", "second_table"] == get_query_tables(
        "SELECT foo FROM test_table, second_table WHERE id = 1"
    )


def test_get_query_limit_and_offset():
    assert (200, 927600) == get_query_limit_and_offset(
        "SELECT * FOO foo LIMIT 927600,200"
    )


def test_generalize_sql():
    assert generalize_sql() is None
    assert "SELECT * FROM foo;" == generalize_sql("SELECT * FROM foo;")
    assert "SELECT * FROM foo WHERE id = N" == generalize_sql(
        "SELECT * FROM foo WHERE id = 123"
    )
    assert "SELECT test FROM foo" == generalize_sql("SELECT /* foo */ test FROM foo")


def test_preprocess_query():
    assert "SELECT * FROM foo WHERE id = 123" == preprocess_query(
        "SELECT * FROM foo WHERE id = 123"
    )
    assert "SELECT /* foo */ test FROM `foo`.`bar`" == preprocess_query(
        "SELECT /* foo */ test\nFROM `foo`.`bar`"
    )


def test_get_query_tokens():
    tokens = get_query_tokens("SELECT * FROM foo;")
    assert len(tokens) == 5

    assert tokens[0].normalized == "SELECT"
    assert tokens[1].ttype is Wildcard
    assert tokens[2].normalized == "FROM"
    assert tokens[3].normalized == "foo"
    assert tokens[4].ttype is Punctuation

    assert [] == get_query_tokens("")
