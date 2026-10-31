from sql_metadata import Parser
from sql_metadata.keywords_lists import QueryType


def test_drop_table():
    parser = Parser("DROP TABLE foo")
    assert parser.query_type == QueryType.DROP
    assert parser.tables == ["foo"]
    assert parser.columns == []


def test_drop_index_excludes_index_name_from_tables():
    assert Parser("DROP INDEX idx ON users").tables == ["users"]
    assert Parser("DROP INDEX IF EXISTS idx").tables == []
