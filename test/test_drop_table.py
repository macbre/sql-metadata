from sql_metadata import Parser
from sql_metadata.keywords_lists import QueryType


def test_drop_table():
    parser = Parser("DROP TABLE foo")
    assert parser.query_type == QueryType.DROP
    assert parser.tables == ["foo"]
    assert parser.columns == []
