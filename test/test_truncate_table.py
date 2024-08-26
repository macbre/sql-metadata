from sql_metadata import Parser
from sql_metadata.keywords_lists import QueryType


def test_truncate_table():
    parser = Parser("TRUNCATE TABLE foo")
    assert parser.query_type == QueryType.TRUNCATE
    assert parser.tables == ["foo"]
    assert parser.columns == []
