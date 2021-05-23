from sql_metadata import Parser
from sql_metadata import QueryType


def test_redshift():
    parser = Parser("ALTER TABLE target_table APPEND FROM source_table")
    assert parser.tables == [
        "target_table",
        "source_table",
    ]
    assert parser.query_type == QueryType.ALTER
    assert Parser("ALTER TABLE x APPEND FROM y").tables == ["x", "y"]
