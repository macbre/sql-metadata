from sql_metadata.parser import Parser


def test_redshift():
    parser = Parser("ALTER TABLE target_table APPEND FROM source_table")
    assert parser.tables == [
        "target_table",
        "source_table",
    ]
    assert parser.query_type == "Alter"
    assert Parser("ALTER TABLE x APPEND FROM y").tables == ["x", "y"]
