from sql_metadata.parser import Parser


def test_redshift():
    assert Parser("ALTER TABLE target_table APPEND FROM source_table").tables == [
        "target_table",
        "source_table",
    ]
    assert Parser("ALTER TABLE x APPEND FROM y").tables == ["x", "y"]
