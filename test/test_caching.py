from sql_metadata import Parser


def test_cleared_cache():
    parser = Parser("Select * from test")
    assert parser.tables == ["test"]

    parser.query = "Select * from test2"
    assert parser._tables is None
