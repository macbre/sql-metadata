from sql_metadata import Parser


def test_fully_qualified_select_and_condition():
    query = """
    select dbo.a.col, b.col from dbo.a, db_two.dbo.b
    where dbo.a.col = b.col
    """

    parser = Parser(query)
    assert parser.columns == ["dbo.a.col", "b.col"]
    assert parser.tables == ["dbo.a", "db_two.dbo.b"]


def test_fully_qualified_with_db_name():
    query = """
    select dbo.a.col, db_two.dbo.b.col as b_col from dbo.a, db_two.dbo.b
    where dbo.a.col = db_two.dbo.b.col
    """

    parser = Parser(query)
    assert parser.columns == ["dbo.a.col", "db_two.dbo.b.col"]
    assert parser.tables == ["dbo.a", "db_two.dbo.b"]
    assert parser.columns_aliases == {"b_col": "db_two.dbo.b.col"}
