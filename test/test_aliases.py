from sql_metadata.parser import Parser


def test_get_query_table_aliases():
    assert Parser("SELECT bar FROM foo").tables_aliases == {}
    assert Parser("SELECT bar FROM foo AS f").tables_aliases == {"f": "foo"}
    # assert Parser('SELECT bar FROM foo f') == {'f': 'foo'}
    assert Parser("SELECT bar AS value FROM foo AS f").tables_aliases == {"f": "foo"}
    assert Parser(
        "SELECT bar AS value FROM foo AS f INNER JOIN dimensions AS d ON f.id = d.id"
    ).tables_aliases == {"f": "foo", "d": "dimensions"}
    assert (
        Parser("SELECT e.foo FROM (SELECT * FROM bar) AS e").tables_aliases == {}
    ), "Sub-query aliases are ignored"
    assert Parser(
        "SELECT a.* FROM product_a AS a JOIN product_b AS b ON a.ip_address = b.ip_address"
    ).tables_aliases == {"a": "product_a", "b": "product_b"}


def test_select_aliases():
    assert Parser("SELECT e.foo FROM bar AS e").columns == ["bar.foo"]
    # assert get_query_columns('SELECT e.foo FROM bar e') == ['bar.foo']


def test_tables_aliases_are_resolved():
    """
    See https://github.com/macbre/sql-metadata/issues/52
    """
    sql = "SELECT a.* FROM users1 AS a JOIN users2 AS b ON a.ip_address = b.ip_address"

    parser = Parser(sql)
    assert parser.tables == ["users1", "users2"]
    assert parser.tables_aliases == {"a": "users1", "b": "users2"}
    assert parser.columns == [
        "users1.*",
        "users1.ip_address",
        "users2.ip_address",
    ], "Should resolve table aliases"
