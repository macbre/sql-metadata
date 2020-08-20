from sql_metadata import get_query_tables, get_query_columns


def test_tables_aliases_are_resolved():
    """
    See https://github.com/macbre/sql-metadata/issues/52
    """
    sql = "SELECT a.* FROM users1 AS a JOIN users2 AS b ON a.ip_address = b.ip_address"

    assert get_query_tables(sql) == ['users1', 'users2']
    assert get_query_columns(sql) == ['users1.*', 'users1.ip_address', 'users2.ip_address'], 'Should resolve table aliases'
