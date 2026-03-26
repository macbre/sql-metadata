from sql_metadata.parser import Parser


def test_get_query_table_aliases():
    assert Parser("SELECT bar FROM foo").tables_aliases == {}
    assert Parser("SELECT bar FROM foo AS f").tables_aliases == {"f": "foo"}
    assert Parser("SELECT bar FROM foo f").tables_aliases == {"f": "foo"}
    assert Parser("SELECT bar AS value FROM foo AS f").tables_aliases == {"f": "foo"}
    # use SQL keywords as table aliases
    assert Parser("SELECT system.bar FROM foo AS system").tables_aliases == {
        "system": "foo"
    }
    assert Parser("SELECT system.bar FROM foo system").tables_aliases == {
        "system": "foo"
    }
    assert Parser(
        "SELECT bar AS value FROM foo AS f INNER JOIN dimensions AS d ON f.id = d.id"
    ).tables_aliases == {"f": "foo", "d": "dimensions"}
    assert (
        Parser("SELECT e.foo FROM (SELECT * FROM bar) AS e").tables_aliases == {}
    ), "Sub-query aliases are ignored"
    assert Parser(
        "SELECT a.* FROM product_a AS a "
        "JOIN product_b AS b ON a.ip_address = b.ip_address"
    ).tables_aliases == {"a": "product_a", "b": "product_b"}


def test_select_aliases():
    assert Parser("SELECT e.foo FROM bar AS e").columns == ["bar.foo"]
    assert Parser("SELECT e.foo FROM bar e").columns == ["bar.foo"]


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
    ]


def test_column_alias_same_as_join_table_alias():
    # solved: https://github.com/macbre/sql-metadata/issues/424
    query = """
    SELECT
        dependent_schema.name as dependent_schema,
        relationships.dependent_name as dependent_name
    FROM relationships
        JOIN schema AS dependent_schema
            ON relationships.dependent_schema_id = dependent_schema.id
        JOIN schema AS referenced_schema
            ON relationships.referenced_schema_id = referenced_schema.id
    GROUP BY dependent_schema, dependent_name
    ORDER BY dependent_schema, dependent_name
    """
    parser = Parser(query)
    assert parser.tables == ["relationships", "schema"]
    assert parser.tables_aliases == {
        "dependent_schema": "schema",
        "referenced_schema": "schema",
    }
    assert "schema.name" in parser.columns
    assert "relationships.dependent_name" in parser.columns
