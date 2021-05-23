from sql_metadata import Parser


def test_postgress_quoted_names():
    # https://github.com/macbre/sql-metadata/issues/85
    parser = Parser(
        'INSERT INTO "test" ("name") VALUES (\'foo\') RETURNING "test"."id"'
    )
    assert ["test"] == parser.tables
    assert ["name"] == parser.columns
    assert {"INSERT": ["name"]} == parser.columns_dict
    assert "INSERT INTO test (name) VALUES (X) RETURNING test.id" == parser.generalize
    assert parser.values == ["foo"]

    parser = Parser(
        'SELECT "test"."id", "test"."name" FROM "test" WHERE "test"."name" = \'foo\' LIMIT 21 FOR UPDATE'
    )
    assert ["test"] == parser.tables
    assert ["test.id", "test.name"] == parser.columns
    assert {
        "SELECT": ["test.id", "test.name"],
        "where": ["test.name"],
    } == parser.columns_dict
    assert (
        "SELECT test.id, test.name FROM test WHERE test.name = X LIMIT N FOR UPDATE"
        == parser.generalize
    )

    parser = Parser('UPDATE "test" SET "name" = \'bar\' WHERE "test"."id" = 1')
    assert ["test"] == parser.tables
    assert ["name", "test.id"] == parser.columns
    assert {"UPDATE": ["name"], "where": ["test.id"]} == parser.columns_dict
    assert "UPDATE test SET name = X WHERE test.id = N" == parser.generalize
