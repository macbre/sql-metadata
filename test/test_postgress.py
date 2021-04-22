from sql_metadata import Parser


def test_postgress_quoted_names():
    # https://github.com/macbre/sql-metadata/issues/85
    assert ["test"] == Parser(
        'INSERT INTO "test" ("name") VALUES (\'foo\') RETURNING "test"."id"'
    ).tables
    assert ["name"] == Parser(
        'INSERT INTO "test" ("name") VALUES (\'foo\') RETURNING "test"."id"'
    ).columns
    assert (
        "INSERT INTO test (name) VALUES (X) RETURNING test.id"
        == Parser(
            'INSERT INTO "test" ("name") VALUES (\'foo\') RETURNING "test"."id"'
        ).generalize
    )

    assert ["test"] == Parser(
        'SELECT "test"."id", "test"."name" FROM "test" WHERE "test"."name" = \'foo\' LIMIT 21 FOR UPDATE'
    ).tables
    assert ["test.id", "test.name"] == Parser(
        'SELECT "test"."id", "test"."name" FROM "test" WHERE "test"."name" = \'foo\' LIMIT 21 FOR UPDATE'
    ).columns
    assert (
        "SELECT test.id, test.name FROM test WHERE test.name = X LIMIT N FOR UPDATE"
        == Parser(
            'SELECT "test"."id", "test"."name" FROM "test" WHERE "test"."name" = \'foo\' LIMIT 21 FOR UPDATE'
        ).generalize
    )

    assert ["test"] == Parser(
        'UPDATE "test" SET "name" = \'bar\' WHERE "test"."id" = 1'
    ).tables
    # TODO: check if this should return name also?
    assert ["test.id"] == Parser(
        'UPDATE "test" SET "name" = \'bar\' WHERE "test"."id" = 1'
    ).columns
    assert (
        "UPDATE test SET name = X WHERE test.id = N"
        == Parser('UPDATE "test" SET "name" = \'bar\' WHERE "test"."id" = 1').generalize
    )
