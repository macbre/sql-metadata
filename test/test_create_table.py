import pytest

from sql_metadata import Parser


def test_is_create_table_query():
    with pytest.raises(ValueError):
        assert Parser("BEGIN").query_type

    assert Parser("SELECT * FROM `foo` ()").query_type == "Select"
    assert Parser("CREATE TABLE `foo` ()").query_type == "Create"
    assert (
        Parser(
            "create table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
        ).query_type
        == "Create"
    )


def test_create_table():
    parser = Parser(
        """
CREATE TABLE `new_table` (
    `item_id` int(9) NOT NULL AUTO_INCREMENT,
    `foo` varchar(16) NOT NULL DEFAULT '',
    PRIMARY KEY (`item_id`,`foo`),
    KEY `idx_foo` (`foo`)
) CHARACTER SET utf8;
    """
    )

    assert parser.tables == ["new_table"]
    assert parser.columns == ["item_id", "foo"]


def test_create_table_as_select():
    parser = Parser(
        """
create table abc.foo
    as SELECT pqr.foo1 , ab.foo2
    FROM foo pqr, bar ab;
    """
    )

    assert parser.tables == ["abc.foo", "foo", "bar"]
    assert parser.columns == ["foo.foo1", "bar.foo2"]
