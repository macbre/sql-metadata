from sql_metadata import Parser


def test_is_create_table_query():
    assert Parser("BEGIN")._is_create_table_query is False
    assert Parser("SELECT * FROM `foo` ()")._is_create_table_query is False

    assert Parser("CREATE TABLE `foo` ()")._is_create_table_query is True
    assert (
        Parser(
            "create table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
        )._is_create_table_query
        is True
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
    # assert parser.columns == ["item_id", "foo"]
