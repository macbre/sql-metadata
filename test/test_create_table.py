from sql_metadata import Parser


def test_create_table():
    parser = Parser("""
CREATE TABLE `new_table` (
    `item_id` int(9) NOT NULL AUTO_INCREMENT,
    `foo` varchar(16) NOT NULL DEFAULT '',
    PRIMARY KEY (`item_id`,`foo`),
    KEY `idx_foo` (`foo`)
) CHARACTER SET utf8;
    """)

    assert parser.tables == ["new_table"]
    assert parser.columns == ["item_id", "foo"]
