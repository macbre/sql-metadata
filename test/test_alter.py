from sql_metadata import Parser, QueryType


def test_alter_table_indices_key():
    parser = Parser("ALTER TABLE foo_table ADD KEY `idx_foo` (`bar`);")
    assert parser.query_type == QueryType.ALTER
    assert parser.tables == ["foo_table"]


def test_alter_table_indices_index():
    parser = Parser("ALTER TABLE foo_table ADD INDEX `idx_foo` (`bar`);")
    assert parser.query_type == QueryType.ALTER
    assert parser.tables == ["foo_table"]
