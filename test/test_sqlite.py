"""
Set of unit tests for handling of SQLite queries
"""

from sql_metadata.parser import Parser


def test_natural_join():
    query = "SELECT id FROM table1 NATURAL JOIN table2"

    assert ["table1", "table2"] == Parser(query).tables
    assert ["id"] == Parser(query).columns
