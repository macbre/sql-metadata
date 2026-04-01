"""
Set of unit tests for handling of SQLite queries
"""

from sql_metadata.parser import Parser


def test_natural_join():
    query = "SELECT id FROM table1 NATURAL JOIN table2"

    assert ["table1", "table2"] == Parser(query).tables
    assert ["id"] == Parser(query).columns


def test_single_quoted_identifiers():
    # Solved: https://github.com/macbre/sql-metadata/issues/541
    query = (
        "SELECT r.Year, AVG(r.'Walt Disney Parks and Resorts') AS Avg_Parks_Revenue"
        " FROM 'revenue' r WHERE r.Year=2000"
    )
    parser = Parser(query)
    assert parser.tables == ["revenue"]
    assert parser.columns == ["revenue.Year", "revenue.Walt Disney Parks and Resorts"]
