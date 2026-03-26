"""
Set of unit tests for handling of Apache Hive queries
"""

from sql_metadata.parser import Parser


def test_insert_overwrite_table():
    assert ["foo_report"] == Parser("INSERT TABLE foo_report").tables
    assert ["foo_report"] == Parser("INSERT OVERWRITE TABLE foo_report").tables
    assert ["foo_report", "bar"] == Parser(
        "INSERT OVERWRITE TABLE foo_report SELECT foo FROM bar"
    ).tables

    assert ["foo"] == Parser(
        "INSERT OVERWRITE TABLE foo_report SELECT foo FROM bar"
    ).columns


def test_complex_hive_query():
    # https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
    dag = """
INSERT OVERWRITE TABLE foo_report
SELECT
  d.domain, r.wiki_id, r.beacon, r.pageviews
FROM
  (SELECT wiki_id, beacon, sum(pageviews) AS pageviews FROM rollup_wiki_beacon_pageviews
   WHERE period_id = '1' AND
     (
       year > '{{ beginYear28 }}' OR
       (year = '{{ beginYear28 }}' AND month > '{{ beginMonth28 }}') OR
       (year = '{{ beginYear28 }}' AND month = '{{ beginMonth28 }}'
       AND day > '{{ beginDay28 }}')
     ) AND (
       year < '{{ beginYear }}' OR
       (year = '{{ beginYear }}' AND month < '{{ beginMonth }}') OR
       (year = '{{ beginYear }}' AND month = '{{ beginMonth }}'
       AND day <= '{{ beginDay }}')
     )
   GROUP BY wiki_id, beacon) r
JOIN statsdb.dimension_wikis d ON r.wiki_id = d.wiki_id;
    """

    assert [
        "foo_report",
        "rollup_wiki_beacon_pageviews",
        "statsdb.dimension_wikis",
    ] == Parser(dag).tables


def test_hive_alter_table_drop_partition():
    # solved: https://github.com/macbre/sql-metadata/issues/495
    query = "ALTER TABLE table_name DROP IF EXISTS PARTITION (dt = 20240524)"
    parser = Parser(query)
    assert parser.tables == ["table_name"]
    assert "PARTITION" not in parser.tables
    assert "dt" not in parser.tables


def test_hive_insert_overwrite_with_partition():
    # solved: https://github.com/macbre/sql-metadata/issues/502
    query = """
    INSERT OVERWRITE TABLE tbl PARTITION (dt='20240101')
    SELECT col1, col2 FROM table1
    JOIN table2 ON table1.id = table2.id
    """
    parser = Parser(query)
    assert parser.tables == ["tbl", "table1", "table2"]
    assert "dt" not in parser.tables
    assert parser.columns == ["col1", "col2", "table1.id", "table2.id"]
    assert parser.columns_dict == {
        "select": ["col1", "col2"],
        "join": ["table1.id", "table2.id"],
    }
