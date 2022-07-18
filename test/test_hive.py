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
        "statsdb.dimension_wikis",
        "rollup_wiki_beacon_pageviews",
    ] == Parser(dag).tables
