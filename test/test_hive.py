"""
Set of unit tests for handling of Apache Hive queries
"""
from sql_metadata import get_query_columns, get_query_tables


def test_insert_overwrite_table():
    assert ['foo_report'] == get_query_tables('INSERT TABLE foo_report')
    assert ['foo_report'] == get_query_tables('INSERT OVERWRITE TABLE foo_report')
    assert ['foo_report', 'bar'] == get_query_tables('INSERT OVERWRITE TABLE foo_report SELECT foo FROM bar')

    assert ['foo'] == get_query_columns('INSERT OVERWRITE TABLE foo_report SELECT foo FROM bar')


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
       (year = '{{ beginYear28 }}' AND month = '{{ beginMonth28 }}' AND day > '{{ beginDay28 }}')
     ) AND (
       year < '{{ beginYear }}' OR
       (year = '{{ beginYear }}' AND month < '{{ beginMonth }}') OR
       (year = '{{ beginYear }}' AND month = '{{ beginMonth }}' AND day <= '{{ beginDay }}')
     )
   GROUP BY wiki_id, beacon) r
JOIN statsdb.dimension_wikis d ON r.wiki_id = d.wiki_id;
    """

    assert ['foo_report', 'rollup_wiki_beacon_pageviews', 'statsdb.dimension_wikis'] == get_query_tables(dag)
