from sql_metadata import get_query_columns


def test_get_query_columns():
    assert get_query_columns('SELECT * FROM `test_table`') == ['*']
    assert get_query_columns('SELECT foo.* FROM `test_table`') == ['foo.*']
    assert get_query_columns('SELECT foo FROM `test_table`') == ['foo']
    assert get_query_columns('SELECT count(foo) FROM `test_table`') == ['foo']
    assert get_query_columns('SELECT COUNT(foo), max(time_id) FROM `test_table`') == ['foo', 'time_id']
    assert get_query_columns('SELECT id, foo FROM test_table WHERE id = 3') == ['id', 'foo']
    assert get_query_columns('SELECT id, foo FROM test_table WHERE foo_id = 3 AND bar = 5') == ['id', 'foo', 'foo_id', 'bar']
    assert get_query_columns('SELECT foo, count(*) as bar FROM `test_table` WHERE id = 3') == ['foo', 'id']
    assert get_query_columns('SELECT foo, test as bar FROM `test_table`') == ['foo', 'test']
    assert get_query_columns('SELECT /* a comment */ bar FROM test_table') == ['bar']


def test_get_query_columns_order_by():
    assert get_query_columns("SELECT foo FROM bar ORDER BY id") == ['foo', 'id']
    assert get_query_columns("SELECT foo FROM bar WHERE id > 20 ORDER BY id") == ['foo', 'id']
    assert get_query_columns("SELECT id, foo FROM bar ORDER BY id DESC") == ['id', 'foo']
    assert get_query_columns("SELECT user_id,foo FROM bar ORDER BY id LIMIT 20") == ['user_id', 'foo', 'id']


def test_get_query_columns_complex():
    # @see https://github.com/macbre/sql-metadata/issues/6
    assert get_query_columns("SELECT 1 as c    FROM foo_pageviews      WHERE time_id = '2018-01-07 00:00:00'   AND period_id = '2' LIMIT 1") == ['time_id', 'period_id']

    # table aliases
    assert get_query_columns("SELECT r.wiki_id AS id, pageviews_7day AS pageviews FROM report_wiki_recent_pageviews AS r "
        "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id WHERE d.is_public = '1' "
        "AND r.lang IN ( 'en', 'ru' ) AND r.hub_name = 'gaming' ORDER BY pageviews DESC LIMIT 300") \
        == ['report_wiki_recent_pageviews.wiki_id', 'pageviews_7day', 'dimension_wikis.wiki_id', 'dimension_wikis.is_public', 'report_wiki_recent_pageviews.lang', 'report_wiki_recent_pageviews.hub_name', 'pageviews']

    # self joins
    assert get_query_columns("SELECT  count(fw1.wiki_id) as wam_results_total  FROM `fact_wam_scores` `fw1` "
        "left join `fact_wam_scores` `fw2` ON ((fw1.wiki_id = fw2.wiki_id) AND "
        "(fw2.time_id = FROM_UNIXTIME(1466380800))) left join `dimension_wikis` `dw` "
        "ON ((fw1.wiki_id = dw.wiki_id))  WHERE (fw1.time_id = FROM_UNIXTIME(1466467200)) "
        "AND (dw.url like '%%' OR dw.title like '%%') AND fw1.vertical_id IN "
        "('0','1','2','3','4','5','6','7')  AND (fw1.wiki_id NOT "
        "IN ('23312','70256','168929','463633','381622','1089624')) "
        "AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))") \
        == ['fw1.wiki_id', 'fw2.wiki_id', 'fw2.time_id', 'dw.wiki_id', 'fw1.time_id', 'dw.url', 'dw.title', 'fw1.vertical_id']

    assert get_query_columns("SELECT date_format(time_id,'%Y-%m-%d') AS date, pageviews AS cnt         FROM rollup_wiki_pageviews      WHERE period_id = '2'   AND wiki_id = '1676379'         AND time_id BETWEEN '2018-01-08'        AND '2018-01-01'") == ['time_id', 'pageviews', 'period_id', 'wiki_id']

    assert get_query_columns("INSERT /* VoteHelper::addVote xxx */  INTO `page_vote` (article_id,user_id,`time`) VALUES ('442001','27574631','20180228130846')") == ['article_id', 'user_id', 'time']

    # REPLACE queries
    assert get_query_columns("REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) VALUES ('47','infoboxes','')") == ['pp_page', 'pp_propname', 'pp_value']
