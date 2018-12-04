from sql_metadata import preprocess_query, get_query_tokens,\
    get_query_columns, get_query_tables, get_query_limit_and_offset

from sqlparse.tokens import DML, Keyword


def test_get_query_tokens():
    assert get_query_tokens("") == []

    tokens = get_query_tokens("SELECT * FROM foo")

    assert len(tokens) == 4

    assert tokens[0].ttype is DML
    assert str(tokens[0]) == 'SELECT'
    assert tokens[2].ttype is Keyword
    assert str(tokens[2]) == 'FROM'


def test_preprocess_query():
    assert preprocess_query('SELECT DISTINCT dw.lang FROM `dimension_wikis` `dw` INNER JOIN `fact_wam_scores` `fwN` ON ((dw.wiki_id = fwN.wiki_id)) WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC') == \
        'SELECT DISTINCT dw.lang FROM `dimension_wikis` INNER JOIN `fact_wam_scores` ON ((dw.wiki_id = fwN.wiki_id)) WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC'

    assert preprocess_query("SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` `fwN` left join `fact_wam_scores` `fwN` ON ((fwN.wiki_id = fwN.wiki_id) AND (fwN.time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` `dw` ON ((fwN.wiki_id = dw.wiki_id)) WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))") == \
        "SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` left join `fact_wam_scores` ON ((fwN.wiki_id = fwN.wiki_id) AND (fwN.time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` ON ((fwN.wiki_id = dw.wiki_id)) WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"

    # normalize database selector
    assert preprocess_query("SELECT foo FROM `db`.`test`") == \
        "SELECT foo FROM db.test"

    assert preprocess_query("SELECT r1.wiki_id AS id FROM report_wiki_recent_pageviews AS r1 INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id") == \
        "SELECT r1.wiki_id AS id FROM report_wiki_recent_pageviews AS r1 INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id"


def test_get_query_tables():
    assert ['test_table'] == get_query_tables('SELECT * FROM `test_table`')

    assert ['0001_test_table'] == get_query_tables('SELECT * FROM `0001_test_table`')

    assert ['test_table'] == get_query_tables('SELECT foo FROM `test_table`')

    assert ['s.t'] == get_query_tables('SELECT * FROM s.t')

    assert ['db.test_table'] == get_query_tables('SELECT foo FROM `db`.`test_table`')

    assert ['test_table'] == get_query_tables('SELECT foo FROM test_table WHERE id = 1')

    assert ['test_table', 'second_table'] == get_query_tables('SELECT foo FROM test_table, second_table WHERE id = 1')

    assert ['revision', 'page', 'wikicities_user'] == get_query_tables('SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_shaN,page_namespace,page_title,page_id,page_latest,user_name FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) LEFT JOIN `wikicities_user` ON ((rev_user != N) AND (user_id = rev_user)) WHERE rev_id = X LIMIT N')

    assert ['events'] == get_query_tables("SELECT COUNT( 0 ) AS cnt, date_format(event_date, '%Y-%m-%d') AS date 	 FROM events 	 WHERE event_date BETWEEN '2017-10-18 00:00:00' 	 AND '2017-10-24 23:59:59'  	 AND wiki_id = '1289985' GROUP BY date WITH ROLLUP")

    # complex queries
    # @see https://github.com/macbre/query-digest/issues/16
    assert ['report_wiki_recent_pageviews', 'dimension_wikis'] == \
        get_query_tables("SELECT r.wiki_id AS id, pageviews_Nday AS pageviews FROM report_wiki_recent_pageviews AS r INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id WHERE d.public = X AND r.lang = X AND r.hub_name = X ORDER BY pageviews DESC LIMIT N")

    assert ['dimension_wikis', 'fact_wam_scores'] == \
         get_query_tables("SELECT DISTINCT dw.lang FROM `dimension_wikis` `dw` INNER JOIN `fact_wam_scores` `fwN` ON ((dw.wiki_id = fwN.wiki_id)) WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC")

    assert ['fact_wam_scores', 'dimension_wikis'] == \
         get_query_tables("SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` `fwN` left join `fact_wam_scores` `fwN` ON ((fwN.wiki_id = fwN.wiki_id) AND (fwN.time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` `dw` ON ((fwN.wiki_id = dw.wiki_id)) WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))")

    assert ['revision', 'page', 'wikicities_cN.user'] == \
         get_query_tables("SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_shaN,page_namespace,page_title,page_id,page_latest,user_name FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) LEFT JOIN `wikicities_cN`.`user` ON ((rev_user != N) AND (user_id = rev_user)) WHERE rev_id = X LIMIT N")

    # complex queries, take two
    # @see https://github.com/macbre/sql-metadata/issues/6
    assert ['foo_pageviews'] == \
        get_query_tables("SELECT 1 as c    FROM foo_pageviews      WHERE time_id = '2018-01-07 00:00:00'   AND period_id = '2' LIMIT 1")

    # table aliases
    assert ['report_wiki_recent_pageviews', 'dimension_wikis'] == \
        get_query_tables("SELECT r.wiki_id AS id, pageviews_7day AS pageviews FROM report_wiki_recent_pageviews AS r INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id WHERE d.public = '1' AND r.lang IN ( 'en', 'ru' ) AND r.hub_name = 'gaming' ORDER BY pageviews DESC LIMIT 300")

    # self joins
    assert ['fact_wam_scores', 'dimension_wikis'] == \
        get_query_tables("SELECT  count(fw1.wiki_id) as wam_results_total  FROM `fact_wam_scores` `fw1` left join `fact_wam_scores` `fw2` ON ((fw1.wiki_id = fw2.wiki_id) AND (fw2.time_id = FROM_UNIXTIME(1466380800))) left join `dimension_wikis` `dw` ON ((fw1.wiki_id = dw.wiki_id))  WHERE (fw1.time_id = FROM_UNIXTIME(1466467200)) AND (dw.url like '%%' OR dw.title like '%%') AND fw1.vertical_id IN ('0','1','2','3','4','5','6','7')  AND (fw1.wiki_id NOT IN ('23312','70256','168929','463633','381622','524772','476782','9764','214934','170145','529622','52149','96420','390','468156','690804','197434','29197','88043','37317','466775','402313','169142','746246','119847','57268','1089624')) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))")

    assert ['rollup_wiki_pageviews'] == \
        get_query_tables("SELECT date_format(time_id,'%Y-%m-%d') AS date, pageviews AS cnt         FROM rollup_wiki_pageviews      WHERE period_id = '2'   AND wiki_id = '1676379'         AND time_id BETWEEN '2018-01-08'        AND '2018-01-01'")

    # INSERT queries
    assert ['0070_insert_ignore_table'] == \
        get_query_tables("INSERT IGNORE INTO `0070_insert_ignore_table` VALUES (9, '123', '2017-01-01');")

    assert ['0070_insert_ignore_table'] == \
        get_query_tables("INSERT into `0070_insert_ignore_table` VALUES (9, '123', '2017-01-01');")

    assert ['foo'] == \
        get_query_tables("INSERT INTO `foo` (id,text) VALUES (X,X)")

    assert ['page_vote'] == \
        get_query_tables("INSERT /* VoteHelper::addVote xxx */  INTO `page_vote` (article_id,user_id,time) VALUES ('442001','27574631','20180228130846')")

    # UPDATE queries
    assert ['page'] == \
        get_query_tables("UPDATE `page` SET page_touched = X WHERE page_id = X")

    # REPLACE queries
    assert ['page_props'] == \
        get_query_tables("REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) VALUES ('47','infoboxes','')")

    # JOINs
    assert ['product_a.users', 'product_b.users'] == \
        get_query_tables("SELECT a.* FROM product_a.users AS a JOIN product_b.users AS b ON a.ip_address = b.ip_address")


def test_joins():
    assert ['redirect', 'page'] == \
        get_query_tables("SELECT  page_title  FROM `redirect` INNER JOIN `page` "
                         "ON (rd_title = 'foo' AND rd_namespace = '100' AND (page_id = rd_from))")

    assert ['page_title', 'rd_title', 'rd_namespace', 'page_id', 'rd_from'] == \
        get_query_columns("SELECT  page_title  FROM `redirect` INNER JOIN `page` "
                          "ON (rd_title = 'foo' AND rd_namespace = '100' AND (page_id = rd_from))")


def test_handle_force_index():
    query = "SELECT  page_title,page_namespace  FROM `page` FORCE INDEX (page_random) " \
            "JOIN `categorylinks` ON ((page_id=cl_from))  WHERE page_is_redirect = '0' " \
            "AND (page_random >= 0.197372293871) AND cl_to = 'Muppet_Characters'  " \
            "ORDER BY page_random LIMIT 1"

    assert get_query_tables(query) == ['page', 'categorylinks']
    assert get_query_columns(query) == \
        ['page_title', 'page_namespace', 'page_id', 'cl_from', 'page_is_redirect', 'page_random', 'cl_to']


def test_get_query_limit_and_offset():
    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset') is None
    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset /* limit 1000,50 */') is None

    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 50') == (50, 0)
    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 50 OFFSET 1000') == (50, 1000)
    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset Limit 50 offset 1000') == (50, 1000)
    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 1000, 50') == (50, 1000)
    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 1000,50') == (50, 1000)
    assert get_query_limit_and_offset('SELECT foo_limit FROM bar_offset limit 1000,50') == (50, 1000)

    assert get_query_limit_and_offset(
        "SELECT /* CategoryPaginationViewer::processSection */  "
        "page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix  FROM `page` "
        "INNER JOIN `categorylinks` FORCE INDEX (cl_sortkey) ON ((cl_from = page_id))  "
        "WHERE cl_type = 'page' AND cl_to = 'Spotify/Song'  "
        "ORDER BY cl_sortkey LIMIT 927600,200") == (200, 927600)


def test_insert_into_select():
    # https://dev.mysql.com/doc/refman/5.7/en/insert-select.html
    query = "INSERT INTO foo SELECT * FROM bar"
    assert get_query_tables(query) == ['foo', 'bar']
    assert get_query_columns(query) == ['*']

    query = "INSERT INTO foo SELECT id, price FROM bar"
    assert get_query_tables(query) == ['foo', 'bar']
    assert get_query_columns(query) == ['id', 'price']

    query = "INSERT INTO foo SELECT id, price FROM bar WHERE qty > 200"
    assert get_query_tables(query) == ['foo', 'bar']
    assert get_query_columns(query) == ['id', 'price', 'qty']


def test_cast_and_convert_functions():
    # https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html
    assert get_query_columns('SELECT count(c) as test, id FROM foo where cast(d as bigint) > e') == ['c', 'id', 'd', 'e']
    assert get_query_columns('SELECT CONVERT(latin1_column USING utf8) FROM latin1_table;') == ['latin1_column']
