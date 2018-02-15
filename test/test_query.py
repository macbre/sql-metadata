from unittest import TestCase

from sql_metadata import preprocess_query, get_query_columns, get_query_tables, get_query_limit_and_offset


class TestUtils(TestCase):

    def test_preprocess_query(self):
        self.assertEquals(
            preprocess_query('SELECT DISTINCT dw.lang FROM `dimension_wikis` `dw` INNER JOIN `fact_wam_scores` `fwN` ON ((dw.wiki_id = fwN.wiki_id)) WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC'),
            'SELECT DISTINCT lang FROM `dimension_wikis` INNER JOIN `fact_wam_scores` ON ((wiki_id = wiki_id)) WHERE time_id = FROM_UNIXTIME(N) ORDER BY lang ASC'
        )

        self.assertEquals(
            preprocess_query("SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` `fwN` left join `fact_wam_scores` `fwN` ON ((fwN.wiki_id = fwN.wiki_id) AND (fwN.time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` `dw` ON ((fwN.wiki_id = dw.wiki_id)) WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"),
            "SELECT count(wiki_id) as wam_results_total FROM `fact_wam_scores` left join `fact_wam_scores` ON ((wiki_id = wiki_id) AND (time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` ON ((wiki_id = wiki_id)) WHERE (time_id = FROM_UNIXTIME(N)) AND (url like X OR title like X) AND vertical_id IN (XYZ) AND lang = X AND (wiki_id NOT IN (XYZ)) AND ((url IS NOT NULL AND title IS NOT NULL))"
        )

        # remove database selector
        self.assertEquals(
            preprocess_query("SELECT foo FROM `db`.`test`"),
            "SELECT foo FROM test"
        )

        self.assertEquals(
            preprocess_query("SELECT r1.wiki_id AS id FROM report_wiki_recent_pageviews AS r1 INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id"),
            "SELECT wiki_id AS id FROM report_wiki_recent_pageviews AS r1 INNER JOIN dimension_wikis AS d ON wiki_id = wiki_id"
        )

    def test_get_query_columns(self):
        self.assertListEqual(['*'],
                             get_query_columns('SELECT * FROM `test_table`'))

        self.assertListEqual(['foo'],
                             get_query_columns('SELECT foo FROM `test_table`'))

        self.assertListEqual(['foo'],
                             get_query_columns('SELECT count(foo) FROM `test_table`'))

        self.assertListEqual(['foo', 'time_id'],
                             get_query_columns('SELECT COUNT(foo), max(time_id) FROM `test_table`'))

        self.assertListEqual(['id', 'foo'],
                             get_query_columns('SELECT id, foo FROM test_table WHERE id = 3'))

        self.assertListEqual(['id', 'foo', 'foo_id', 'bar'],
                             get_query_columns('SELECT id, foo FROM test_table WHERE foo_id = 3 AND bar = 5'))

        self.assertListEqual(['foo', 'id'],
                             get_query_columns('SELECT foo, count(*) as bar FROM `test_table` WHERE id = 3'))

        self.assertListEqual(['foo', 'test'],
                             get_query_columns('SELECT foo, test as bar FROM `test_table`'))

        self.assertListEqual(['bar'],
                             get_query_columns('SELECT /* a comment */ bar FROM test_table'))

        # complex queries, take two
        # @see https://github.com/macbre/sql-metadata/issues/6
        self.assertListEqual(['time_id', 'period_id'],
                             get_query_columns("SELECT 1 as c    FROM foo_pageviews      WHERE time_id = '2018-01-07 00:00:00'   AND period_id = '2' LIMIT 1"))

        # table aliases
        self.assertListEqual(['wiki_id', 'pageviews_7day', 'is_public', 'lang', 'hub_name', 'pageviews'],
                             get_query_columns("SELECT r.wiki_id AS id, pageviews_7day AS pageviews FROM report_wiki_recent_pageviews AS r "
                                               "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id WHERE d.is_public = '1' "
                                               "AND r.lang IN ( 'en', 'ru' ) AND r.hub_name = 'gaming' ORDER BY pageviews DESC LIMIT 300"))

        # self joins
        self.assertListEqual(['wiki_id', 'time_id', 'url', 'title', 'vertical_id'],
                             get_query_columns("SELECT  count(fw1.wiki_id) as wam_results_total  FROM `fact_wam_scores` `fw1` "
                                               "left join `fact_wam_scores` `fw2` ON ((fw1.wiki_id = fw2.wiki_id) AND "
                                               "(fw2.time_id = FROM_UNIXTIME(1466380800))) left join `dimension_wikis` `dw` "
                                               "ON ((fw1.wiki_id = dw.wiki_id))  WHERE (fw1.time_id = FROM_UNIXTIME(1466467200)) "
                                               "AND (dw.url like '%%' OR dw.title like '%%') AND fw1.vertical_id IN "
                                               "('0','1','2','3','4','5','6','7')  AND (fw1.wiki_id NOT "
                                               "IN ('23312','70256','168929','463633','381622','1089624')) "
                                               "AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"))

        self.assertListEqual(['time_id', 'pageviews', 'period_id', 'wiki_id'],
                             get_query_columns("SELECT date_format(time_id,'%Y-%m-%d') AS date, pageviews AS cnt         FROM rollup_wiki_pageviews      WHERE period_id = '2'   AND wiki_id = '1676379'         AND time_id BETWEEN '2018-01-08'        AND '2018-01-01'"))

        # assert False

    def test_get_query_tables(self):
        self.assertListEqual(['test_table'],
                             get_query_tables('SELECT * FROM `test_table`'))

        self.assertListEqual(['0001_test_table'],
                             get_query_tables('SELECT * FROM `0001_test_table`'))

        self.assertListEqual(['test_table'],
                             get_query_tables('SELECT foo FROM `test_table`'))

        self.assertListEqual(['test_table'],
                             get_query_tables('SELECT foo FROM `db`.`test_table`'))

        self.assertListEqual(['test_table'],
                             get_query_tables('SELECT foo FROM test_table WHERE id = 1'))

        self.assertListEqual(['test_table', 'second_table'],
                             get_query_tables('SELECT foo FROM test_table, second_table WHERE id = 1'))

        self.assertListEqual(['revision', 'page', 'wikicities_user'],
                             get_query_tables('SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_shaN,page_namespace,page_title,page_id,page_latest,user_name FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) LEFT JOIN `wikicities_user` ON ((rev_user != N) AND (user_id = rev_user)) WHERE rev_id = X LIMIT N'))

        self.assertListEqual(['events'],
                             get_query_tables("SELECT COUNT( 0 ) AS cnt, date_format(event_date, '%Y-%m-%d') AS date 	 FROM events 	 WHERE event_date BETWEEN '2017-10-18 00:00:00' 	 AND '2017-10-24 23:59:59'  	 AND wiki_id = '1289985' GROUP BY date WITH ROLLUP"))

        # complex queries
        # @see https://github.com/macbre/query-digest/issues/16
        self.assertListEqual(['report_wiki_recent_pageviews', 'dimension_wikis'],
                             get_query_tables("SELECT r.wiki_id AS id, pageviews_Nday AS pageviews FROM report_wiki_recent_pageviews AS r INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id WHERE d.public = X AND r.lang = X AND r.hub_name = X ORDER BY pageviews DESC LIMIT N"))

        self.assertListEqual(['dimension_wikis', 'fact_wam_scores'],
                             get_query_tables("SELECT DISTINCT dw.lang FROM `dimension_wikis` `dw` INNER JOIN `fact_wam_scores` `fwN` ON ((dw.wiki_id = fwN.wiki_id)) WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC"))

        self.assertListEqual(['fact_wam_scores', 'dimension_wikis'],
                             get_query_tables("SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` `fwN` left join `fact_wam_scores` `fwN` ON ((fwN.wiki_id = fwN.wiki_id) AND (fwN.time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` `dw` ON ((fwN.wiki_id = dw.wiki_id)) WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"))

        self.assertListEqual(['revision', 'page', 'user'],
                             get_query_tables("SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_shaN,page_namespace,page_title,page_id,page_latest,user_name FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) LEFT JOIN `wikicities_cN`.`user` ON ((rev_user != N) AND (user_id = rev_user)) WHERE rev_id = X LIMIT N"))

        # complex queries, take two
        # @see https://github.com/macbre/sql-metadata/issues/6
        self.assertListEqual(['foo_pageviews'],
                             get_query_tables("SELECT 1 as c    FROM foo_pageviews      WHERE time_id = '2018-01-07 00:00:00'   AND period_id = '2' LIMIT 1"))

        # table aliases
        self.assertListEqual(['report_wiki_recent_pageviews', 'dimension_wikis'],
                             get_query_tables("SELECT r.wiki_id AS id, pageviews_7day AS pageviews FROM report_wiki_recent_pageviews AS r INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id WHERE d.public = '1' AND r.lang IN ( 'en', 'ru' ) AND r.hub_name = 'gaming' ORDER BY pageviews DESC LIMIT 300"))

        # self joins
        self.assertListEqual(['fact_wam_scores', 'dimension_wikis'],
                             get_query_tables("SELECT  count(fw1.wiki_id) as wam_results_total  FROM `fact_wam_scores` `fw1` left join `fact_wam_scores` `fw2` ON ((fw1.wiki_id = fw2.wiki_id) AND (fw2.time_id = FROM_UNIXTIME(1466380800))) left join `dimension_wikis` `dw` ON ((fw1.wiki_id = dw.wiki_id))  WHERE (fw1.time_id = FROM_UNIXTIME(1466467200)) AND (dw.url like '%%' OR dw.title like '%%') AND fw1.vertical_id IN ('0','1','2','3','4','5','6','7')  AND (fw1.wiki_id NOT IN ('23312','70256','168929','463633','381622','524772','476782','9764','214934','170145','529622','52149','96420','390','468156','690804','197434','29197','88043','37317','466775','402313','169142','746246','119847','57268','1089624')) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"))

        self.assertListEqual(['rollup_wiki_pageviews'],
                             get_query_tables("SELECT date_format(time_id,'%Y-%m-%d') AS date, pageviews AS cnt         FROM rollup_wiki_pageviews      WHERE period_id = '2'   AND wiki_id = '1676379'         AND time_id BETWEEN '2018-01-08'        AND '2018-01-01'"))

        # INSERT queries
        self.assertListEqual(['0070_insert_ignore_table'],
                             get_query_tables("INSERT IGNORE INTO `0070_insert_ignore_table` VALUES (9, '123', '2017-01-01');"))

        self.assertListEqual(['0070_insert_ignore_table'],
                             get_query_tables("INSERT into `0070_insert_ignore_table` VALUES (9, '123', '2017-01-01');"))

        self.assertListEqual(['foo'],
                             get_query_tables("INSERT INTO `foo` (id,text) VALUES (X,X)"))

        # UPDATE queries
        self.assertListEqual(['page'],
                             get_query_tables("UPDATE `page` SET page_touched = X WHERE page_id = X"))

        # assert False

    def test_get_query_limit_and_offset(self):
        self.assertIsNone(get_query_limit_and_offset('SELECT foo_limit FROM bar_offset'))
        self.assertIsNone(get_query_limit_and_offset('SELECT foo_limit FROM bar_offset /* limit 1000,50 */'))

        self.assertEquals(
            (50, 0),
            get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 50')
        )

        self.assertEquals(
            (50, 1000),
            get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 50 OFFSET 1000')
        )

        self.assertEquals(
            (50, 1000),
            get_query_limit_and_offset('SELECT foo_limit FROM bar_offset Limit 50 offset 1000')
        )

        self.assertEquals(
            (50, 1000),
            get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 1000, 50')
        )

        self.assertEquals(
            (50, 1000),
            get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 1000,50')
        )

        self.assertEquals(
            (50, 1000),
            get_query_limit_and_offset('SELECT foo_limit FROM bar_offset limit 1000,50')
        )

        self.assertEquals(
            (200, 927600),
            get_query_limit_and_offset("SELECT /* CategoryPaginationViewer::processSection */  page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix  FROM `page` INNER JOIN `categorylinks` FORCE INDEX (cl_sortkey) ON ((cl_from = page_id))  WHERE cl_type = 'page' AND cl_to = 'Spotify/Song'  ORDER BY cl_sortkey LIMIT 927600,200")
        )
