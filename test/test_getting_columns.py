from sql_metadata.parser import Parser


def test_cast_and_convert_functions():
    # https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html
    parser = Parser("SELECT count(c) as test, id FROM foo where cast(d as bigint) > e")
    assert parser.columns == ["c", "id", "d", "e"]
    assert parser.columns_dict == {"select": ["c", "id"], "where": ["d", "e"]}

    parser = Parser("SELECT CONVERT(latin1_column USING utf8) FROM latin1_table;")
    assert parser.columns == ["latin1_column"]
    assert parser.columns_dict == {"select": ["latin1_column"]}


def test_queries_with_null_conditions():
    parser = Parser(
        "SELECT id FROM cm WHERE cm.status = 1 AND cm.OPERATIONDATE IS NULL AND cm.OID IN(123123);"
    )
    assert parser.columns == ["id", "cm.status", "cm.OPERATIONDATE", "cm.OID"]
    assert parser.columns_dict == {
        "select": ["id"],
        "where": ["cm.status", "cm.OPERATIONDATE", "cm.OID"],
    }

    parser = Parser(
        "SELECT id FROM cm WHERE cm.status = 1 AND cm.OPERATIONDATE IS NOT NULL AND cm.OID IN(123123);"
    )
    assert parser.columns == ["id", "cm.status", "cm.OPERATIONDATE", "cm.OID"]
    assert parser.columns_dict == {
        "select": ["id"],
        "where": ["cm.status", "cm.OPERATIONDATE", "cm.OID"],
    }


def test_queries_with_distinct():
    assert Parser("SELECT DISTINCT DATA.ASSAY_ID FROM foo").columns == ["DATA.ASSAY_ID"]

    assert Parser("SELECT UNIQUE DATA.ASSAY_ID FROM foo").columns == ["DATA.ASSAY_ID"]


def test_joins():
    assert ["page_title", "rd_title", "rd_namespace", "page_id", "rd_from",] == Parser(
        "SELECT  page_title  FROM `redirect` INNER JOIN `page` "
        "ON (rd_title = 'foo' AND rd_namespace = '100' AND (page_id = rd_from))"
    ).columns


def test_getting_columns():
    assert Parser("SELECT * FROM `test_table`").columns == ["*"]
    assert Parser("SELECT foo.* FROM `test_table`").columns == ["foo.*"]
    assert Parser("SELECT foo FROM `test_table`").columns == ["foo"]
    assert Parser("SELECT count(foo) FROM `test_table`").columns == ["foo"]
    assert Parser("SELECT COUNT(foo), max(time_id) FROM `test_table`").columns == [
        "foo",
        "time_id",
    ]
    assert Parser("SELECT id, foo FROM test_table WHERE id = 3").columns == [
        "id",
        "foo",
    ]
    assert Parser(
        "SELECT id, foo FROM test_table WHERE foo_id = 3 AND bar = 5"
    ).columns == ["id", "foo", "foo_id", "bar"]
    assert Parser(
        "SELECT foo, count(*) as bar FROM `test_table` WHERE id = 3"
    ).columns == ["foo", "id"]
    assert Parser("SELECT foo, test as bar FROM `test_table`").columns == [
        "foo",
        "test",
    ]
    assert Parser("SELECT /* a comment */ bar FROM test_table").columns == ["bar"]


def test_columns_with_order_by():
    assert Parser("SELECT foo FROM bar ORDER BY id").columns == ["foo", "id"]
    assert Parser("SELECT foo FROM bar WHERE id > 20 ORDER BY id").columns == [
        "foo",
        "id",
    ]
    assert Parser("SELECT id, foo FROM bar ORDER BY id DESC").columns == [
        "id",
        "foo",
    ]
    assert Parser("SELECT user_id,foo FROM bar ORDER BY id LIMIT 20").columns == [
        "user_id",
        "foo",
        "id",
    ]


def test_update_and_replace():
    # UPDATE queries
    parser = Parser(
        "UPDATE `page` SET page_touched = other_column WHERE page_id = 'test'"
    )
    assert parser.columns == ["page_touched", "other_column", "page_id"]
    assert parser.columns_dict == {
        "update": ["page_touched", "other_column"],
        "where": ["page_id"],
    }

    parser = Parser("UPDATE `page` SET page_touched = 'value' WHERE page_id = 'test'")
    assert parser.columns == ["page_touched", "page_id"]
    assert parser.columns_dict == {"update": ["page_touched"], "where": ["page_id"]}

    # REPLACE queries
    parser = Parser(
        "REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) VALUES ('47','infoboxes','')"
    )
    assert parser.columns == ["pp_page", "pp_propname", "pp_value"]
    assert parser.columns_dict == {"insert": ["pp_page", "pp_propname", "pp_value"]}


def test_complex_queries_columns():
    # @see https://github.com/macbre/sql-metadata/issues/6
    assert Parser(
        "SELECT 1 as c    FROM foo_pageviews WHERE time_id = '2018-01-07 00:00:00' AND period_id = '2' LIMIT 1"
    ).columns == ["time_id", "period_id"]

    # table aliases
    parser = Parser(
        "SELECT r.wiki_id AS id, pageviews_7day AS pageviews FROM report_wiki_recent_pageviews AS r "
        "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id WHERE d.is_public = '1' "
        "AND r.lang IN ( 'en', 'ru' ) AND r.hub_name = 'gaming' ORDER BY pageviews DESC LIMIT 300"
    )
    assert parser.columns == [
        "report_wiki_recent_pageviews.wiki_id",
        "pageviews_7day",
        "dimension_wikis.wiki_id",
        "dimension_wikis.is_public",
        "report_wiki_recent_pageviews.lang",
        "report_wiki_recent_pageviews.hub_name",
        "pageviews",
    ]
    assert parser.columns_dict == {
        "select": ["report_wiki_recent_pageviews.wiki_id", "pageviews_7day"],
        "join": ["report_wiki_recent_pageviews.wiki_id", "dimension_wikis.wiki_id"],
        "where": [
            "dimension_wikis.is_public",
            "report_wiki_recent_pageviews.lang",
            "report_wiki_recent_pageviews.hub_name",
        ],
        "order_by": ["pageviews"],
    }

    # self joins
    parser = Parser(
        "SELECT  count(fw1.wiki_id) as wam_results_total  FROM `fact_wam_scores` `fw1` "
        "left join `fact_wam_scores` `fw2` ON ((fw1.wiki_id = fw2.wiki_id) AND "
        "(fw2.time_id = FROM_UNIXTIME(1466380800))) left join `dimension_wikis` `dw` "
        "ON ((fw1.wiki_id = dw.wiki_id))  WHERE (fw1.time_id = FROM_UNIXTIME(1466467200)) "
        "AND (dw.url like '%%' OR dw.title like '%%') AND fw1.vertical_id IN "
        "('0','1','2','3','4','5','6','7')  AND (fw1.wiki_id NOT "
        "IN ('23312','70256','168929','463633','381622','1089624')) "
        "AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"
    )
    assert parser.columns == [
        "fw1.wiki_id",
        "fw2.wiki_id",
        "fw2.time_id",
        "dw.wiki_id",
        "fw1.time_id",
        "dw.url",
        "dw.title",
        "fw1.vertical_id",
    ]
    assert parser.columns_dict == {
        "select": ["fw1.wiki_id"],
        "join": ["fw1.wiki_id", "fw2.wiki_id", "fw2.time_id", "dw.wiki_id"],
        "where": [
            "fw1.time_id",
            "dw.url",
            "dw.title",
            "fw1.vertical_id",
            "fw1.wiki_id",
        ],
    }

    assert Parser(
        "SELECT date_format(time_id,'%Y-%m-%d') AS date, pageviews AS cnt         FROM rollup_wiki_pageviews      WHERE period_id = '2'   AND wiki_id = '1676379'         AND time_id BETWEEN '2018-01-08'        AND '2018-01-01'"
    ).columns == ["time_id", "pageviews", "period_id", "wiki_id"]

    parser = Parser(
        "INSERT /* VoteHelper::addVote xxx */  INTO `page_vote` (article_id,user_id,`time`) VALUES ('442001','27574631','20180228130846')"
    )
    assert parser.columns == ["article_id", "user_id", "time"]

    # REPLACE queries
    parser = Parser(
        "REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) VALUES ('47','infoboxes','')"
    )
    assert parser.columns == ["pp_page", "pp_propname", "pp_value"]
    assert parser.columns_dict == {"insert": ["pp_page", "pp_propname", "pp_value"]}

    assert Parser(
        "SELECT /* CategoryPaginationViewer::processSection */  "
        "page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix  FROM `page` "
        "INNER JOIN `categorylinks` FORCE INDEX (cl_sortkey) ON ((cl_from = page_id))  "
        "WHERE cl_type = 'page' AND cl_to = 'Spotify/Song'  "
        "ORDER BY cl_sortkey LIMIT 927600,200"
    ).columns_dict == {
        "select": [
            "page_namespace",
            "page_title",
            "page_len",
            "page_is_redirect",
            "cl_sortkey_prefix",
        ],
        "join": ["cl_from", "page_id"],
        "where": ["cl_type", "cl_to"],
        "order_by": ["cl_sortkey"],
    }


def test_columns_and_sql_functions():
    """
    See https://github.com/macbre/sql-metadata/issues/125
    """
    assert Parser("select max(col3)+avg(col)+1+sum(col2) from dual").columns == [
        "col3",
        "col",
        "col2",
    ]
    assert Parser("select avg(col)+sum(col2) from dual").columns == ["col", "col2"]
    assert Parser(
        "select count(col)+max(col2)+ min(col3)+ count(distinct  col4) + custom_func(col5) from dual"
    ).columns == ["col", "col2", "col3", "col4", "col5"]
