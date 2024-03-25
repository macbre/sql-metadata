from sql_metadata.keywords_lists import QueryType
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
        "SELECT id FROM cm WHERE cm.status = 1 AND cm.OPERATIONDATE IS NULL "
        "AND cm.OID IN(123123);"
    )
    assert parser.columns == ["id", "cm.status", "cm.OPERATIONDATE", "cm.OID"]
    assert parser.columns_dict == {
        "select": ["id"],
        "where": ["cm.status", "cm.OPERATIONDATE", "cm.OID"],
    }

    parser = Parser(
        "SELECT id FROM cm WHERE cm.status = 1 AND cm.OPERATIONDATE IS NOT NULL "
        "AND cm.OID IN(123123);"
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
    assert ["page_title", "rd_title", "rd_namespace", "page_id", "rd_from"] == Parser(
        "SELECT  page_title  FROM `redirect` INNER JOIN `page` "
        "ON (rd_title = 'foo' AND rd_namespace = '100' AND (page_id = rd_from))"
    ).columns

    assert ["page_title"] == Parser(
        "SELECT  page_title  FROM `redirect` CROSS JOIN `page` "
    ).columns

    assert ["page_title", "rd_title"] == Parser(
        "SELECT  page_title  FROM `redirect` CROSS JOIN (select rd_title from `page`) as other"
    ).columns


def test_joins_using():
    parser = Parser(
        "SELECT  page_title  FROM `redirect` INNER JOIN `page` "
        "USING (page_title, rd_title, rd_namespace)"
    )
    assert parser.columns == ["page_title", "rd_title", "rd_namespace"]
    assert parser.columns_dict == {
        "select": ["page_title"],
        "join": ["page_title", "rd_title", "rd_namespace"],
    }


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
    assert (
        Parser(
            """
                WITH foo AS (SELECT test_table.* FROM test_table)
                SELECT foo.bar FROM foo
            """
        ).columns
        == ["test_table.*", "bar"]
    )


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
        "REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) "
        "VALUES ('47','infoboxes','')"
    )
    assert parser.query_type == QueryType.REPLACE
    assert parser.columns == ["pp_page", "pp_propname", "pp_value"]
    assert parser.columns_dict == {"insert": ["pp_page", "pp_propname", "pp_value"]}


def test_complex_queries_columns():
    # @see https://github.com/macbre/sql-metadata/issues/6
    assert Parser(
        "SELECT 1 as c    FROM foo_pageviews WHERE time_id = '2018-01-07 00:00:00' "
        "AND period_id = '2' LIMIT 1"
    ).columns == ["time_id", "period_id"]

    # table aliases
    parser = Parser(
        "SELECT r.wiki_id AS id, pageviews_7day AS pageviews "
        "FROM report_wiki_recent_pageviews AS r "
        "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id "
        "WHERE d.is_public = '1' "
        "AND r.lang IN ( 'en', 'ru' ) AND r.hub_name = 'gaming' "
        "ORDER BY pageviews DESC LIMIT 300"
    )
    assert parser.columns_aliases_names == ["id", "pageviews"]
    assert parser.columns_aliases == {
        "id": "report_wiki_recent_pageviews.wiki_id",
        "pageviews": "pageviews_7day",
    }
    assert parser.columns == [
        "report_wiki_recent_pageviews.wiki_id",
        "pageviews_7day",
        "dimension_wikis.wiki_id",
        "dimension_wikis.is_public",
        "report_wiki_recent_pageviews.lang",
        "report_wiki_recent_pageviews.hub_name",
    ]
    assert parser.columns_aliases_dict == {
        "order_by": ["pageviews"],
        "select": ["id", "pageviews"],
    }
    assert parser.columns_aliases == {
        "id": "report_wiki_recent_pageviews.wiki_id",
        "pageviews": "pageviews_7day",
    }
    assert parser.columns_dict == {
        "select": ["report_wiki_recent_pageviews.wiki_id", "pageviews_7day"],
        "join": ["report_wiki_recent_pageviews.wiki_id", "dimension_wikis.wiki_id"],
        "where": [
            "dimension_wikis.is_public",
            "report_wiki_recent_pageviews.lang",
            "report_wiki_recent_pageviews.hub_name",
        ],
        "order_by": ["pageviews_7day"],
    }

    # self joins
    parser = Parser(
        "SELECT  count(fw1.wiki_id) as wam_results_total  FROM `fact_wam_scores` `fw1` "
        "left join `fact_wam_scores` `fw2` ON ((fw1.wiki_id = fw2.wiki_id) AND "
        "(fw2.time_id = FROM_UNIXTIME(1466380800))) left join `dimension_wikis` `dw` "
        "ON ((fw1.wiki_id = dw.wiki_id))  "
        "WHERE (fw1.time_id = FROM_UNIXTIME(1466467200)) "
        "AND (dw.url like '%%' OR dw.title like '%%') AND fw1.vertical_id IN "
        "('0','1','2','3','4','5','6','7')  AND (fw1.wiki_id NOT "
        "IN ('23312','70256','168929','463633','381622','1089624')) "
        "AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"
    )
    assert parser.tables_aliases == {
        "dw": "dimension_wikis",
        "fw1": "fact_wam_scores",
        "fw2": "fact_wam_scores",
    }
    assert parser.columns == [
        "fact_wam_scores.wiki_id",
        "fact_wam_scores.time_id",
        "dimension_wikis.wiki_id",
        "dimension_wikis.url",
        "dimension_wikis.title",
        "fact_wam_scores.vertical_id",
    ]
    assert parser.columns_dict == {
        "select": ["fact_wam_scores.wiki_id"],
        "join": [
            "fact_wam_scores.wiki_id",
            "fact_wam_scores.time_id",
            "dimension_wikis.wiki_id",
        ],
        "where": [
            "fact_wam_scores.time_id",
            "dimension_wikis.url",
            "dimension_wikis.title",
            "fact_wam_scores.vertical_id",
            "fact_wam_scores.wiki_id",
        ],
    }


def test_columns_with_comments():
    parser = Parser(
        "INSERT /* VoteHelper::addVote xxx */  "
        "INTO `page_vote` (article_id,user_id,`time`) "
        "VALUES ('442001','27574631','20180228130846')"
    )
    assert parser.query_type == QueryType.INSERT
    assert parser.columns == ["article_id", "user_id", "time"]

    # REPLACE queries
    parser = Parser(
        "REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) "
        "VALUES ('47','infoboxes','')"
    )
    assert parser.query_type == QueryType.REPLACE
    assert parser.columns == ["pp_page", "pp_propname", "pp_value"]
    assert parser.columns_dict == {"insert": ["pp_page", "pp_propname", "pp_value"]}

    assert Parser(
        "SELECT /* CategoryPaginationViewer::processSection */  "
        "page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix  "
        "FROM `page` "
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

    parser = Parser(
        """WITH aa AS --sdfsdfsdf 
        (SELECT C1, C2 FROM T1) 
        SELECT C1, C2 FROM aa"""
    )
    assert parser.columns == ["C1", "C2"]
    assert parser.columns_dict == {"select": ["C1", "C2"]}


def test_columns_with_keyword_aliases():
    parser = Parser(
        "SELECT date_format(time_id,'%Y-%m-%d') AS date, pageviews AS cnt         "
        "FROM rollup_wiki_pageviews      "
        "WHERE period_id = '2'   "
        "AND wiki_id = '1676379'         "
        "AND time_id BETWEEN '2018-01-08'        "
        "AND '2018-01-01'"
    )
    assert parser.columns == ["time_id", "pageviews", "period_id", "wiki_id"]
    assert parser.columns_aliases_names == ["date", "cnt"]


def test_columns_and_sql_functions():
    """
    See https://github.com/macbre/sql-metadata/issues/125
    """
    assert Parser("SELECT max(col3)+avg(col)+1+sum(col2) from dual").columns == [
        "col3",
        "col",
        "col2",
    ]
    assert Parser("SELECT avg(col)+sum(col2) from dual").columns == ["col", "col2"]
    assert Parser(
        "SELECT count(col)+max(col2)+ min(col3)+ count(distinct  col4) + "
        "custom_func(col5) from dual"
    ).columns == ["col", "col2", "col3", "col4", "col5"]


def test_columns_starting_with_keywords():
    query = """
    SELECT `schema_name`, full_table_name, `column_name`, `catalog_name`,
    `table_name`, column_length, column_weight, annotation
    FROM corporate.all_tables
    """
    parser = Parser(query)
    assert parser.columns == [
        "schema_name",
        "full_table_name",
        "column_name",
        "catalog_name",
        "table_name",
        "column_length",
        "column_weight",
        "annotation",
    ]


def test_columns_as_unquoted_keywords():
    query = """
    SELECT schema_name, full_table_name, column_name, catalog_name,
    table_name, column_length, column_weight, annotation
    FROM corporate.all_tables
    """
    parser = Parser(query)
    assert parser.columns == [
        "schema_name",
        "full_table_name",
        "column_name",
        "catalog_name",
        "table_name",
        "column_length",
        "column_weight",
        "annotation",
    ]


def test_columns_with_keywords_parts():
    query = """
    SELECT column_length, column_weight, table_random, drop_20, create_table
    FROM sample_table
    """
    assert Parser(query).columns == [
        "column_length",
        "column_weight",
        "table_random",
        "drop_20",
        "create_table",
    ]


def test_columns_with_complex_aliases_same_as_columns():
    query = """
    SELECT targetingtype, sellerid, sguid, 'd01' as datetype, adgroupname, targeting,
    customersearchterm,
    'product_search_term' as `type`,
    sum(impressions) as impr,
    sum(clicks) as clicks,
    sum(seventotalunits) as sold,
    sum(sevenadvertisedskuunits) as advertisedskuunits,
    sum(sevenotherskuunits) as otherskuunits,
    sum(sevendaytotalsales) as totalsales,
    round(sum(spend), 4) as spend, if(sum(impressions) > 0,
    round(sum(clicks)/sum(impressions), 4), 0) as ctr,
    if(sum(clicks) > 0, round(sum(seventotalunits)/sum(clicks), 4), 0) as cr,
    if(sum(clicks) > 0, round(sum(spend)/sum(clicks), 2), 0) as cpc
    from amazon_pl.search_term_report_impala
    where reportday >= to_date('2021-05-16 00:00:00.0')
    and reportday <= to_date('2021-05-16 00:00:00.0')
    and targetingtype in ('auto','manual')
    and sguid is not null and sguid != ''
    group by targetingtype,sellerid,sguid,adgroupname,targeting,customersearchterm
    order by impr desc
    """
    parser = Parser(query)
    assert parser.columns == [
        "targetingtype",
        "sellerid",
        "sguid",
        "adgroupname",
        "targeting",
        "customersearchterm",
        "impressions",
        "clicks",
        "seventotalunits",
        "sevenadvertisedskuunits",
        "sevenotherskuunits",
        "sevendaytotalsales",
        "spend",
        "reportday",
    ]


def test_columns_aliases_as_unqoted_keywords():
    query = """
    SELECT
    product_search_term as type,
    sum(clicks) as clicks,
    sum(seventotalunits) as schema_name,
    sum(sevenadvertisedskuunits) as advertisedskuunits
    from amazon_pl.search_term_report_impala
    """
    parser = Parser(query)
    assert parser.columns == [
        "product_search_term",
        "clicks",
        "seventotalunits",
        "sevenadvertisedskuunits",
    ]
    assert parser.columns_aliases_names == [
        "type",
        "clicks",
        "schema_name",
        "advertisedskuunits",
    ]
    assert parser.columns_aliases == {
        "advertisedskuunits": "sevenadvertisedskuunits",
        "schema_name": "seventotalunits",
        "type": "product_search_term",
    }


def test_columns_with_aliases_same_as_columns():
    query = """
    SELECT
    round(sum(impressions),1) as impressions,
    sum(clicks) as clicks
    from amazon_pl.search_term_report_impala
    """
    parser = Parser(query)
    assert parser.columns == ["impressions", "clicks"]
    assert parser.columns_aliases == {}

    query = """
    SELECT
    if(sum(clicks) > 0, round(sum(seventotalunits)/sum(clicks), 4), 0) as clicks,
    if(sum(clicks) > 0, round(sum(spend)/sum(clicks), 2), 0) as cpc
    from amazon_pl.search_term_report_impala
    """
    parser = Parser(query)
    assert parser.columns == ["clicks", "seventotalunits", "spend"]
    assert parser.columns_aliases == {
        "clicks": ["clicks", "seventotalunits"],
        "cpc": ["clicks", "spend"],
    }


def test_columns_with_distinct():
    query = "SELECT DISTINCT customer_id FROM table"
    parser = Parser(query)
    assert parser.columns == ["customer_id"]
    assert parser.columns_dict == {"select": ["customer_id"]}


def test_getting_columns_dict_with_distinct():
    query = "select a from tb1 where b in (select distinct b from tb2)"
    parsed = Parser(query)
    assert parsed.columns_dict == {"select": ["a", "b"], "where": ["b"]}
    assert parsed.columns == ["a", "b"]


def test_aliases_switching_column_names():
    query = "select a as b, b as a from tb"
    parsed = Parser(query)
    assert parsed.columns == ["a", "b"]
    assert parsed.columns_dict == {"select": ["a", "b"]}


def test_having_columns():
    query = """
    SELECT Country
    FROM Customers
    GROUP BY Country
    HAVING COUNT(CustomerID) > 5;
    """
    parsed = Parser(query)
    assert parsed.columns == ["Country", "CustomerID"]
    assert parsed.columns_dict == {
        "select": ["Country"],
        "group_by": ["Country"],
        "having": ["CustomerID"],
    }


def test_nested_queries():
    query = """
    SELECT max(dt) FROM
        (
         SELECT max(dt) as dt FROM t      
      UNION ALL
          SELECT max(dt) as dt FROM t2
        )
    """
    parser = Parser(query)
    assert parser.columns == ["dt"]
    assert parser.columns_dict == {"select": ["dt"]}

    query = """
    SELECT max(dt) FROM
        (
         SELECT max(dt) as dt FROM t      
        )
    """
    parser = Parser(query)
    assert parser.columns == ["dt"]
    assert parser.columns_dict == {"select": ["dt"]}

    query = """
    SELECT max(dt) FROM
        (
         SELECT dt FROM t      
        )
    """
    parser = Parser(query)
    assert parser.columns == ["dt"]
    assert parser.columns_dict == {"select": ["dt"]}
