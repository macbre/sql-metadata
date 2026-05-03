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


def test_lateral_view_not_in_tables():
    # Solved: https://github.com/macbre/sql-metadata/issues/369
    # LATERAL VIEW aliases should not appear as tables
    parser = Parser("""SELECT event_day, action_type
        FROM t
        LATERAL VIEW EXPLODE(ARRAY(1, 2)) lv AS action_type""")
    assert parser.tables == ["t"]
    assert parser.columns == ["event_day", "action_type"]


def test_array_subscript_with_lateral_view():
    # Solved: https://github.com/macbre/sql-metadata/issues/369
    # Array subscript [n] should not trigger MSSQL bracketed dialect
    parser = Parser("""SELECT max(split(fourth_category, '~')[2]) AS ch_4th_class
        FROM t
        LATERAL VIEW EXPLODE(ARRAY(1, 2)) lv AS action_type""")
    assert parser.tables == ["t"]


def test_complex_lateral_view_with_array_subscript():
    # Solved: https://github.com/macbre/sql-metadata/issues/369
    parser = Parser("""select
        event_day,
        cuid,
        event_product_all,
        max(os_name) as os_name,
        max(app_version) as app_version,
        max(if(event_product_all ='tomas',
            if(is_bdapp_new='1',ch_4th_class,'-'),ta.channel)) as channel,
        max(age) as age,
        max(age_point) as age_point,
        max(is_bdapp_new) as is_new,
        action_type,
        max(if(is_feed_dau=1, immersive_type, 0)) AS detail_page_type
    from
    (
        select  event_day,
                event_product_all,
                os_name,
                app_version,
                channel,
                age,
                age_point,
                is_bdapp_new,
                action_type,
                is_feed_dau,
                immersive_type,
                attr_channel
        from bdapp_ads_bhv_cuid_all_1d
        lateral view explode(array(
            case when is_bdapp_dau=1 then 'bdapp' end,
            case when is_feed_dau=1 then 'feed' end,
            case when is_search_dau=1 then 'search' end,
            case when is_novel_dau=1 then 'novel' end,
            case when is_tts_dau=1 then 'radio' end
            )) lv AS action_type
        lateral view explode(
            case when event_product = 'lite'
                and appid in ('hao123', 'flyflow', 'lite_mission')
                then array('lite', appid)
            when event_product = 'lite' and appid = '10001'
                then array('lite', 'purelite')
            else array(event_product) end
            ) lv AS event_product_all
        where event_day in ('20230102')
            and event_product in ('lite', 'tomas')
            and is_bdapp_dau = '1'
            and action_type is not null
    )ta
    left outer join
    (
        select channel,max(split(fourth_category,'~')[2]) as ch_4th_class
        from udw_ns.default.ug_dim_channel_new_df
        where event_day = '20230102'
        group by  channel
    )tb on ta.attr_channel=tb.channel
    group by event_day, cuid, event_product_all, action_type
    limit 100""")
    assert parser.tables == [
        "bdapp_ads_bhv_cuid_all_1d",
        "udw_ns.default.ug_dim_channel_new_df",
    ]
