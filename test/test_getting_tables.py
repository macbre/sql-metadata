from sql_metadata.parser import Parser


def test_simple_queries_tables():
    assert ["test_table"] == Parser("SELECT * FROM `test_table`").tables

    assert ["0001_test_table"] == Parser("SELECT * FROM `0001_test_table`").tables

    assert ["test_table"] == Parser("SELECT foo FROM `test_table`").tables

    assert ["s.t"] == Parser("SELECT * FROM s.t").tables

    assert ["db.test_table"] == Parser("SELECT foo FROM `db`.`test_table`").tables

    assert ["test_table"] == Parser("SELECT foo FROM test_table WHERE id = 1").tables

    assert ["test_table", "second_table"] == Parser(
        "SELECT foo FROM test_table, second_table WHERE id = 1"
    ).tables

    assert ["test_table", "second_table"] == Parser(
        "SELECT foo FROM test_table CROSS JOIN second_table WHERE id = 1"
    ).tables

    assert ["revision", "page", "wikicities_user"] == Parser(
        "SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,"
        "rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_shaN,"
        "page_namespace,page_title,page_id,page_latest,user_name "
        "FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) "
        "LEFT JOIN `wikicities_user` ON ((rev_user != N) AND (user_id = rev_user)) "
        "WHERE rev_id = X LIMIT N"
    ).tables

    assert ["events"] == Parser(
        "SELECT COUNT( 0 ) AS cnt, date_format(event_date, '%Y-%m-%d') AS date 	 "
        "FROM events 	 WHERE event_date BETWEEN '2017-10-18 00:00:00' 	 "
        "AND '2017-10-24 23:59:59'  	 "
        "AND wiki_id = '1289985' GROUP BY date WITH ROLLUP"
    ).tables


def test_complex_query_tables():
    # complex queries
    # @see https://github.com/macbre/query-digest/issues/16
    assert ["report_wiki_recent_pageviews", "dimension_wikis"] == Parser(
        "SELECT r.wiki_id AS id, pageviews_Nday AS pageviews "
        "FROM report_wiki_recent_pageviews AS r "
        "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id "
        "WHERE d.public = X AND r.lang = X AND r.hub_name = X "
        "ORDER BY pageviews DESC LIMIT N"
    ).tables

    assert ["dimension_wikis", "fact_wam_scores"] == Parser(
        "SELECT DISTINCT dw.lang FROM `dimension_wikis` `dw` "
        "INNER JOIN `fact_wam_scores` `fwN` ON ((dw.wiki_id = fwN.wiki_id)) "
        "WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC"
    ).tables

    assert ["fact_wam_scores", "dimension_wikis"] == Parser(
        "SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` `fwN` "
        "left join `fact_wam_scores` `fwN` ON ((fwN.wiki_id = fwN.wiki_id) "
        "AND (fwN.time_id = FROM_UNIXTIME(N))) "
        "left join `dimension_wikis` `dw` ON ((fwN.wiki_id = dw.wiki_id)) "
        "WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) "
        "AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) "
        "AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"
    ).tables

    assert ["revision", "page", "wikicities_cN.user"] == Parser(
        "SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,"
        "rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_shaN,"
        "page_namespace,page_title,page_id,page_latest,user_name "
        "FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) "
        "LEFT JOIN `wikicities_cN`.`user` ON ((rev_user != N) "
        "AND (user_id = rev_user)) WHERE rev_id = X LIMIT N"
    ).tables

    # complex queries, take two
    # @see https://github.com/macbre/sql-metadata/issues/6
    assert ["foo_pageviews"] == Parser(
        "SELECT 1 as c    FROM foo_pageviews      "
        "WHERE time_id = '2018-01-07 00:00:00'   AND period_id = '2' LIMIT 1"
    ).tables

    # table aliases
    assert ["report_wiki_recent_pageviews", "dimension_wikis"] == Parser(
        "SELECT r.wiki_id AS id, pageviews_7day AS pageviews "
        "FROM report_wiki_recent_pageviews AS r "
        "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id "
        "WHERE d.public = '1' AND r.lang IN ( 'en', 'ru' ) "
        "AND r.hub_name = 'gaming' ORDER BY pageviews DESC LIMIT 300"
    ).tables

    # include multiple FROM tables when they prefixed
    # @see https://github.com/macbre/sql-metadata/issues/38
    assert ["MYDB1.TABLE1", "MYDB2.TABLE2"] == Parser(
        "SELECT A.FIELD1, B.FIELD1, (A.FIELD1 * B.FIELD1) AS QTY "
        "FROM MYDB1.TABLE1 AS A, MYDB2.TABLE2 AS B"
    ).tables

    # test whitespaces in keywords
    # @see https://github.com/macbre/sql-metadata/issues/80
    assert (
        ["tab", "tab2"]
        == Parser(
            """SELECT a,b,c from tab
    full  outer \r\n\t  join tab2  on (col1 = col2) group
\r\n   \t   by  a, b, c """
        ).tables
    )


def test_joins():
    # self joins
    assert ["fact_wam_scores", "dimension_wikis"] == Parser(
        "SELECT  count(fw1.wiki_id) as wam_results_total  "
        "FROM `fact_wam_scores` `fw1` left join `fact_wam_scores` `fw2` "
        "ON ((fw1.wiki_id = fw2.wiki_id) "
        "AND (fw2.time_id = FROM_UNIXTIME(1466380800))) "
        "left join `dimension_wikis` `dw` ON ((fw1.wiki_id = dw.wiki_id))  "
        "WHERE (fw1.time_id = FROM_UNIXTIME(1466467200)) "
        "AND (dw.url like '%%' OR dw.title like '%%') "
        "AND fw1.vertical_id IN ('0','1','2','3','4','5','6','7')  "
        "AND (fw1.wiki_id NOT IN ('23312','70256','168929','463633','381622','524772',"
        "'476782','9764','214934','170145','529622','52149','96420','390','468156',"
        "'690804','197434','29197','88043','37317','466775','402313','169142','746246',"
        "'119847','57268','1089624')) "
        "AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"
    ).tables

    assert ["rollup_wiki_pageviews"] == Parser(
        "SELECT date_format(time_id,'%Y-%m-%d') AS date, pageviews AS cnt         "
        "FROM rollup_wiki_pageviews      "
        "WHERE period_id = '2'   "
        "AND wiki_id = '1676379'         "
        "AND time_id BETWEEN '2018-01-08'        "
        "AND '2018-01-01'"
    ).tables

    # JOINs
    assert ["product_a.users", "product_b.users"] == Parser(
        "SELECT a.* FROM product_a.users AS a "
        "JOIN product_b.users AS b ON a.ip_address = b.ip_address"
    ).tables

    assert ["redirect", "page"] == Parser(
        "SELECT  page_title  FROM `redirect` INNER JOIN `page` "
        "ON (rd_title = 'foo' AND rd_namespace = '100' AND (page_id = rd_from))"
    ).tables

    assert ["redirect", "page"] == Parser(
        "SELECT  page_title  FROM `redirect` INNER JOIN `page` `foo` "
        "ON (rd_title = 'foo' AND rd_namespace = '100' AND (foo.page_id = rd_from))"
    ).tables

    # see #34
    assert ["foos", "bars"] == Parser(
        "SELECT foo FROM `foos` JOIN `bars` ON (foos.id = bars.id)"
    ).tables

    assert ["foos", "bars"] == Parser(
        "SELECT foo FROM `foos` FULL JOIN `bars` ON (foos.id = bars.id)"
    ).tables

    assert ["foos", "bars"] == Parser(
        "SELECT foo FROM `foos` FULL OUTER JOIN `bars` ON (foos.id = bars.id)"
    ).tables

    assert ["foos", "bars"] == Parser(
        "SELECT foo FROM `foos` RIGHT OUTER JOIN `bars` ON (foos.id = bars.id)"
    ).tables

    assert ["foos", "bars"] == Parser(
        "SELECT foo FROM `foos` LEFT OUTER JOIN `bars` ON (foos.id = bars.id)"
    ).tables

    assert ["foos", "bars"] == Parser("SELECT foo FROM `foos` CROSS JOIN `bars`").tables


def test_quoted_names():
    # handle quoted names
    assert ["MYDB.MYTABLE"] == Parser('SELECT COUNT(*) FROM "MYDB".MYTABLE').tables

    assert ["MYDB.MYTABLE"] == Parser('SELECT COUNT(*) FROM MYDB."MYTABLE"').tables

    assert ["MYDB.MYTABLE"] == Parser('SELECT COUNT(*) FROM "MYDB"."MYTABLE"').tables

    assert ["MYDB.MYSCHEMA.MYTABLE"] == Parser(
        'SELECT COUNT(*) FROM "MYDB".MYSCHEMA.MYTABLE'
    ).tables

    assert ["MYDB.MYSCHEMA.MYTABLE"] == Parser(
        'SELECT COUNT(*) FROM MYDB."MYSCHEMA".MYTABLE'
    ).tables

    assert ["MYDB.MYSCHEMA.MYTABLE"] == Parser(
        'SELECT COUNT(*) FROM MYDB.MYSCHEMA."MYTABLE"'
    ).tables

    assert ["MYDB.MYSCHEMA.MYTABLE"] == Parser(
        'SELECT COUNT(*) FROM "MYDB"."MYSCHEMA"."MYTABLE"'
    ).tables


def test_update_and_replace():
    # UPDATE queries
    assert ["page"] == Parser(
        "UPDATE `page` SET page_touched = X WHERE page_id = X"
    ).tables

    # REPLACE queries
    assert ["page_props"] == Parser(
        "REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) "
        "VALUES ('47','infoboxes','')"
    ).tables


def test_order_bys():
    # ORDER BY
    assert ["bar"] == Parser("SELECT foo FROM bar ORDER BY id").tables

    assert ["bar"] == Parser("SELECT foo FROM bar WHERE id > 20 ORDER BY id").tables

    assert ["bar"] == Parser("SELECT foo FROM bar ORDER BY id DESC").tables

    assert ["bar"] == Parser("SELECT foo FROM bar ORDER BY id LIMIT 20").tables


def test_three_part_qualified_names():
    # database.schema.table formats
    assert ["MYDB1.MYSCHEMA1.MYTABLE1"] == Parser(
        "SELECT * FROM MYDB1.MYSCHEMA1.MYTABLE1"
    ).tables

    assert ["MYDB1.MYSCHEMA1.MYTABLE1", "MYDB2.MYSCHEMA2.MYTABLE2"] == Parser(
        "SELECT * FROM MYDB1.MYSCHEMA1.MYTABLE1 JOIN MYDB2.MYSCHEMA2.MYTABLE2"
    ).tables

    assert ["MYDB1.MYSCHEMA1.MYTABLE1", "MYDB2.MYSCHEMA2.MYTABLE2"] == Parser(
        "SELECT * FROM MYDB1.MYSCHEMA1.MYTABLE1 INNER JOIN MYDB2.MYSCHEMA2.MYTABLE2"
    ).tables

    assert ["MYDB1.MYSCHEMA1.MYTABLE1", "MYDB2.MYSCHEMA2.MYTABLE2"] == Parser(
        "SELECT * FROM MYDB1.MYSCHEMA1.MYTABLE1 A "
        "LEFT JOIN MYDB2.MYSCHEMA2.MYTABLE2 B ON A.COL = B.COL"
    ).tables

    assert ["MYDB1.MYSCHEMA1.MYTABLE1", "MYDB2.MYSCHEMA2.MYTABLE2"] == Parser(
        "SELECT * FROM MYDB1.MYSCHEMA1.MYTABLE1 INNER JOIN MYDB2.MYSCHEMA2.MYTABLE2"
    ).tables


def test_insert_queries():
    # INSERT queries
    assert ["0070_insert_ignore_table"] == Parser(
        "INSERT IGNORE INTO `0070_insert_ignore_table` VALUES (9, '123', '2017-01-01');"
    ).tables

    assert ["0070_insert_ignore_table"] == Parser(
        "INSERT into `0070_insert_ignore_table` VALUES (9, '123', '2017-01-01');"
    ).tables

    assert ["foo"] == Parser("INSERT INTO `foo` (id,text) VALUES (X,X)").tables

    assert ["page_vote"] == Parser(
        "INSERT /* VoteHelper::addVote xxx */  "
        "INTO `page_vote` (article_id,user_id,time) "
        "VALUES ('442001','27574631','20180228130846')"
    ).tables


def test_select_aliases():
    assert Parser("SELECT e.foo FROM bar AS e").tables == ["bar"]
    assert Parser("SELECT e.foo FROM bar e").tables == ["bar"]
    assert Parser("SELECT e.foo FROM (SELECT * FROM bar) AS e").tables == ["bar"]
    assert Parser("SELECT e.foo FROM (SELECT * FROM bar) e").tables == ["bar"]


def test_table_name_with_group_by():
    expected_tables = ["SH.sales"]

    assert (
        Parser("SELECT s.cust_id,count(s.cust_id) FROM SH.sales s").tables
        == expected_tables
    )

    assert (
        Parser(
            "SELECT s.cust_id,count(s.cust_id) FROM SH.sales s GROUP BY s.cust_id"
        ).tables
        == expected_tables
    )

    assert (
        Parser(
            """
                    SELECT s.cust_id,count(s.cust_id) FROM SH.sales s
                    GROUP BY s.cust_id HAVING s.cust_id != '1660'
                    AND s.cust_id != '2'
                    """.strip()
        ).tables
        == expected_tables
    )


def test_datasets():
    # see https://github.com/macbre/sql-metadata/issues/38
    assert Parser(
        "SELECT A.FIELD1, B.FIELD1, (A.FIELD1 * B.FIELD1) AS QTY "
        "FROM TABLE1 AS A, TABLE2 AS B"
    ).tables == ["TABLE1", "TABLE2"]

    assert Parser(
        "SELECT A.FIELD1, B.FIELD1, (A.FIELD1 * B.FIELD1) AS QTY "
        "FROM DATASET1.TABLE1, DATASET2.TABLE2"
    ).tables == ["DATASET1.TABLE1", "DATASET2.TABLE2"]

    assert Parser(
        "SELECT A.FIELD1, B.FIELD1, (A.FIELD1 * B.FIELD1) AS QTY "
        "FROM DATASET1.TABLE1 AS A, DATASET2.TABLE2 AS B"
    ).tables == ["DATASET1.TABLE1", "DATASET2.TABLE2"]


def test_queries_with_distinct():
    assert Parser("SELECT DISTINCT DATA.ASSAY_ID FROM foo").tables == ["foo"]


def test_table_names_with_dashes():
    assert Parser("SELECT * FROM `schema-with-dash.tablename`").tables == [
        "schema-with-dash.tablename"
    ]


def test_unions():
    # @see https://github.com/macbre/sql-metadata/issues/79
    assert ["tab1", "tab2"] == Parser(
        "SELECT col1, col2, col3 from tab1 union all SELECT col4, col5, col6 from tab2"
    ).tables

    # @see https://github.com/macbre/sql-metadata/issues/94
    assert ["d", "g"] == Parser(
        "SELECT a,b,c FROM d UNION ALL SELECT e,f FROM g"
    ).tables


def test_with_brackets():
    assert (
        ["database1.table1", "database2.table2"]
        == Parser(
            """
SELECT
"xxxxx"
FROM
(database1.table1 alias
LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx"))
"""
        ).tables
    )

    assert (
        ["inner_table"]
        == Parser(
            """
SELECT
t.foo
FROM
(SELECT foo FROM inner_table
WHERE bar = '1') t
"""
        ).tables
    )


def test_db2_query():
    query = """
    SELECT ca.IDENTIFICATION_CODE identificationCode,
eo.KBO_NUMBER kboNumber,
eo.PARTY_NAME,
ca.total_guaranteed totale_borgtocht,
coalesce(sum(ae1.remainder),0) Saldo,
coalesce(sum(ae3.remainder),0) uitstel_van_betaling,
coalesce(sum(ae4.remainder),0) reservering_aangifte,
coalesce(sum(ae5.remainder),0) reservering_vergunning,
coalesce(sum(ae6.remainder),0) zekerheid_douanevervoer,
coalesce(sum(ae7.remainder),0) zekerheid_accijnsbeweging,
coalesce(sum(ae8.remainder),0) FRCT
from CUSTOMER_ACCOUNT ca
inner join economic_operator eo on eo.id = ca.economic_operator_id
join contact_details cd on cd.id = ca.contact_details_id
left join ( ca1_remainder_total_guaranteed crtg
inner join accounting_entity ae1 on ae1.id = crtg.accounting_entity_id)
on crtg.id = ca.ca1_id
left join (ca3_credit_account cca inner join accounting_entity ae3 on ae3.id =
cca.accounting_entity_id) on cca.id = ca.ca3_id
left join (ca4_reservations_declaration crd inner join accounting_entity ae4 on
ae4.id = crd.accounting_entity_id) on crd.id = ca.ca4_id
left join (ca5_reservations_permits crp inner join accounting_entity ae5 on ae5.id
= crp.accounting_entity_id) on crp.id = ca.ca5_id
left join (CA6_GUARANTEE_CUSTOMS_TRANSPORT gct inner join accounting_entity ae6 on
ae6.id = gct.accounting_entity_id) on gct.id = ca.ca6_id
left join (CA7_GUARANTEE_EXCISE_PRODUCTS gep inner join accounting_entity ae7 on
ae7.id = gep.accounting_entity_id) on gep.id = ca.ca7_id
left join (ca8_frct cf inner join ca8_frct_per_discharge cfpd on cfpd.CA8_ID =
cf.id inner join accounting_entity ae8 on ae8.id = cfpd.accounting_entity_id) on
cf.id = ca.ca8_id
group by eo.PARTY_NAME,eo.KBO_NUMBER, ca.IDENTIFICATION_CODE, ca.total_guaranteed
order by eo.KBO_NUMBER, ca.IDENTIFICATION_CODE
with ur
    """
    parser = Parser(query)
    assert parser.tables == [
        "CUSTOMER_ACCOUNT",
        "economic_operator",
        "contact_details",
        "ca1_remainder_total_guaranteed",
        "accounting_entity",
        "ca3_credit_account",
        "ca4_reservations_declaration",
        "ca5_reservations_permits",
        "CA6_GUARANTEE_CUSTOMS_TRANSPORT",
        "CA7_GUARANTEE_EXCISE_PRODUCTS",
        "ca8_frct",
        "ca8_frct_per_discharge",
    ]
    assert parser.columns == [
        "CUSTOMER_ACCOUNT.IDENTIFICATION_CODE",
        "economic_operator.KBO_NUMBER",
        "economic_operator.PARTY_NAME",
        "CUSTOMER_ACCOUNT.total_guaranteed",
        "accounting_entity.remainder",
        "economic_operator.id",
        "CUSTOMER_ACCOUNT.economic_operator_id",
        "contact_details.id",
        "CUSTOMER_ACCOUNT.contact_details_id",
        "accounting_entity.id",
        "ca1_remainder_total_guaranteed.accounting_entity_id",
        "ca1_remainder_total_guaranteed.id",
        "CUSTOMER_ACCOUNT.ca1_id",
        "ca3_credit_account.accounting_entity_id",
        "ca3_credit_account.id",
        "CUSTOMER_ACCOUNT.ca3_id",
        "ca4_reservations_declaration.accounting_entity_id",
        "ca4_reservations_declaration.id",
        "CUSTOMER_ACCOUNT.ca4_id",
        "ca5_reservations_permits.accounting_entity_id",
        "ca5_reservations_permits.id",
        "CUSTOMER_ACCOUNT.ca5_id",
        "CA6_GUARANTEE_CUSTOMS_TRANSPORT.accounting_entity_id",
        "CA6_GUARANTEE_CUSTOMS_TRANSPORT.id",
        "CUSTOMER_ACCOUNT.ca6_id",
        "CA7_GUARANTEE_EXCISE_PRODUCTS.accounting_entity_id",
        "CA7_GUARANTEE_EXCISE_PRODUCTS.id",
        "CUSTOMER_ACCOUNT.ca7_id",
        "ca8_frct_per_discharge.CA8_ID",
        "ca8_frct.id",
        "ca8_frct_per_discharge.accounting_entity_id",
        "CUSTOMER_ACCOUNT.ca8_id",
    ]

    assert parser.columns_aliases_names == [
        "identificationCode",
        "kboNumber",
        "totale_borgtocht",
        "Saldo",
        "uitstel_van_betaling",
        "reservering_aangifte",
        "reservering_vergunning",
        "zekerheid_douanevervoer",
        "zekerheid_accijnsbeweging",
        "FRCT",
    ]

    assert parser.columns_aliases == {
        "FRCT": "accounting_entity.remainder",
        "Saldo": "accounting_entity.remainder",
        "identificationCode": "CUSTOMER_ACCOUNT.IDENTIFICATION_CODE",
        "kboNumber": "economic_operator.KBO_NUMBER",
        "reservering_aangifte": "accounting_entity.remainder",
        "reservering_vergunning": "accounting_entity.remainder",
        "totale_borgtocht": "CUSTOMER_ACCOUNT.total_guaranteed",
        "uitstel_van_betaling": "accounting_entity.remainder",
        "zekerheid_accijnsbeweging": "accounting_entity.remainder",
        "zekerheid_douanevervoer": "accounting_entity.remainder",
    }


def test_get_tables_with_leading_digits():
    # see #139

    # Identifiers may begin with a digit
    # but unless quoted may not consist solely of digits.
    assert ["0020"] == Parser("SELECT * FROM `0020`").tables

    assert ["0020_big_table"] == Parser(
        "SELECT t.val as value, count(*) "
        "FROM `0020_big_table` as t WHERE id BETWEEN 10 AND 20 GROUP BY val"
    ).tables
    assert ["0020_big_table"] == Parser(
        "SELECT t.val as value, count(*) FROM `0020_big_table`"
    ).tables
    assert ["0020_big_table"] == Parser(
        "SELECT t.val as value, count(*) "
        'FROM "0020_big_table" as t WHERE id BETWEEN 10 AND 20 GROUP BY val'
    ).tables
    assert ["0020_big_table"] == Parser(
        "SELECT t.val as value, count(*) "
        "FROM 0020_big_table as t WHERE id BETWEEN 10 AND 20 GROUP BY val"
    ).tables
    assert ["0020_big_table"] == Parser(
        "SELECT t.val as value, count(*) "
        "FROM `0020_big_table` as t WHERE id BETWEEN 10 AND 20 GROUP BY val"
    ).tables
    assert ["0020_big_table"] == Parser(
        "SELECT t.val as value, count(*) FROM 0020_big_table"
    ).tables


def test_insert_ignore_with_comments():
    queries = [
        "INSERT IGNORE /* foo */ INTO bar VALUES (1, '123', '2017-01-01');",
        "/* foo */ INSERT IGNORE INTO bar VALUES (1, '123', '2017-01-01');"
        "-- foo\nINSERT IGNORE INTO bar VALUES (1, '123', '2017-01-01');"
        "# foo\nINSERT IGNORE INTO bar VALUES (1, '123', '2017-01-01');",
    ]

    for query in queries:
        assert ["bar"] == Parser(query).tables


def test_mutli_from_aliases_without_as():
    query = """
    select distinct e1.eid as eid from uw_emp "e1", uw_emp "e2", uw_payroll p
    where e1.eid = e2.eid and e1.eid = p.eid
    """
    parser = Parser(query)
    assert parser.tables == ["uw_emp", "uw_payroll"]
    assert parser.tables_aliases == {"p": "uw_payroll", "e1": "uw_emp", "e2": "uw_emp"}


def test_tables_with_aggregation():
    query = """
    SELECT
    tc.col2
    , max(tc.col3) col3_mx
    , trunc(max(tc.col4)) col4_mx
    , extract (week from (max(tc.col5))) col5_week
    , sum(NVL(tc.amt,0)) tot_amt
    FROM
    my_sc.tab1 tc
    WHERE
    tc.col1 = 'AAA'
    group by
    tc.col2
    """
    parser = Parser(query)
    assert parser.tables == ["my_sc.tab1"]


def test_insert_with_on_conflict():
    sql = """
    INSERT INTO global_config (entry_id) VALUES ($1)
    ON CONFLICT (entry_id) UPDATE SET entry_id = $1
    """

    parser = Parser(sql)
    assert parser.tables == ["global_config"]
    assert parser.columns == ["entry_id"]


def test_insert_with_on_conflict_set_name():
    sql = """
    INSERT INTO global_config (`SET`) VALUES ($1)
    ON CONFLICT (`SET`) UPDATE SET `SET` = $1
    """

    parser = Parser(sql)
    assert parser.tables == ["global_config"]
    assert parser.columns == ["SET"]


def test_with_keyword_in_joins():
    query = """
    SELECT DWH_DM.COM_MONITOR_OPT_TREND.OC_YM_REF,
    CALENDAR_OPT_TREND.YM_REF, DWH_DM.COM_MONITOR_OPT_TREND.RULE,
    OPT_TREND_RANKING.QNTY_YM_NG, DWH_DM.COM_MONITOR_OPT_TREND.ABS_OC,
    DWH_DM.COM_MONITOR_OPT_TREND.EFFC_CPTY_N3, DWH_DM.COM_MONITOR_OPT_TREND.ABS_PIP,
    DWH_DM.COM_MONITOR_OPT_TREND.EFFC_CPTY_N3-DWH_DM.COM_MONITOR_OPT_TREND.ABS_OC,
    DWH_DM.COM_MONITOR_OPT_TREND.EFFC_CPTY_N3-DWH_DM.COM_MONITOR_OPT_TREND.ABS_PIP
    FROM ( 
        SELECT RULE , EFFC_CPTY_N3-ABS_PIP QNTY_YM_NG 
        FROM DWH_DM.COM_MONITOR_OPT_TREND 
        WHERE OC_YM_REF = (
        SELECT MAX(OC_YM_REF)
        FROM DWH_DM.COM_MONITOR_OPT_TREND LAST_MONTH) 
        ORDER BY (EFFC_CPTY_N3-ABS_PIP) DESC ) OPT_TREND_RANKING 
    RIGHT OUTER JOIN DWH_DM.COM_MONITOR_OPT_TREND 
    ON (DWH_DM.COM_MONITOR_OPT_TREND.RULE=OPT_TREND_RANKING.RULE) 
    INNER JOIN ( 
        WITH CTE_RANGE AS (
        SELECT TO_CHAR(ADD_MONTHS(TO_DATE(MAX(OC_YM_REF), 'YYYYMM'),-11),'YYYYMM')
        AS START_MONTH, MAX(OC_YM_REF) AS END_MONTH FROM DWH_DM.COM_MONITOR_OPT_TREND) 
        SELECT YM_REF, 
        TO_CHAR(TO_DATE(YM_REF,'YYYYMM'), 'Mon', 'NLS_DATE_LANGUAGE = ENGLISH') 
        MONTH_REF FROM DWH_DM.FER_D_CALENDAR_MONTH_SLS 
        INNER JOIN CTE_RANGE 
        ON YM_REF BETWEEN CTE_RANGE.START_MONTH AND CTE_RANGE.END_MONTH ORDER BY YM_REF)
        CALENDAR_OPT_TREND 
        ON (CALENDAR_OPT_TREND.YM_REF=DWH_DM.COM_MONITOR_OPT_TREND.OC_YM_REF) 
    WHERE ( DWH_DM.COM_MONITOR_OPT_TREND.RULE NOT IN ('DELY') )
    """
    parser = Parser(query)
    assert parser.tables == [
        "DWH_DM.COM_MONITOR_OPT_TREND",
        "DWH_DM.FER_D_CALENDAR_MONTH_SLS",
    ]

    assert parser.columns == [
        "DWH_DM.COM_MONITOR_OPT_TREND.OC_YM_REF",
        "YM_REF",
        "DWH_DM.COM_MONITOR_OPT_TREND.RULE",
        "EFFC_CPTY_N3",
        "ABS_PIP",
        "DWH_DM.COM_MONITOR_OPT_TREND.ABS_OC",
        "DWH_DM.COM_MONITOR_OPT_TREND.EFFC_CPTY_N3",
        "DWH_DM.COM_MONITOR_OPT_TREND.ABS_PIP",
        "RULE",
        "OC_YM_REF",
    ]

    assert parser.with_names == ["CTE_RANGE"]


def test_getting_proper_tables_with_keyword_aliases():
    # use SQL keywords as table aliases
    assert Parser("SELECT system.bar FROM foo AS system").tables == ["foo"]
    assert Parser("SELECT system.bar FROM foo system").tables == ["foo"]

    assert Parser(
        "SELECT system.bar FROM foo system union all select * from b user"
    ).tables == ["foo", "b"]

    query = """
    SELECT system.bar FROM foo system 
    join select user.asd as val from b user on user.gfd=system.bar
    """

    assert Parser(query).tables == ["foo", "b"]
    assert Parser(query).columns == ["foo.bar", "b.asd", "b.gfd"]
    assert Parser(query).columns_aliases == {"val": "b.asd"}
    assert Parser(query).tables_aliases == {"system": "foo", "user": "b"}


def test_cross_join_with_subquery():
    query = "SELECT foo FROM `foos` CROSS JOIN (SELECT * FROM `bars`) AS `foobar`"
    parser = Parser(query)
    assert parser.tables == ["foos", "bars"]
    assert parser.subqueries_names == ["foobar"]
    assert parser.subqueries == {
        "foobar": "SELECT * FROM bars",
    }


def test_insert_into_on_duplicate_key_ipdate():
    assert Parser(
        "INSERT INTO user (id, name, age)"
        " VALUES ('user1', 'john doe', 20)"
        " ON DUPLICATE KEY UPDATE name='john doe', age=20"
    ).tables == ["user"]


def test_join_followed_by_tables():
    query = """
    SELECT 
        web_site_id, 
        sum(sales_price) AS sales, 
        sum(profit) AS profit, 
        sum(return_amt) AS returns1, 
        sum(net_loss) AS profit_loss 
    FROM 
        (
        SELECT 
            ws_web_site_sk AS wsr_web_site_sk, 
            ws_sold_date_sk AS date_sk, 
            ws_ext_sales_price AS sales_price, 
            ws_net_profit AS profit, 
            cast(
            0 AS decimal(7, 2)
            ) AS return_amt, 
            cast(
            0 AS decimal(7, 2)
            ) AS net_loss 
        FROM 
            web_sales 
        UNION ALL 
        SELECT 
            ws_web_site_sk AS wsr_web_site_sk, 
            wr_returned_date_sk AS date_sk, 
            cast(
            0 AS decimal(7, 2)
            ) AS sales_price, 
            cast(
            0 AS decimal(7, 2)
            ) AS profit, 
            wr_return_amt AS return_amt, 
            wr_net_loss AS net_loss 
        FROM 
            web_returns 
            LEFT OUTER JOIN web_sales ON (
            wr_item_sk = ws_item_sk 
            AND wr_order_number = ws_order_number
            )
        ) salesreturns, 
        date_dim, 
        web_site 
    WHERE 
        date_sk = d_date_sk 
        AND d_date BETWEEN cast('2002-08-22' AS date) 
        AND (
        cast('2002-08-22' AS date) + INTERVAL '14' day
        ) 
        AND wsr_web_site_sk = web_site_sk 
    GROUP BY 
        web_site_id
    """
    parser = Parser(query)
    assert parser.tables == ["web_sales", "web_returns", "date_dim", "web_site"]

    parser = Parser(
        """
    SELECT * 
    FROM Sales 
        JOIN Customers 
            ON Sales.CustomerID = Customers.CustomerID, 
        (SELECT MAX(Revenue) FROM Sales),
        Stores
    """
    )
    assert parser.tables == ["Sales", "Customers", "Stores"]


def test_subquery_followed_by_tables():
    query = """
    SELECT top 100 c_last_name ,
           c_first_name ,
           ca_city ,
           bought_city ,
           ss_ticket_number ,
           amt,
           profit
    FROM
    (SELECT ss_ticket_number ,
            ss_customer_sk ,
            ca_city bought_city ,
            sum(ss_coupon_amt) amt ,
            sum(ss_net_profit) profit
    FROM store_sales,
            date_dim,
            store,
            household_demographics,
            customer_address
    WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
        AND store_sales.ss_store_sk = store.s_store_sk
        AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        AND store_sales.ss_addr_sk = customer_address.ca_address_sk
        AND (household_demographics.hd_dep_count = 3
            OR household_demographics.hd_vehicle_count= 2)
        AND date_dim.d_dow IN (6,
                                0)
        AND date_dim.d_year IN (1998,
                                1998+1,
                                1998+2)
        AND store.s_city IN ('Oak Grove',
                            'Fairview',
                            'Riverside',
                            'Five Points',
                            'Midway')
    GROUP BY ss_ticket_number,
                ss_customer_sk,
                ss_addr_sk,
                ca_city) dn,
        customer,
        customer_address current_addr
    WHERE ss_customer_sk = c_customer_sk
    AND customer.c_current_addr_sk = current_addr.ca_address_sk
    AND current_addr.ca_city <> bought_city
    ORDER BY c_last_name ,
            c_first_name ,
            ca_city ,
            bought_city ,
            ss_ticket_number
    """

    parser = Parser(query)
    assert parser.tables == [
        "store_sales",
        "date_dim",
        "store",
        "household_demographics",
        "customer_address",
        "customer",
    ]
