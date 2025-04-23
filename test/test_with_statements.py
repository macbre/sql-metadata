import pytest

from sql_metadata import Parser
from sql_metadata.keywords_lists import QueryType


def test_with_statements():
    parser = Parser(
        """
WITH
database1.tableFromWith AS (SELECT aa.* FROM table3 as aa
                            left join table4 on aa.col1=table4.col2),
test as (SELECT * from table3)
SELECT
"xxxxx"
FROM
database1.tableFromWith alias
LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx")
"""
    )
    assert parser.tables == ["table3", "table4", "database2.table2"]
    assert parser.with_names == ["database1.tableFromWith", "test"]
    assert parser.with_queries == {
        "database1.tableFromWith": "SELECT aa.* FROM table3 as aa left join table4 on "
        "aa.col1 = table4.col2",
        "test": "SELECT * from table3",
    }
    parser = Parser(
        """
WITH
database1.tableFromWith AS (SELECT * FROM table3),
database1.tableFromWith2 AS (SELECT * FROM table4),
database1.tableFromWith3 AS (SELECT * FROM table5),
database1.tableFromWith4 AS (SELECT * FROM table6)
SELECT
"xxxxx"
FROM
database1.tableFromWith alias
LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx")
"""
    )

    assert parser.with_names == [
        "database1.tableFromWith",
        "database1.tableFromWith2",
        "database1.tableFromWith3",
        "database1.tableFromWith4",
    ]
    assert parser.with_queries == {
        "database1.tableFromWith": "SELECT * FROM table3",
        "database1.tableFromWith2": "SELECT * FROM table4",
        "database1.tableFromWith3": "SELECT * FROM table5",
        "database1.tableFromWith4": "SELECT * FROM table6",
    }
    assert parser.tables == ["table3", "table4", "table5", "table6", "database2.table2"]

    parser = Parser(
        """
WITH
cte1 AS (SELECT a, b FROM table1),
cte2 AS (SELECT c, d FROM table2)
SELECT cte1.b, d FROM cte1 JOIN cte2
WHERE cte1.a = cte2.c;
"""
    )

    assert parser.with_names == ["cte1", "cte2"]
    assert parser.with_queries == {
        "cte1": "SELECT a, b FROM table1",
        "cte2": "SELECT c, d FROM table2",
    }
    assert parser.tables == ["table1", "table2"]
    assert parser.columns == ["a", "b", "c", "d"]


def test_with_with_columns():
    # fix for setting columns in with
    # https://github.com/macbre/sql-metadata/issues/128
    query = (
        "WITH t1 AS (SELECT * FROM t2), "
        "t3 (c1, c2) AS (SELECT c3, c4 FROM t4) SELECT * FROM t1, t3, t5;"
    )
    parser = Parser(query)
    assert parser.with_names == ["t1", "t3"]
    assert parser.with_queries == {
        "t1": "SELECT * FROM t2",
        "t3": "SELECT c3, c4 FROM t4",
    }
    assert parser.tables == ["t2", "t4", "t5"]
    assert parser.columns == ["*", "c3", "c4"]
    assert parser.columns_aliases_names == ["c1", "c2"]
    assert parser.columns_aliases == {"c1": "c3", "c2": "c4"}


def test_multiple_with_statements_with_with_columns():
    # fix for setting columns in with
    # https://github.com/macbre/sql-metadata/issues/128
    query = """
    WITH
    t1 (c1, c2) AS (SELECT * FROM t2),
    t3 (c3, c4) AS (SELECT c5, c6 FROM t4)
    SELECT * FROM t1, t3;
    """
    parser = Parser(query)
    assert parser.with_names == ["t1", "t3"]
    assert parser.with_queries == {
        "t1": "SELECT * FROM t2",
        "t3": "SELECT c5, c6 FROM t4",
    }
    assert parser.tables == ["t2", "t4"]
    assert parser.columns == ["*", "c5", "c6"]
    assert parser.columns_aliases_names == ["c1", "c2", "c3", "c4"]
    assert parser.columns_aliases == {"c1": "*", "c2": "*", "c3": "c5", "c4": "c6"}
    assert parser.query_type == QueryType.SELECT


def test_complicated_with():
    query = """
    WITH uisd_filter_table as (
        select
            session_id,
            srch_id,
            srch_ci,
            srch_co,
            srch_los,
            srch_sort_type,
            impr_list
        from
            uisd
        where
            datem <= date_sub(date_add(current_date(), 92), 7 * 52)
            and lower(srch_sort_type) in ('expertpicks', 'recommended')
            and srch_ci <= date_sub(date_add(current_date(), 92), 7 * 52)
            and srch_co >= date_sub(date_add(current_date(), 1), 7 * 52)
    )
    select
        DISTINCT session_id,
        srch_id,
        srch_ci,
        srch_co,
        srch_los,
        srch_sort_type,
        l.impr_property_id as expe_property_id,
        l.impr_position_across_pages
    from
        uisd_filter_table lateral view explode(impr_list) table as l
    """
    parser = Parser(query)
    assert parser.query_type == QueryType.SELECT
    assert parser.with_names == ["uisd_filter_table"]
    assert parser.with_queries == {
        "uisd_filter_table": "select session_id, srch_id, srch_ci, srch_co, srch_los, "
        "srch_sort_type, impr_list from uisd where datem <= "
        "date_sub(date_add(current_date(), 92), 7 * 52) and "
        "lower(srch_sort_type) in ('expertpicks', 'recommended') "
        "and srch_ci <= date_sub(date_add(current_date(), 92), 7 "
        "* 52) and srch_co >= date_sub(date_add(current_date(), "
        "1), 7 * 52)"
    }
    assert parser.tables == [
        "uisd",
        "impr_list",
    ]  # this one is wrong too should be table
    assert parser.columns == [
        "session_id",
        "srch_id",
        "srch_ci",
        "srch_co",
        "srch_los",
        "srch_sort_type",
        "impr_list",
        "datem",
        "l.impr_property_id",
        "l.impr_position_across_pages",
    ]


def test_resolving_with_clauses_with_columns():
    query = """
    WITH
    query1 (c1, c2) AS (SELECT * FROM t2),
    query2 (c3, c4) AS (SELECT c5, c6 FROM t4)
    SELECT query1.c2, query2.c4
    FROM query1 left join query2 on query1.c1 = query2.c3
    order by query1.c2;
    """
    parser = Parser(query)
    assert parser.with_names == ["query1", "query2"]
    assert parser.with_queries == {
        "query1": "SELECT * FROM t2",
        "query2": "SELECT c5, c6 FROM t4",
    }
    assert parser.tables == ["t2", "t4"]
    assert parser.columns_aliases == {"c1": "*", "c2": "*", "c3": "c5", "c4": "c6"}
    assert parser.columns_aliases_names == ["c1", "c2", "c3", "c4"]
    assert parser.columns_aliases_dict == {
        "join": ["c1", "c3"],
        "order_by": ["c2"],
        "select": ["c1", "c2", "c3", "c4"],
    }
    assert parser.columns == ["*", "c5", "c6"]
    assert parser.columns_dict == {
        "join": ["*", "c5"],
        "order_by": ["*"],
        "select": ["*", "c5", "c6"],
    }
    assert parser.query_type == QueryType.SELECT


def test_resolving_with_columns():
    query = """
    WITH
    query1 AS (SELECT c1, c2 FROM t5),
    query2 AS (SELECT c3, c4 FROM t6)
    SELECT query1.c2, query2.c4
    FROM query1 left join query2 on query1.c1 = query2.c3
    order by query1.c2;
    """
    parser = Parser(query)
    assert parser.with_names == ["query1", "query2"]
    assert parser.with_queries == {
        "query1": "SELECT c1, c2 FROM t5",
        "query2": "SELECT c3, c4 FROM t6",
    }
    assert parser.tables == ["t5", "t6"]
    assert parser.columns_aliases == {}
    assert parser.columns_aliases_names == []
    assert parser.columns_aliases_dict is None
    assert parser.columns == ["c1", "c2", "c3", "c4"]
    assert parser.columns_dict == {
        "join": ["c1", "c3"],
        "order_by": ["c2"],
        "select": ["c1", "c2", "c3", "c4"],
    }
    assert parser.query_type == QueryType.SELECT


def test_resolving_with_columns_with_wildcard():
    query = """
    WITH
    query1 AS (SELECT c1, c2, c4 FROM t5),
    query2 AS (SELECT c3, c7 FROM t6)
    SELECT query1.*, query2.c7
    FROM query1 left join query2 on query1.c4 = query2.c3
    order by query2.c7;
    """
    parser = Parser(query)
    assert parser.with_names == ["query1", "query2"]
    assert parser.with_queries == {
        "query1": "SELECT c1, c2, c4 FROM t5",
        "query2": "SELECT c3, c7 FROM t6",
    }
    assert parser.tables == ["t5", "t6"]
    assert parser.columns_aliases == {}
    assert parser.columns_aliases_names == []
    assert parser.columns == ["c1", "c2", "c4", "c3", "c7"]
    assert parser.columns_dict == {
        "join": ["c4", "c3"],
        "order_by": ["c7"],
        "select": ["c1", "c2", "c4", "c3", "c7"],
    }
    assert parser.query_type == QueryType.SELECT


def test_resolving_with_columns_with_nested_tables_prefixes():
    query = """
    WITH
    query1 AS (SELECT t5.c1, t5.c2, t6.c4 FROM t5 left join t6 on t5.link1=t6.link2),
    query2 AS (SELECT c3, c7 FROM t7 union all select c4, c12 from t8)
    SELECT query1.*, query2.c7, query2.c3
    FROM query1 left join query2 on query1.c4 = query2.c3
    order by query2.c7;
    """
    parser = Parser(query)
    assert parser.with_names == ["query1", "query2"]
    assert parser.with_queries == {
        "query1": "SELECT t5.c1, t5.c2, t6.c4 FROM t5 left join t6 on t5.link1 = "
        "t6.link2",
        "query2": "SELECT c3, c7 FROM t7 union all select c4, c12 from t8",
    }
    assert parser.tables == ["t5", "t6", "t7", "t8"]
    assert parser.columns_aliases == {}
    assert parser.columns_aliases_names == []
    assert parser.columns == [
        "t5.c1",
        "t5.c2",
        "t6.c4",
        "t5.link1",
        "t6.link2",
        "c3",
        "c7",
        "c4",
        "c12",
    ]
    assert parser.columns_dict == {
        "join": ["t5.link1", "t6.link2", "t6.c4", "c3"],
        "order_by": ["c7"],
        "select": [
            "t5.c1",
            "t5.c2",
            "t6.c4",
            "c3",
            "c7",
            "c4",
            "c12",
            "t5.link1",
            "t6.link2",
        ],
    }
    assert parser.query_type == QueryType.SELECT


def test_nested_with_statement_in_create_table():
    qry = """
            CREATE table xyz as
            with sub as (select it_id from internal_table)
            SELECT *
            from (
                with abc as (select * from other_table)
                select name, age, it_id
                from table_z
                join abc on (table_z.it_id = abc.it_id)
            ) as table_a
            join table_b on (table_a.name = table_b.name)
            left join table_c on (table_a.age = table_c.age)
            left join sub on (table_a.it_id = sub.it_id)
            order by table_a.name, table_a.age
            """
    parser = Parser(qry)
    assert parser.tables == [
        "xyz",
        "internal_table",
        "other_table",
        "table_z",
        "table_b",
        "table_c",
    ]
    assert parser.columns == [
        "it_id",
        "*",
        "name",
        "age",
        "table_z.it_id",
        "table_b.name",
        "table_c.age",
    ]
    assert parser.columns_dict == {
        "select": ["it_id", "*", "name", "age"],
        "join": [
            "table_z.it_id",
            "it_id",
            "name",
            "table_b.name",
            "age",
            "table_c.age",
        ],
        "order_by": ["name", "age"],
    }
    assert parser.with_names == ["sub", "abc"]
    assert parser.subqueries_names == ["table_a"]
    assert parser.with_queries == {
        "abc": "select * from other_table",
        "sub": "select it_id from internal_table",
    }
    assert parser.subqueries == {
        "table_a": "with abc as(select * from other_table) select name, age, it_id "
        "from table_z join abc on (table_z.it_id = abc.it_id)"
    }

    assert parser.query_type == QueryType.CREATE


def test_insert_overwrite():
    query = """
    WITH AAA AS
    (
      SELECT *
      FROM
        db1.tb1 AS jt
      WHERE
        col_date >= CURRENT_DATE
    )
    , BBB AS
    (
      SELECT
        col1,
        ROW_NUMBER() OVER(PARTITION BY col2 ORDER BY col3 DESC, col4 DESC) AS row_count
      FROM
        AAA
    )
    , CCC AS
    (
      SELECT *
      FROM
        BBB
      WHERE
        row_count = 1
    )
    , DDD AS
    (
      SELECT *
      FROM
        CCC
      WHERE
        col1 = 'HI'
    )
    INSERT OVERWRITE TABLE db4.tb25
    SELECT
      jt.col1,
      jt.col2,
      jt.col3
    FROM
      DDD AS jt
    ;
    """
    parser = Parser(query)
    assert parser.with_names == ["AAA", "BBB", "CCC", "DDD"]
    assert parser.columns == [
        "*",
        "col_date",
        "col1",
        "col2",
        "col3",
        "col4",
        "db1.tb1.col1",
        "db1.tb1.col2",
        "db1.tb1.col3",
    ]
    assert parser.tables == ["db1.tb1", "db4.tb25"]


def test_window_in_with():
    query = """
        WITH cte_1 AS (
            SELECT
                column_1, column_2
            FROM
                table_1
            WINDOW window_1 AS (
                PARTITION BY column_2
            )
        )
        SELECT
            column_1, column_2
        FROM
            cte_1 AS alias_1
    """

    parser = Parser(query)
    assert parser.with_names == ["cte_1"]
    assert parser.columns == ["column_1", "column_2"]
    assert parser.with_queries == {
        "cte_1": "SELECT column_1, column_2 FROM table_1 WINDOW window_1 AS(PARTITION BY column_2)"
    }
    assert parser.tables == ["table_1"]


def test_comment_between_with_and_query():
    query = """
        WITH cte_1 AS (
            SELECT column_1, column_2
            FROM table_1
        )
        /* COMMENT_1 */
        -- COMMENT_2
        SELECT column_1, column_2
        FROM cte_1 AS alias_1
    """

    parser = Parser(query)
    assert parser.with_names == ["cte_1"]
    assert parser.columns == ["column_1", "column_2"]
    assert parser.with_queries == {"cte_1": "SELECT column_1, column_2 FROM table_1"}
    assert parser.tables == ["table_1"]


def test_identifier_syntax():
    """
    Specific for ClickHouse With indentifier syntax

    https://clickhouse.com/docs/en/sql-reference/statements/select/with#examples
    """

    query = """
        WITH
            '2019-08-01 15:23:00' as ts_upper_bound
        SELECT EventDate, EventTime
        FROM hits
        WHERE
            EventDate = toDate(ts_upper_bound) AND
            EventTime <= ts_upper_bound;
    """

    parser = Parser(query)

    assert parser.tables == ["hits"]
    assert parser.columns == ["EventDate", "EventTime", "ts_upper_bound"]


def test_as_was_preceded_by_with_query():
    # fix
    # When 'with .* as (.*) as ...', it should prompt an error instead of an infinite loop.
    query = """
        WITH
        t1 (c1, c2) AS (SELECT * FROM t2) AS a1
        SELECT 1;
    """
    parser = Parser(query)
    with pytest.raises(ValueError, match="This query is wrong"):
        parser.tables

    query = """
        WITH
            t1 as (SELECT * FROM t2) AS
        SELECT 1;
    """
    parser = Parser(query)
    with pytest.raises(ValueError, match="This query is wrong"):
        parser.tables

    query = """
        WITH
            '2023-01-01' as (date) AS
        SELECT 1;
    """
    parser = Parser(query)
    with pytest.raises(ValueError, match="This query is wrong"):
        parser.tables
