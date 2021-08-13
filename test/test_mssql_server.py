from sql_metadata.parser import Parser


def test_sql_server_cte():
    """
    Tests support for SQL Server's common table expression (CTE).

    @see https://www.sqlservertutorial.net/sql-server-basics/sql-server-cte/
    """

    parser = Parser(
        """
        WITH x AS (
            SELECT * FROM n
        )
        SELECT
            *
        FROM x
        JOIN y ON x.a = y.a
            """
    )
    assert parser.tables == ["n", "y"]
    assert parser.with_names == ["x"]
    assert parser.with_queries == {"x": "SELECT * FROM n"}
    assert parser.columns == ["*", "a", "y.a"]

    assert (
        Parser(
            """
                WITH x AS (
                    SELECT * FROM n
                )
                SELECT
                    *
                FROM x
                JOIN y ON x.a = y.a
                    """
        ).tables
        == ["n", "y"]
    )

    assert (
        Parser(
            """
                WITH foo AS (
                    SELECT * FROM n
                )
                UPDATE z from foo set z.q = foo.y
                    """
        ).tables
        == ["n", "z"]
    )

    assert (
        Parser(
            """
                WITH foo AS (
                     SELECT * FROM tab
                )
                DELETE FROM z JOIN foo ON z.a = foo.a
                    """.strip()
        ).tables
        == ["tab", "z"]
    )


def test_sql_server_cte_sales_by_year():
    sales_query = """
WITH cte_sales AS (
    SELECT
        staff_id,
        COUNT(*) order_count
    FROM
        sales.orders
    WHERE
        YEAR(order_date) = 2018
    GROUP BY
        staff_id
)
SELECT
    AVG(order_count) average_orders_by_staff
FROM
    cte_sales;
    """.strip()

    parser = Parser(sales_query)
    assert parser.tables == ["sales.orders"]
    assert parser.with_names == ["cte_sales"]
    assert parser.with_queries == {
        "cte_sales": "SELECT staff_id, COUNT(*) order_count FROM sales.orders WHERE "
        "YEAR(order_date) = 2018 GROUP BY staff_id"
    }
    assert parser.columns_aliases_names == ["order_count", "average_orders_by_staff"]
    assert parser.columns_aliases == {
        "average_orders_by_staff": "order_count",
        "order_count": "*",
    }
    assert parser.columns == [
        "staff_id",
        "order_date",
    ]
    assert parser.columns_dict == {
        "group_by": ["staff_id"],
        "select": ["staff_id", "*"],
        "where": ["order_date"],
    }


def test_sql_server_rank_subquery():
    """Test for #204"""
    parser = Parser(
        """
        select t.RANKED, t.RANKED_two, t.test, t.row_num from (SELECT
        ,RANK() OVER (PARTITION BY col_one ORDER BY col_two) RANKED
        ,RANK() OVER (PARTITION BY col_one ORDER BY col_two) as RANKED_two
        ,ROW_NUMBER() OVER (PARTITION BY col_one ORDER BY col_two) row_num
        ,ROW_NUMBER() OVER (PARTITION BY col_one ORDER BY col_two) as row_num_tow
        , col_three as test
        FROM nice_table) as t
        where t.RANKED = 1
        and t.RANKED_two = 2
        """
    )
    assert parser.tables == ["nice_table"]
    assert ["nice_table"] == parser.tables
    assert ["col_one", "col_two", "col_three"] == parser.columns
