from sql_metadata.parser import Parser


def test_sql_server_cte():
    """
    Tests support for SQL Server's common table expression (CTE).

    @see https://www.sqlservertutorial.net/sql-server-basics/sql-server-cte/
    """
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
                """.strip()
        ).tables
        == ["n", "y"]
    )

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
                """.strip()
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
                """.strip()
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
