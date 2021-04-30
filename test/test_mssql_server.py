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
    select
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
    update z from foo set z.q = foo.y 
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

    assert Parser(sales_query).tables == ["sales.orders"]
    # TODO: Check if average_orders_by_staff should be included,
    #  if no why order_count is included - what is the rule here?
    assert Parser(sales_query).columns == [
        "staff_id",
        "order_count",
        "order_date",
        "average_orders_by_staff",
    ]
