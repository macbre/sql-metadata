import pytest

from sql_metadata.parser import Parser


@pytest.mark.parametrize(
    "query, expected",
    [
        pytest.param(
            "SELECT * FROM mydb..test_table",
            ["mydb..test_table"],
            id="Default schema, db qualified",
        ),
        pytest.param(
            "SELECT * FROM ..test_table",
            ["..test_table"],
            id="Default schema, db unqualified",
        ),
        pytest.param(
            "SELECT * FROM [mydb].[dbo].[test_table]",
            ["[mydb].[dbo].[test_table]"],
            id="With object identifier delimiters",
        ),
        pytest.param(
            "SELECT * FROM [my_server].[mydb].[dbo].[test_table]",
            ["[my_server].[mydb].[dbo].[test_table]"],
            id="With linked-server and object identifier delimiters",
        ),
    ],
)
def test_simple_queries_tables(query, expected):
    assert Parser(query).tables == expected


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

    parser = Parser(
        """
        WITH foo AS (
            SELECT * FROM n
        )
        UPDATE z from foo set z.q = foo.y
        """
    )
    assert parser.tables == ["n", "z"]

    parser = Parser(
        """
        WITH foo AS (
             SELECT * FROM tab
        )
        DELETE FROM z JOIN foo ON z.a = foo.a
        """
    )
    assert parser.tables == ["tab", "z"]


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
    """

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


def test_partition_over_with_rank_and_one_order():
    """Test for #204"""
    parser = Parser(
        """
        select t.RANKED, t.RANKED_two, t.test from (
        SELECT
        RANK() OVER (PARTITION BY col_one ORDER BY col_two) RANKED,
        RANK() OVER (PARTITION BY col_one ORDER BY col_two) as RANKED_two,
        col_three as test
        FROM nice_table) as t
        where t.RANKED = 1
        and t.RANKED_two = 2
        order by test
        """
    )
    assert parser.tables == ["nice_table"]
    assert parser.columns_aliases_names == ["RANKED", "RANKED_two", "test"]
    assert parser.columns_aliases == {
        "RANKED": ["col_one", "col_two"],
        "RANKED_two": ["col_one", "col_two"],
        "test": "col_three",
    }
    assert parser.columns == ["col_one", "col_two", "col_three"]
    assert parser.columns_dict == {
        "order_by": ["col_three"],
        "select": ["col_one", "col_two", "col_three"],
        "where": ["col_one", "col_two"],
    }


def test_partition_over_with_row_number_and_many_orders():
    """Test for #204"""
    parser = Parser(
        """
        select t.row_no, t.row_no_two, t.test from (
        SELECT
        ROW_NUMBER() OVER (
        PARTITION BY col_one
        ORDER BY col_two, col_three, col_four) row_no,
        ROW_NUMBER() OVER (
        PARTITION BY col_one
        ORDER BY col_two, col_three) as row_no_two,
        col_three as test
        FROM nice_table) as t
        where t.row_no = 1
        and t.row_no_two = 2
        order by t.row_no
        """
    )
    assert parser.tables == ["nice_table"]
    assert parser.columns_aliases_names == ["row_no", "row_no_two", "test"]
    assert parser.columns_aliases == {
        "row_no": ["col_one", "col_two", "col_three", "col_four"],
        "row_no_two": ["col_one", "col_two", "col_three"],
        "test": "col_three",
    }
    assert parser.columns == ["col_one", "col_two", "col_three", "col_four"]
    assert parser.columns_dict == {
        "order_by": ["col_one", "col_two", "col_three", "col_four"],
        "select": ["col_one", "col_two", "col_three", "col_four"],
        "where": ["col_one", "col_two", "col_three", "col_four"],
    }
