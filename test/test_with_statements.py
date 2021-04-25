from sql_metadata import Parser


def test_with_statements():
    assert (
        ["database1.tableFromWith", "test"]
        == Parser(
            """
        WITH
            database1.tableFromWith AS (SELECT aa.* FROM table3 as aa 
                                        left join table4 on aa.col1=table4.col2),
            test as (select * from table3)
        SELECT
          "xxxxx"
        FROM
          database1.tableFromWith alias
        LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx")
        """
        ).with_names
    )

    assert (
        ["table3", "table4", "database2.table2"]
        == Parser(
            """
WITH
    database1.tableFromWith AS (SELECT aa.* FROM table3 as aa 
                                left join table4 on aa.col1=table4.col2),
    test as (select * from table3)
SELECT
  "xxxxx"
FROM
  database1.tableFromWith alias
LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx")
"""
        ).tables
    )

    assert (
        [
            "database1.tableFromWith",
            "database1.tableFromWith2",
            "database1.tableFromWith3",
            "database1.tableFromWith4",
        ]
        == Parser(
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
        ).with_names
    )

    assert (
        ["table3", "table4", "table5", "table6", "database2.table2"]
        == Parser(
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
        ).tables
    )

    assert (
        ["cte1", "cte2"]
        == Parser(
            """
WITH
cte1 AS (SELECT a, b FROM table1),
cte2 AS (SELECT c, d FROM table2)
SELECT b, d FROM cte1 JOIN cte2
WHERE cte1.a = cte2.c;
"""
        ).with_names
    )

    assert (
        ["table1", "table2"]
        == Parser(
            """
    WITH
cte1 AS (SELECT a, b FROM table1),
cte2 AS (SELECT c, d FROM table2)
SELECT b, d FROM cte1 JOIN cte2
WHERE cte1.a = cte2.c;
    """
        ).tables
    )
