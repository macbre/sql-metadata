import pytest

from sql_metadata import Parser


def test_reported_cte_aliases_in_where():
    # https://github.com/macbre/sql-metadata/issues/438
    parser = Parser("""
        WITH customer_total_return AS (
            SELECT sr_customer_sk AS ctr_customer_sk,
                   sr_store_sk AS ctr_store_sk, SUM(SR_FEE) AS ctr_total_return
            FROM store_returns, date_dim
            WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
            GROUP BY sr_customer_sk, sr_store_sk
        )
        SELECT c_customer_id FROM customer_total_return ctr1, store, customer
        WHERE ctr1.ctr_total_return > (
            SELECT AVG(ctr_total_return) * 1.2 FROM customer_total_return ctr2
            WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
        )
          AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD'
          AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id LIMIT 100
    """)
    assert parser.columns_dict["where"] == [
        "sr_returned_date_sk",
        "d_date_sk",
        "d_year",
        "SR_FEE",
        "sr_store_sk",
        "s_store_sk",
        "s_state",
        "sr_customer_sk",
        "c_customer_sk",
    ]
    assert parser.tables == ["store_returns", "date_dim", "store", "customer"]
    assert parser.tables_aliases == {}


@pytest.mark.parametrize(
    "query,columns,sections",
    [
        (
            "WITH cte AS (SELECT a AS x FROM t) "
            "SELECT c.x FROM cte AS c WHERE c.x > 0 ORDER BY c.x",
            ["a"],
            {"select": ["a"], "where": ["a"], "order_by": ["a"]},
        ),
        (
            "WITH cte AS (SELECT a + b AS x FROM t) SELECT c.x FROM cte c",
            ["a", "b"],
            {"select": ["a", "b"]},
        ),
        (
            "WITH cte(x) AS (SELECT a FROM t) SELECT c.x FROM cte c WHERE c.x > 0",
            ["a"],
            {"select": ["a"], "where": ["a"]},
        ),
        (
            "WITH cte AS (SELECT t.a AS x, t.b AS y FROM t) "
            "SELECT l.x, r.y FROM cte l JOIN cte r ON l.x = r.x WHERE r.y > 0",
            ["t.a", "t.b"],
            {"select": ["t.a", "t.b"], "join": ["t.a"], "where": ["t.b"]},
        ),
        (
            "WITH cte AS (SELECT a, b FROM t) SELECT c.* FROM cte c",
            ["a", "b"],
            {"select": ["a", "b"]},
        ),
        (
            "WITH cte AS (SELECT a AS x FROM t) SELECT c.x FROM (cte) AS c",
            ["a"],
            {"select": ["a"]},
        ),
    ],
)
def test_cte_table_alias_columns(query, columns, sections):
    # https://github.com/macbre/sql-metadata/issues/438
    parser = Parser(query)
    assert parser.columns == columns
    assert parser.columns_dict == sections
    assert parser.tables == ["t"]
    assert "cte" not in parser.tables_aliases.values()


def test_cte_alias_in_correlated_subquery():
    parser = Parser(
        "WITH cte AS (SELECT t.a AS x FROM t) "
        "SELECT c.x FROM cte c WHERE EXISTS "
        "(SELECT 1 FROM u WHERE u.a = c.x)"
    )
    assert parser.columns == ["t.a", "u.a"]
    assert parser.columns_dict == {"select": ["t.a"], "where": ["u.a", "t.a"]}
    assert parser.tables == ["t", "u"]
    assert parser.tables_aliases == {}


def test_chained_cte_table_aliases():
    parser = Parser(
        "WITH first AS (SELECT t.a AS x FROM t), "
        "second AS (SELECT f.x AS y FROM first f) SELECT s.y FROM second s"
    )
    direct = Parser(
        "WITH first AS (SELECT t.a AS x FROM t), "
        "second AS (SELECT first.x AS y FROM first) SELECT second.y FROM second"
    )
    assert parser.columns == ["t.a"]
    # Preserve the existing chained-CTE section resolution, independently of aliases.
    assert parser.columns_dict == direct.columns_dict


def test_existing_physical_table_alias_precedence():
    parser = Parser("WITH cte AS (SELECT c.a AS x FROM t c) SELECT c.x FROM cte c")
    assert parser.columns == ["t.a", "t.x"]
    assert parser.tables_aliases == {"c": "t"}


@pytest.mark.parametrize("source", ["cte c", "(SELECT a FROM t) c"])
def test_unknown_nested_column_keeps_its_qualifier(source):
    parser = Parser(f"WITH cte AS (SELECT a FROM t) SELECT c.missing FROM {source}")
    assert parser.columns == ["a", "c.missing"]


@pytest.mark.parametrize(
    "inner_source",
    ["u c", "(SELECT u.x FROM u) c"],
)
def test_local_source_shadows_outer_cte_alias(inner_source):
    parser = Parser(
        "WITH cte AS (SELECT t.a AS x FROM t) "
        "SELECT c.x FROM cte c WHERE EXISTS "
        f"(SELECT c.x FROM {inner_source} WHERE c.x > 0)"
    )
    assert parser.columns == ["t.a", "u.x"]
    assert parser.columns_dict["where"] == ["u.x"]
    assert parser.tables == ["t", "u"]


def test_nested_cte_alias_does_not_escape_its_select():
    parser = Parser(
        "WITH cte AS (SELECT t.a AS x FROM t) "
        "SELECT d.x FROM (SELECT c.x FROM cte c) d JOIN u c ON d.x = c.x"
    )
    assert parser.columns == ["t.a", "u.x"]
    assert parser.columns_dict["join"] == ["t.a", "u.x"]
    assert parser.tables_aliases == {"c": "u"}


def test_nested_with_definition_is_not_an_outer_alias_source():
    parser = Parser(
        "SELECT 1 FROM real_table c WHERE c.x > 0 AND EXISTS "
        "(WITH real_table AS (SELECT t.a AS x FROM t) SELECT x FROM real_table)"
    )
    # The inner CTE is not a source for the outer WHERE's c.x reference.
    assert "t.a" not in parser.columns_dict["where"]


@pytest.mark.parametrize(
    "query,columns,aliases",
    [
        ("SELECT p.a FROM t p WHERE p.a > 0", ["t.a"], {"p": "t"}),
        ("WITH cte AS (SELECT a AS x FROM t) SELECT cte.x FROM cte", ["a"], {}),
        (
            "WITH cte AS (SELECT a FROM t) SELECT c.x FROM schema.cte c",
            ["a", "schema.cte.x"],
            {"c": "schema.cte"},
        ),
        (
            "WITH cte AS (SELECT a FROM t) SELECT c.x FROM u c",
            ["a", "u.x"],
            {"c": "u"},
        ),
    ],
)
def test_non_cte_source_aliases_unchanged(query, columns, aliases):
    parser = Parser(query)
    assert parser.columns == columns
    assert parser.tables_aliases == aliases
