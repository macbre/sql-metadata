from sql_metadata.parser import Parser


def test_get_query_tokens():
    assert Parser("").tokens == []

    tokens = Parser("SELECT * FROM foo").tokens

    assert len(tokens) == 4
    assert tokens[0] == "SELECT"
    assert tokens[1] == "*"
    assert tokens[2] == "FROM"
    assert tokens[3] == "foo"


def test_preprocessing():
    # normalize database selector
    assert Parser("SELECT foo FROM `db`.`test`").query == "SELECT foo FROM `db`.`test`"
    assert Parser('SELECT foo FROM "db"."test"').query == "SELECT foo FROM `db`.`test`"

    assert (
        Parser(
            "SELECT r1.wiki_id AS id FROM report_wiki_recent_pageviews AS r1 "
            "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id"
        ).query
        == "SELECT r1.wiki_id AS id FROM report_wiki_recent_pageviews AS r1 "
        "INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id"
    )

    # comments are kept
    assert (
        Parser("SELECT /*my random comment*/ foo, id FROM `db`.`test`").query
        == "SELECT /*my random comment*/ foo, id FROM `db`.`test`"
    )

    # check " in strings are kept
    assert (
        Parser("SELECT * from aa where name = 'test name with \" in string'").query
        == "SELECT * from aa where name = 'test name with \" in string'"
    )
    assert (
        Parser("SELECT * from aa where name = 'test name with \"aa\" in string'").query
        == "SELECT * from aa where name = 'test name with \"aa\" in string'"
    )
    assert (
        Parser("SELECT * from aa where name = 'test name with \"aa\" in string'").query
        == "SELECT * from aa where name = 'test name with \"aa\" in string'"
    )
    assert (
        Parser(
            "SELECT * from aa where name = 'test name with \"aa\" in string' "
            "and aa =' as \"aa.oo\" '"
        ).query
        == "SELECT * from aa where name = 'test name with \"aa\" in string' "
        "and aa =' as \"aa.oo\" '"
    )


def test_case_insensitive():
    # case-insensitive handling
    # https://github.com/macbre/sql-metadata/issues/71
    assert ["abc.foo", "foo", "bar"] == Parser(
        "CREATE table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).tables

    assert ["abc.foo", "foo", "bar"] == Parser(
        "CREATE table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).tables

    assert ["foo.foo1", "bar.foo2"] == Parser(
        "CREATE table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).columns

    assert ["foo.foo1", "bar.foo2"] == Parser(
        "CREATE table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).columns


def test_handle_force_index():
    query = (
        "SELECT  page_title,page_namespace  FROM `page` FORCE INDEX (page_random) "
        "JOIN `categorylinks` ON ((page_id=cl_from))  WHERE page_is_redirect = '0' "
        "AND (page_random >= 0.197372293871) AND cl_to = 'Muppet_Characters'  "
        "ORDER BY page_random LIMIT 1"
    )
    parser = Parser(query)
    assert parser.tables == ["page", "categorylinks"]
    assert parser.columns == [
        "page_title",
        "page_namespace",
        "page_id",
        "cl_from",
        "page_is_redirect",
        "page_random",
        "cl_to",
    ]
    assert parser.columns_dict == {
        "select": ["page_title", "page_namespace"],
        "join": ["page_id", "cl_from"],
        "where": ["page_is_redirect", "page_random", "cl_to"],
        "order_by": ["page_random"],
    }


def test_insert_into_select():
    # https://dev.mysql.com/doc/refman/5.7/en/insert-select.html
    query = "INSERT INTO foo SELECT * FROM bar"
    assert Parser(query).tables == ["foo", "bar"]
    assert Parser(query).columns == ["*"]

    query = "INSERT INTO foo SELECT id, price FROM bar"
    assert Parser(query).tables == ["foo", "bar"]
    assert Parser(query).columns == ["id", "price"]

    query = "INSERT INTO foo SELECT id, price FROM bar WHERE qty > 200"
    assert Parser(query).tables == ["foo", "bar"]
    assert Parser(query).columns == ["id", "price", "qty"]
    assert Parser(query).columns_dict == {"select": ["id", "price"], "where": ["qty"]}


def test_case_syntax():
    # https://dev.mysql.com/doc/refman/8.0/en/case.html
    assert Parser(
        "SELECT case when p > 0 then 1 else 0 end as cs from c where g > f"
    ).columns == ["p", "g", "f"]
    assert Parser(
        "SELECT case when p > 0 then 1 else 0 end as cs from c where g > f"
    ).tables == ["c"]


def test_empty_query_property():
    """The query property returns empty string for empty SQL."""
    assert Parser("").query == ""


def test_query_rewrites_double_quoted_identifier_with_hash_comment():
    """Double-quoted identifiers are rewritten even when a # comment is present."""
    # Regression guard: the MySQL tokenizer reclassifies "X" as STRING,
    # so preprocess_query must use the default tokenizer unconditionally.
    assert (
        Parser('SELECT "col_a" FROM t # trailing hash').query
        == "SELECT `col_a` FROM t # trailing hash"
    )


def test_tokens_caching():
    """Second access to tokens returns the cached list."""
    p = Parser("SELECT col FROM t")
    first = p.tokens
    second = p.tokens
    assert first is second
