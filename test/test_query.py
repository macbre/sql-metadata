from sql_metadata.parser import Parser


def test_get_query_tokens():
    assert Parser("").tokens == []

    tokens = Parser("SELECT * FROM foo").tokens

    assert len(tokens) == 4

    assert str(tokens[0]) == "SELECT"
    assert tokens[2].is_keyword
    assert str(tokens[2]) == "FROM"


def test_preprocessing():
    # assert (
    #     Parser(
    #         "SELECT DISTINCT dw.lang FROM `dimension_wikis` `dw` INNER JOIN `fact_wam_scores` `fwN` ON ((dw.wiki_id = fwN.wiki_id)) WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC"
    #     ).query
    #     == "SELECT DISTINCT dw.lang FROM `dimension_wikis`  INNER JOIN `fact_wam_scores` ON ((dw.wiki_id = fwN.wiki_id)) WHERE fwN.time_id = FROM_UNIXTIME(N) ORDER BY dw.lang ASC"
    # )
    #
    # assert (
    #     Parser(
    #         "SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` `fwN` left join `fact_wam_scores` `fwN` ON ((fwN.wiki_id = fwN.wiki_id) AND (fwN.time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` `dw` ON ((fwN.wiki_id = dw.wiki_id)) WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"
    #     ).query
    #     == "SELECT count(fwN.wiki_id) as wam_results_total FROM `fact_wam_scores` left join `fact_wam_scores` ON ((fwN.wiki_id = fwN.wiki_id) AND (fwN.time_id = FROM_UNIXTIME(N))) left join `dimension_wikis` ON ((fwN.wiki_id = dw.wiki_id)) WHERE (fwN.time_id = FROM_UNIXTIME(N)) AND (dw.url like X OR dw.title like X) AND fwN.vertical_id IN (XYZ) AND dw.lang = X AND (fwN.wiki_id NOT IN (XYZ)) AND ((dw.url IS NOT NULL AND dw.title IS NOT NULL))"
    # )

    # normalize database selector
    assert Parser("SELECT foo FROM `db`.`test`").query == "SELECT foo FROM db.test"

    assert (
        Parser(
            "SELECT r1.wiki_id AS id FROM report_wiki_recent_pageviews AS r1 INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id"
        ).query
        == "SELECT r1.wiki_id AS id FROM report_wiki_recent_pageviews AS r1 INNER JOIN dimension_wikis AS d ON r.wiki_id = d.wiki_id"
    )

    # normalize newlines
    assert (
        Parser("SELECT foo,\nid\nFROM `db`.`test`").query
        == "SELECT foo, id FROM db.test"
    )


def test_case_insensitive():
    # case-insensitive handling
    # https://github.com/macbre/sql-metadata/issues/71
    assert ["abc.foo", "foo", "bar"] == Parser(
        "create table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).tables

    assert ["abc.foo", "foo", "bar"] == Parser(
        "create table abc.foo as select pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).tables

    assert ["foo.foo1", "bar.foo2"] == Parser(
        "create table abc.foo as SELECT pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).columns

    assert ["foo.foo1", "bar.foo2"] == Parser(
        "create table abc.foo as select pqr.foo1 , ab.foo2 FROM foo pqr, bar ab"
    ).columns


def test_handle_force_index():
    query = (
        "SELECT  page_title,page_namespace  FROM `page` FORCE INDEX (page_random) "
        "JOIN `categorylinks` ON ((page_id=cl_from))  WHERE page_is_redirect = '0' "
        "AND (page_random >= 0.197372293871) AND cl_to = 'Muppet_Characters'  "
        "ORDER BY page_random LIMIT 1"
    )

    assert Parser(query).tables == ["page", "categorylinks"]
    assert Parser(query).columns == [
        "page_title",
        "page_namespace",
        "page_id",
        "cl_from",
        "page_is_redirect",
        "page_random",
        "cl_to",
    ]


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


def test_case_syntax():
    # https://dev.mysql.com/doc/refman/8.0/en/case.html
    assert Parser(
        "select case when p > 0 then 1 else 0 end as cs from c where g > f"
    ).columns == ["p", "g", "f"]
    assert Parser(
        "select case when p > 0 then 1 else 0 end as cs from c where g > f"
    ).tables == ["c"]
