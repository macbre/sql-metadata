from sql_metadata.parser import Parser


def test_no_limit_and_offset():
    assert Parser("SELECT foo_limit FROM bar_offset").limit_and_offset is None
    assert (
        Parser("SELECT foo_limit FROM bar_offset /* limit 1000,50 */").limit_and_offset
        is None
    )


def test_only_limit():
    assert Parser("SELECT foo_limit FROM bar_offset LIMIT 50").limit_and_offset == (
        50,
        0,
    )


def test_limit_and_offset():
    assert Parser(
        "SELECT foo_limit FROM bar_offset LIMIT 50 OFFSET 1000"
    ).limit_and_offset == (50, 1000)
    assert Parser(
        "SELECT foo_limit FROM bar_offset Limit 50 offset 1000"
    ).limit_and_offset == (50, 1000)


def test_comma_separated():
    assert Parser(
        "SELECT foo_limit FROM bar_offset LIMIT 1000, 50"
    ).limit_and_offset == (50, 1000)
    parser = Parser("SELECT foo_limit FROM bar_offset LIMIT 1000,50")
    assert parser.limit_and_offset == (50, 1000)
    assert parser.limit_and_offset != (0, 1000)

    assert Parser(
        "SELECT foo_limit FROM bar_offset limit 1000,50"
    ).limit_and_offset == (50, 1000)

    assert Parser(
        "SELECT /* CategoryPaginationViewer::processSection */  "
        "page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix  "
        "FROM `page` "
        "INNER JOIN `categorylinks` FORCE INDEX (cl_sortkey) ON ((cl_from = page_id))  "
        "WHERE cl_type = 'page' AND cl_to = 'Spotify/Song'  "
        "ORDER BY cl_sortkey LIMIT 927600,200"
    ).limit_and_offset == (200, 927600)


def test_with_in_condition():
    # https://github.com/macbre/sql-metadata/issues/382
    assert Parser(
        "SELECT count(*) FROM aa WHERE userid IN (222,333) LIMIT 50 OFFSET 1000"
    ).limit_and_offset == (50, 1000)


def test_limit_and_offset_on_update():
    """UPDATE has no LIMIT — returns None."""
    assert Parser("UPDATE t SET col = 1 WHERE id = 5").limit_and_offset is None


def test_limit_and_offset_on_insert():
    """INSERT has no LIMIT — returns None."""
    assert Parser("INSERT INTO t (a) VALUES (1)").limit_and_offset is None


def test_limit_with_parameter_placeholder():
    """LIMIT with a non-numeric placeholder triggers int conversion failure."""
    assert Parser("SELECT col FROM t LIMIT :limit").limit_and_offset is None


def test_limit_regex_mysql_comma_via_subquery():
    """Regex fallback finds MySQL comma LIMIT in subquery.

    LIMIT ALL makes sqlglot produce a non-integer limit node, triggering the
    regex fallback which then matches the inner subquery's LIMIT 10, 20.
    """
    p = Parser(
        "SELECT * FROM (SELECT id FROM t LIMIT 10, 20) AS sub LIMIT ALL"
    )
    assert p.limit_and_offset == (20, 10)


def test_limit_regex_standard_via_subquery():
    """Regex fallback finds standard LIMIT in subquery."""
    p = Parser(
        "SELECT * FROM (SELECT id FROM t LIMIT 30) AS sub"
        " FETCH FIRST 5 ROWS ONLY"
    )
    assert p.limit_and_offset == (30, 0)


def test_limit_regex_with_offset_via_subquery():
    """Regex fallback finds LIMIT with OFFSET when outer is unparseable."""
    p = Parser(
        "SELECT * FROM (SELECT id FROM t LIMIT 50 OFFSET 100)"
        " AS sub LIMIT ALL"
    )
    assert p.limit_and_offset == (50, 100)


def test_limit_and_offset_comment_only():
    """LIMIT/OFFSET on comment-only SQL returns None (AST is None)."""
    p = Parser("/* just a comment */")
    assert p.limit_and_offset is None
