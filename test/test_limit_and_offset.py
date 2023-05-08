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
