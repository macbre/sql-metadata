from sql_metadata import Parser


def test_getting_values():
    parser = Parser(
        "INSERT /* VoteHelper::addVote xxx */  "
        "INTO `page_vote` (article_id,user_id,`time`) "
        "VALUES ('442001','27574631','20180228130846')"
    )
    assert parser.values == ["442001", "27574631", "20180228130846"]
    assert parser.values_dict == {
        "article_id": "442001",
        "user_id": "27574631",
        "time": "20180228130846",
    }

    # REPLACE queries
    parser = Parser(
        "REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) "
        "VALUES ('47','infoboxes','')"
    )
    assert parser.values == ["47", "infoboxes", ""]
    assert parser.values_dict == {
        "pp_page": "47",
        "pp_propname": "infoboxes",
        "pp_value": "",
    }

    parser = Parser(
        "/* method */ INSERT IGNORE INTO `0070_insert_ignore_table` "
        "VALUES (9, 2.15, '123', '2017-01-01');"
    )
    assert parser.query_type == "INSERT"
    assert parser.values == [9, 2.15, "123", "2017-01-01"]
    assert parser.values_dict == {
        "column_1": 9,
        "column_2": 2.15,
        "column_3": "123",
        "column_4": "2017-01-01",
    }

    assert [] == Parser("SELECT * from foo;").values

    assert Parser("SELECT * from foo;").values_dict is None

    parser = Parser(
        "INSERT INTO `wp_comments` (`comment_post_ID`, `comment_author`, "
        "`comment_author_email`, `comment_author_url`, `comment_author_IP`, "
        "`comment_date`, `comment_date_gmt`, `comment_content`, `comment_karma`, "
        "`comment_approved`, `comment_agent`, `comment_type`, `comment_parent`, "
        "`user_id`) VALUES (1, 'test user', '', '', '127.0.0.1', '2021-02-27 03:21:52',"
        " '2021-02-27 03:21:52', 'test comment', 0, '0', "
        "'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv: 78.0) "
        "Gecko/20100101 Firefox/78.0', "
        "'comment', 0, 0)"
    )
    assert parser.values == [
        1,
        "test user",
        "",
        "",
        "127.0.0.1",
        "2021-02-27 03:21:52",
        "2021-02-27 03:21:52",
        "test comment",
        0,
        "0",
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv: 78.0) "
            "Gecko/20100101 Firefox/78.0"
        ),
        "comment",
        0,
        0,
    ]

    assert parser.values_dict == {
        "comment_post_ID": 1,
        "comment_author": "test user",
        "comment_author_email": "",
        "comment_author_url": "",
        "comment_author_IP": "127.0.0.1",
        "comment_date": "2021-02-27 03:21:52",
        "comment_date_gmt": "2021-02-27 03:21:52",
        "comment_content": "test comment",
        "comment_karma": 0,
        "comment_approved": "0",
        "comment_agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv: 78.0) "
            "Gecko/20100101 Firefox/78.0"
        ),
        "comment_type": "comment",
        "comment_parent": 0,
        "user_id": 0,
    }


def test_values_on_invalid_sql():
    """Values extraction returns empty list for unparseable SQL."""
    from sql_metadata import Parser

    p = Parser(";;;")
    assert p.values == []


def test_values_on_comment_only_sql():
    """Values extraction returns empty list when SQL is only comments."""
    from sql_metadata import Parser

    p = Parser("/* just a comment */")
    assert p.values == []


def test_negative_integer_values():
    """INSERT with a negative integer value."""
    p = Parser("INSERT INTO scores (player, points) VALUES ('alice', -42)")
    assert p.values == ["alice", -42]
    assert p.values_dict == {"player": "alice", "points": -42}


def test_negative_float_values():
    """INSERT with a negative float value."""
    p = Parser(
        "INSERT INTO measurements (sensor, reading) VALUES ('temp', -3.14)"
    )
    assert p.values == ["temp", -3.14]
    assert p.values_dict == {"sensor": "temp", "reading": -3.14}


def test_insert_with_null_value():
    """INSERT with NULL triggers the str(val) fallback in _convert_value."""
    p = Parser("INSERT INTO t (a, b) VALUES (1, NULL)")
    assert p.values == [1, "NULL"]
    assert p.values_dict == {"a": 1, "b": "NULL"}


def test_insert_with_scalar_subquery_in_values():
    """Scalar subquery inside VALUES — columns from the subquery are extracted."""
    p = Parser(
        "INSERT INTO orders (customer_id) "
        "VALUES ((SELECT id FROM customers WHERE email = 'foo@bar.com'))"
    )
    assert p.tables == ["orders", "customers"]
    assert p.columns == ["customer_id", "id", "email"]


def test_insert_multi_row_values():
    # Solved: https://github.com/macbre/sql-metadata/issues/558
    p = Parser("INSERT INTO t (field1, field2) VALUES (1, 2), (3, 4)")
    assert p.values == [[1, 2], [3, 4]]
    assert p.values_dict == {"field1": [1, 3], "field2": [2, 4]}


def test_insert_with_expression_value():
    """INSERT with a function call in VALUES uses str(val) fallback."""
    p = Parser("INSERT INTO t (a) VALUES (CURRENT_TIMESTAMP)")
    assert len(p.values) == 1
