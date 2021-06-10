from sql_metadata import Parser


def test_getting_values():
    parser = Parser(
        "INSERT /* VoteHelper::addVote xxx */  INTO `page_vote` (article_id,user_id,`time`) VALUES ('442001','27574631','20180228130846')"
    )
    assert parser.values == ["442001", "27574631", "20180228130846"]
    assert parser.values_dict == {
        "article_id": "442001",
        "user_id": "27574631",
        "time": "20180228130846",
    }

    # REPLACE queries
    parser = Parser(
        "REPLACE INTO `page_props` (pp_page,pp_propname,pp_value) VALUES ('47','infoboxes','')"
    )
    assert parser.values == ["47", "infoboxes", ""]
    assert parser.values_dict == {
        "pp_page": "47",
        "pp_propname": "infoboxes",
        "pp_value": "",
    }

    parser = Parser(
        "/* method */ INSERT IGNORE INTO `0070_insert_ignore_table` VALUES (9, 2.15, '123', '2017-01-01');"
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
        "INSERT INTO `wp_comments` (`comment_post_ID`, `comment_author`, `comment_author_email`, `comment_author_url`, `comment_author_IP`, `comment_date`, `comment_date_gmt`, `comment_content`, `comment_karma`, `comment_approved`, `comment_agent`, `comment_type`, `comment_parent`, `user_id`) VALUES (1, 'test user', '', '', '127.0.0.1', '2021-02-27 03:21:52', '2021-02-27 03:21:52', 'test comment', 0, '0', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv: 78.0) Gecko/20100101 Firefox/78.0', 'comment', 0, 0)',"
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
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv: 78.0) Gecko/20100101 Firefox/78.0",
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
        "comment_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv: 78.0) Gecko/20100101 Firefox/78.0",
        "comment_type": "comment",
        "comment_parent": 0,
        "user_id": 0,
    }
