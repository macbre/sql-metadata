from sql_metadata import Parser


def test_getting_comments():
    parser = Parser(
        "INSERT /* VoteHelper::addVote xxx */  INTO `page_vote` (article_id,user_id,`time`) VALUES ('442001','27574631','20180228130846')"
    )
    assert parser.comments == ["/* VoteHelper::addVote xxx */"]

    parser = Parser(
        "SELECT /* CategoryPaginationViewer::processSection */  "
        "page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix  FROM `page` "
        "INNER JOIN `categorylinks` FORCE INDEX (cl_sortkey) ON ((cl_from = page_id))  "
        " /* We should add more conditions */ "
        "WHERE cl_type = 'page' AND cl_to = 'Spotify/Song'  "
        "  /* Verify with accounting */   "
        "ORDER BY cl_sortkey LIMIT 927600,200"
    )
    assert parser.comments == [
        "/* CategoryPaginationViewer::processSection */",
        "/* We should add more conditions */",
        "/* Verify with accounting */",
    ]
    assert parser.remove_comments == (
        "SELECT page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix "
        "FROM `page` "
        "INNER JOIN `categorylinks` FORCE INDEX (cl_sortkey) ON ((cl_from = page_id)) "
        "WHERE cl_type = 'page' AND cl_to = 'Spotify/Song' "
        "ORDER BY cl_sortkey LIMIT 927600,200"
    )
