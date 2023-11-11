from sql_metadata import Parser


def test_getting_comments():
    parser = Parser(
        "INSERT /* VoteHelper::addVote xxx */  "
        "INTO `page_vote` (article_id,user_id,`time`) "
        "VALUES ('442001','27574631','20180228130846')"
    )
    assert parser.comments == ["/* VoteHelper::addVote xxx */"]

    parser = Parser(
        "SELECT /* CategoryPaginationViewer::processSection */  "
        "page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix  "
        "FROM `page` "
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
    assert parser.without_comments == (
        "SELECT page_namespace,page_title,page_len,page_is_redirect,cl_sortkey_prefix "
        "FROM `page` "
        "INNER JOIN `categorylinks` FORCE INDEX (cl_sortkey) ON ((cl_from = page_id)) "
        "WHERE cl_type = 'page' AND cl_to = 'Spotify/Song' "
        "ORDER BY cl_sortkey LIMIT 927600,200"
    )
    # no comments and new lines
    assert (
        "SELECT test FROM `foo`.`bar`"
        == Parser("SELECT /* foo */ test\nFROM `foo`.`bar`").without_comments
    )


def test_inline_comments():
    query = """
    SELECT *
    from foo -- this comment should not be hiding rest of the query
    join bar on foo.a=bar.b
    where foo.c = 'am'
    """
    parser = Parser(query)
    assert parser.tables == ["foo", "bar"]
    assert parser.columns == ["*", "foo.a", "bar.b", "foo.c"]
    assert parser.comments == [
        "-- this comment should not be hiding rest of the query\n"
    ]

    query = """
    SELECT * --multiple
    from foo -- comments
    left outer join bar on foo.a=bar.b --works too
    where foo.c = 'am'
    """
    parser = Parser(query)
    assert parser.tables == ["foo", "bar"]
    assert parser.columns == ["*", "foo.a", "bar.b", "foo.c"]
    assert parser.comments == ["--multiple\n", "-- comments\n", "--works too\n"]


def test_inline_comments_with_hash():
    query = """
        SELECT * # multiple
        from foo # comments
        left outer join bar on foo.a=bar.b # works too
        where foo.c = 'am'
        """
    parser = Parser(query)
    assert parser.tables == ["foo", "bar"]
    assert parser.columns == ["*", "foo.a", "bar.b", "foo.c"]
    assert parser.comments == ["# multiple\n", "# comments\n", "# works too\n"]

    query = """
    SELECT
    ACCOUNTING_ENTITY.VERSION as "accountingEntityVersion",
    ACCOUNTING_ENTITY.ACTIVE as "active",
    ACCOUNTING_ENTITY.CATEGORY as "category",
    ACCOUNTING_ENTITY.CREATION_DATE as "creationDate",
    ACCOUNTING_ENTITY.DESCRIPTION as "description",
    ACCOUNTING_ENTITY.ID as "accountingEntityId",
    ACCOUNTING_ENTITY.MINIMAL_REMAINDER as "minimalRemainder",
    ACCOUNTING_ENTITY.REMAINDER as "remainder",
    ACCOUNTING_ENTITY.SYSTEM_TYPE_ID as "aeSystemTypeId",
    ACCOUNTING_ENTITY.DATE_CREATION as "dateCreation",
    ACCOUNTING_ENTITY.DATE_LAST_MODIFICATION as "dateLastModification",
    ACCOUNTING_ENTITY.USER_CREATION as "userCreation",
    ACCOUNTING_ENTITY.USER_LAST_MODIFICATION as "userLastModification"
    FROM ACCOUNTING_ENTITY
    WHERE ACCOUNTING_ENTITY.ID IN (
    SELECT DPD.ACCOUNTING_ENTITY_ID AS "ACCOUNTINGENTITYID" FROM DEBT D
    INNER JOIN DUTY_PER_DEBT DPD ON DPD.DEBT_ID = D.ID
    INNER JOIN DECLARATION_V2 DV2 ON DV2.ID = D.DECLARATION_ID
    WHERE DV2.DECLARATION_REF = #MRNFORMOVEMENT#
    UNION
    SELECT BX.ACCOUNTING_ENTITY_ID AS "ACCOUNTINGENTITYID" FROM BENELUX BX
    INNER JOIN DECLARATION_V2 DV2 ON DV2.ID = BX.DECLARATION_ID
    WHERE DV2.DECLARATION_REF = #MRNFORMOVEMENT#
    UNION
    SELECT CA4D.ACCOUNTING_ENTITY_ID AS "ACCOUNTINGENTITYID" FROM RESERVATION R
    INNER JOIN CA4_RESERVATIONS_DECLARATION CA4D ON CA4D.ID = R.CA4_ID
    INNER JOIN DECLARATION_V2 DV2 ON DV2.ID = R.DECLARATION_ID
    WHERE DV2.DECLARATION_REF = #MRNFORMOVEMENT#
    """
    parser = Parser(query)
    assert parser.tables == [
        "ACCOUNTING_ENTITY",
        "DEBT",
        "DUTY_PER_DEBT",
        "DECLARATION_V2",
        "BENELUX",
        "RESERVATION",
        "CA4_RESERVATIONS_DECLARATION",
    ]
    assert parser.columns_dict == {
        "join": [
            "DUTY_PER_DEBT.DEBT_ID",
            "DEBT.ID",
            "DECLARATION_V2.ID",
            "DEBT.DECLARATION_ID",
            "BENELUX.DECLARATION_ID",
            "CA4_RESERVATIONS_DECLARATION.ID",
            "RESERVATION.CA4_ID",
            "RESERVATION.DECLARATION_ID",
        ],
        "select": [
            "ACCOUNTING_ENTITY.VERSION",
            "ACCOUNTING_ENTITY.ACTIVE",
            "ACCOUNTING_ENTITY.CATEGORY",
            "ACCOUNTING_ENTITY.CREATION_DATE",
            "ACCOUNTING_ENTITY.DESCRIPTION",
            "ACCOUNTING_ENTITY.ID",
            "ACCOUNTING_ENTITY.MINIMAL_REMAINDER",
            "ACCOUNTING_ENTITY.REMAINDER",
            "ACCOUNTING_ENTITY.SYSTEM_TYPE_ID",
            "ACCOUNTING_ENTITY.DATE_CREATION",
            "ACCOUNTING_ENTITY.DATE_LAST_MODIFICATION",
            "ACCOUNTING_ENTITY.USER_CREATION",
            "ACCOUNTING_ENTITY.USER_LAST_MODIFICATION",
            "DUTY_PER_DEBT.ACCOUNTING_ENTITY_ID",
            "BENELUX.ACCOUNTING_ENTITY_ID",
            "CA4_RESERVATIONS_DECLARATION.ACCOUNTING_ENTITY_ID",
        ],
        "where": [
            "ACCOUNTING_ENTITY.ID",
            "DECLARATION_V2.DECLARATION_REF",
            "#MRNFORMOVEMENT",
        ],
    }
    assert parser.comments == []


def test_next_token_not_comment_single():
    query = """
        SELECT column_1 -- comment_1
        FROM table_1 
    """
    parser = Parser(query)
    column_1_tok = parser.tokens[1]

    assert column_1_tok.next_token.is_comment
    assert not column_1_tok.next_token_not_comment.is_comment
    assert column_1_tok.next_token.next_token == column_1_tok.next_token_not_comment


def test_next_token_not_comment_multiple():
    query = """
            SELECT column_1 -- comment_1
            
            /*
            comment_2
            */
            
            # comment_3
            FROM table_1
        """
    parser = Parser(query)
    column_1_tok = parser.tokens[1]

    assert column_1_tok.next_token.is_comment
    assert column_1_tok.next_token.next_token.is_comment
    assert column_1_tok.next_token.next_token.next_token.is_comment
    assert not column_1_tok.next_token_not_comment.is_comment
    assert (
        column_1_tok.next_token.next_token.next_token.next_token
        == column_1_tok.next_token_not_comment
    )


def test_next_token_not_comment_on_non_comments():
    query = """
            SELECT column_1
            FROM table_1
        """
    parser = Parser(query)
    select_tok = parser.tokens[0]

    assert select_tok.next_token == select_tok.next_token_not_comment
    assert (
        select_tok.next_token.next_token
        == select_tok.next_token_not_comment.next_token_not_comment
    )


def test_without_comments_for_multiline_query():
    query = """SELECT * -- comment
        FROM table
        WHERE table.id = '123'"""
    parser = Parser(query)
    assert parser.without_comments == """SELECT * FROM table WHERE table.id = '123'"""
