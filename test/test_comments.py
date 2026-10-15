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


def test_without_comments_for_multiline_query():
    query = """SELECT * -- comment
        FROM table
        WHERE table.id = '123'"""
    parser = Parser(query)
    assert parser.without_comments == """SELECT * FROM table WHERE table.id = '123'"""


def test_table_after_comment_not_ignored():
    # solved: https://github.com/macbre/sql-metadata/issues/251
    query = """SELECT c1 FROM
       --Comment--
        d1, d2, d3"""
    parser = Parser(query)
    assert parser.tables == ["d1", "d2", "d3"]
    assert parser.columns == ["c1"]
    assert parser.columns_dict == {"select": ["c1"]}


def test_extract_comments_empty_string():
    """Extracting comments from empty SQL returns empty list."""
    assert Parser("").comments == []


def test_strip_comments_empty_string():
    """Stripping comments from empty SQL returns empty string."""
    assert Parser("").without_comments == ""


def test_strip_comments_for_parsing_empty():
    """SqlCleaner handles empty strings via strip_comments_for_parsing."""
    from sql_metadata.comments import strip_comments_for_parsing

    assert strip_comments_for_parsing("") == ""


def test_without_comments_preserves_tsql_temp_tables():
    """Preserve T-SQL temporary table names (#tmp, ##tmp) in without_comments."""
    # DROP TABLE
    drop_query = "DROP TABLE IF EXISTS #HistRaw; SELECT 1"
    parser = Parser(drop_query)
    assert parser.without_comments == "DROP TABLE IF EXISTS #HistRaw; SELECT 1"
    assert parser.comments == []

    drop_simple = "DROP TABLE #tmp"
    parser = Parser(drop_simple)
    assert parser.without_comments == "DROP TABLE #tmp"
    assert parser.comments == []

    # SELECT INTO
    into_query = "SELECT a INTO #tmp FROM t; SELECT a FROM #tmp"
    parser = Parser(into_query)
    assert parser.without_comments == "SELECT a INTO #tmp FROM t; SELECT a FROM #tmp"
    assert parser.comments == []

    # FROM
    from_query = "SELECT a FROM #tmp"
    parser = Parser(from_query)
    assert parser.without_comments == "SELECT a FROM #tmp"
    assert parser.comments == []

    # JOIN
    join_query = "SELECT t.a, tmp.b FROM t INNER JOIN #tmp tmp ON t.id = tmp.id"
    parser = Parser(join_query)
    assert (
        parser.without_comments
        == "SELECT t.a, tmp.b FROM t INNER JOIN #tmp tmp ON t.id = tmp.id"
    )
    assert parser.comments == []

    # UPDATE
    update_query = "UPDATE #tmp SET val = 1"
    parser = Parser(update_query)
    assert parser.without_comments == "UPDATE #tmp SET val = 1"
    assert parser.comments == []

    # Comma-separated tables
    comma_query = "SELECT * FROM t, #tmp WHERE t.id = #tmp.id"
    parser = Parser(comma_query)
    assert parser.without_comments == "SELECT * FROM t, #tmp WHERE t.id = #tmp.id"
    assert parser.comments == []

    # Global temp table (##tmp)
    global_query = "SELECT a FROM ##tmp"
    parser = Parser(global_query)
    assert parser.without_comments == "SELECT a FROM ##tmp"
    assert parser.comments == []

