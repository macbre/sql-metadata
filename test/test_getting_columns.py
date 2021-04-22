from sql_metadata.parser import Parser


def test_cast_and_convert_functions():
    # https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html
    assert Parser(
        "SELECT count(c) as test, id FROM foo where cast(d as bigint) > e"
    ).columns == ["c", "id", "d", "e"]
    assert Parser(
        "SELECT CONVERT(latin1_column USING utf8) FROM latin1_table;"
    ).columns == ["latin1_column"]


def test_queries_with_null_conditions():
    assert Parser(
        "SELECT id FROM cm WHERE cm.status = 1 AND cm.OPERATIONDATE IS NULL AND cm.OID IN(123123);"
    ).columns == ["id", "cm.status", "cm.OPERATIONDATE", "cm.OID"]

    assert Parser(
        "SELECT id FROM cm WHERE cm.status = 1 AND cm.OPERATIONDATE IS NOT NULL AND cm.OID IN(123123);"
    ).columns == ["id", "cm.status", "cm.OPERATIONDATE", "cm.OID"]


def test_queries_with_distinct():
    assert Parser("SELECT DISTINCT DATA.ASSAY_ID FROM foo").columns == ["DATA.ASSAY_ID"]

    assert Parser("SELECT UNIQUE DATA.ASSAY_ID FROM foo").columns == ["DATA.ASSAY_ID"]


def test_joins():
    assert ["page_title", "rd_title", "rd_namespace", "page_id", "rd_from",] == Parser(
        "SELECT  page_title  FROM `redirect` INNER JOIN `page` "
        "ON (rd_title = 'foo' AND rd_namespace = '100' AND (page_id = rd_from))"
    ).columns
