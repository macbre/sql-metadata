from sql_metadata import Parser


def test_union_column_aliases():
    # https://github.com/macbre/sql-metadata/issues/401
    # When UNION combines queries with the same alias,
    # columns_aliases should aggregate all source columns
    query = """
    select a.A as M
    from tab1 a
    union all
    select b.B as M
    from tab2 b
    """
    parser = Parser(query)
    assert parser.columns_aliases == {"M": ["tab1.A", "tab2.B"]}
    assert parser.columns == ["tab1.A", "tab2.B"]
    assert parser.tables == ["tab1", "tab2"]


def test_union_alias_with_expression_targets():
    # Regression: scalar then list-target must not nest
    q1 = """
    SELECT a AS x FROM t1
    UNION ALL
    SELECT b + c AS x FROM t2
    """
    assert Parser(q1).columns_aliases == {"x": ["a", "b", "c"]}

    # Regression: list then list-target must not raise TypeError on UniqueList
    q2 = """
    SELECT a + b AS x FROM t1
    UNION ALL
    SELECT c + d AS x FROM t2
    """
    assert Parser(q2).columns_aliases == {"x": ["a", "b", "c", "d"]}


def test_union():
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
WHERE DV2.DECLARATION_REF = #MRNFORMOVEMENT
UNION
SELECT BX.ACCOUNTING_ENTITY_ID AS "ACCOUNTINGENTITYID" FROM BENELUX BX
INNER JOIN DECLARATION_V2 DV2 ON DV2.ID = BX.DECLARATION_ID
WHERE DV2.DECLARATION_REF = #MRNFORMOVEMENT
UNION
SELECT CA4D.ACCOUNTING_ENTITY_ID AS "ACCOUNTINGENTITYID" FROM RESERVATION R
INNER JOIN CA4_RESERVATIONS_DECLARATION CA4D ON CA4D.ID = R.CA4_ID
INNER JOIN DECLARATION_V2 DV2 ON DV2.ID = R.DECLARATION_ID
WHERE DV2.DECLARATION_REF = #MRNFORMOVEMENT)
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
