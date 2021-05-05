from sql_metadata import Parser


def test_column_aliases_with_subquery():
    query = """
    SELECT yearweek(SignDate) as                         Aggregation,
       BusinessSource,
       (select sum(C2Count)
        from (select count(C2) as C2Count, BusinessSource, yearweek(Start1) Start1, yearweek(End1) End1
              from (
                       select ContractID as C2, BusinessSource, StartDate as Start1, EndDate as End1
                       from data_contracts_report
                   ) sq2
              group by 2, 3, 4) sq
        where Start1 <= yearweek(SignDate)
          and End1 >= yearweek(SignDate)
          and sq.BusinessSource = mq.BusinessSource) CountOfConsultants
FROM data_contracts_report mq
where SignDate >= last_day(date_add(now(), interval -13 month))
group by 1, 2
order by 1, 2;
    """
    parser = Parser(query)
    assert parser.tables == ["data_contracts_report"]
    assert parser.subqueries_names == ["sq2", "sq"]
    assert parser.subqueries == {
        "sq": "select count(C2) as C2Count, BusinessSource, yearweek(Start1) Start1, "
        "yearweek(End1) End1 from (select ContractID as C2, BusinessSource, "
        "StartDate as Start1, EndDate as End1 from data_contracts_report) sq2 "
        "group by 2, 3, 4",
        "sq2": "select ContractID as C2, BusinessSource, StartDate as Start1, EndDate "
        "as End1 from data_contracts_report",
    }
    assert parser.columns == [
        "SignDate",
        "BusinessSource",
        "ContractID",
        "StartDate",
        "EndDate",
        "sq.BusinessSource",
        "data_contracts_report.BusinessSource",
    ]
    assert parser.columns_aliases_names == [
        "Aggregation",
        "C2Count",
        "Start1",
        "End1",
        "C2",
        "CountOfConsultants",
    ]
    assert parser.columns_aliases == {
        "Aggregation": "SignDate",
        "C2": "ContractID",
        "C2Count": "C2",
        "CountOfConsultants": "C2Count",
        "End1": "EndDate",
        "Start1": "StartDate",
    }


def test_column_aliases_with_multiple_functions():
    query = """
    SELECT a, sum(b) + sum(c) as alias1, custome_func(d) alias2 from aa, bb
    """
    parser = Parser(query)
    assert parser.tables == ["aa", "bb"]
    assert parser.columns == ["a", "b", "c", "d"]
    assert parser.columns_aliases_names == ["alias1", "alias2"]
    assert parser.columns_aliases == {"alias1": ["b", "c"], "alias2": "d"}


def test_column_aliases_with_columns_operations():
    query = """
    SELECT a, b + c - u as alias1, custome_func(d) alias2 from aa, bb
    """
    parser = Parser(query)
    assert parser.tables == ["aa", "bb"]
    assert parser.columns == ["a", "b", "c", "u", "d"]
    assert parser.columns_aliases_names == ["alias1", "alias2"]
    assert parser.columns_aliases == {"alias1": ["b", "c", "u"], "alias2": "d"}


def test_column_aliases_with_redundant_brackets():
    query = """
    SELECT a, (b + c - u) as alias1, custome_func(d) alias2 from aa, bb
    """
    parser = Parser(query)
    assert parser.tables == ["aa", "bb"]
    assert parser.columns == ["a", "b", "c", "u", "d"]
    assert parser.columns_aliases_names == ["alias1", "alias2"]
    assert parser.columns_aliases == {"alias1": ["b", "c", "u"], "alias2": "d"}


def test_mutiple_functions():
    parser = Parser(
        "select count(col) + max(col2) + min(col3)"
        "+ count(distinct  col4) + custom_func(col5) as result from dual"
    )
    assert parser.columns == ["col", "col2", "col3", "col4", "col5"]
    assert parser.columns_aliases_names == ["result"]
    assert parser.columns_aliases == {"result": ["col", "col2", "col3", "col4", "col5"]}
