from sql_metadata import Parser


def test_column_aliases_with_subquery():
    query = """
    SELECT yearweek(SignDate) as                         Aggregation,
       BusinessSource,
       (SELECT sum(C2Count)
        from (SELECT count(C2) as C2Count, BusinessSource,
        yearweek(Start1) Start1, yearweek(End1) End1
              from (
                       SELECT ContractID as C2, BusinessSource, StartDate as Start1,
                       EndDate as End1
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
        "sq": "SELECT count(C2) as C2Count, BusinessSource, yearweek(Start1) Start1, "
        "yearweek(End1) End1 from (SELECT ContractID as C2, BusinessSource, "
        "StartDate as Start1, EndDate as End1 from data_contracts_report) sq2 "
        "group by 2, 3, 4",
        "sq2": "SELECT ContractID as C2, BusinessSource, StartDate as Start1, EndDate "
        "as End1 from data_contracts_report",
    }
    assert parser.columns == [
        "SignDate",
        "BusinessSource",
        "ContractID",
        "StartDate",
        "EndDate",
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
    SELECT a, (b + c - u) as alias1, custome_func(d) alias2 from aa, bb order by alias1
    """
    parser = Parser(query)
    assert parser.tables == ["aa", "bb"]
    assert parser.columns == ["a", "b", "c", "u", "d"]
    assert parser.columns_aliases_names == ["alias1", "alias2"]
    assert parser.columns_aliases == {"alias1": ["b", "c", "u"], "alias2": "d"}
    assert parser.columns_aliases_dict == {
        "order_by": ["alias1"],
        "select": ["alias1", "alias2"],
    }
    assert parser.columns_dict == {
        "order_by": ["b", "c", "u"],
        "select": ["a", "b", "c", "u", "d"],
    }


def test_mutiple_functions():
    parser = Parser(
        "SELECT count(col) + max(col2) + min(col3)"
        "+ count(distinct  col4) + custom_func(col5) as result from dual"
    )
    assert parser.columns == ["col", "col2", "col3", "col4", "col5"]
    assert parser.columns_aliases_names == ["result"]
    assert parser.columns_aliases == {"result": ["col", "col2", "col3", "col4", "col5"]}


def test_cast_in_where():
    parser = Parser(
        "SELECT count(c) as test, id as uu FROM foo where cast(d as bigint) > uu"
    )
    assert parser.columns_aliases_names == ["test", "uu"]
    assert parser.columns_aliases == {"test": "c", "uu": "id"}
    assert parser.columns_aliases_dict == {"select": ["test", "uu"], "where": ["uu"]}


def test_cast_in_select():
    parser = Parser("select CAST(test as STRING) as test1 from table")
    assert parser.columns_aliases_names == ["test1"]
    assert parser.columns_aliases == {"test1": "test"}
    assert parser.columns_aliases_dict == {"select": ["test1"]}


def test_convert_in_select():
    parser = Parser(
        "SELECT CONVERT(latin1_column USING utf8) as alias FROM latin1_table;"
    )
    assert parser.columns_aliases_names == ["alias"]
    assert parser.columns_aliases == {"alias": "latin1_column"}
    assert parser.columns_aliases_dict == {"select": ["alias"]}


def test_convert_in_join():
    parser = Parser(
        "SELECT la1.col1, la2.col2, CONVERT(la1.col2 USING utf8) FROM latin1_table la1 "
        "left join latin2_table la2 "
        "on CONVERT(la1.latin1_column USING utf8) = "
        "CONVERT(la2.latin1_column USING utf8) "
        "left join latin3_table la3 using (col1, col2);"
    )
    assert parser.columns == [
        "latin1_table.col1",
        "latin2_table.col2",
        "latin1_table.col2",
        "latin1_table.latin1_column",
        "latin2_table.latin1_column",
        "col1",
        "col2",
    ]
    assert parser.columns_dict == {
        "join": [
            "latin1_table.latin1_column",
            "latin2_table.latin1_column",
            "col1",
            "col2",
        ],
        "select": ["latin1_table.col1", "latin2_table.col2", "latin1_table.col2"],
    }
    assert parser.tables == ["latin1_table", "latin2_table", "latin3_table"]


def test_cast_in_select_with_function():
    query = """
    SELECT
    t_alias.id as UniqueId,
    CAST(date_format(t_alias.date, 'yyyyMMdd') as INT) as datekey,
    CAST(concat( '1',
    case when LENGTH(hour(t_alias.starttime))
    then hour(t_alias.starttime)
    else concat('0', hour(t_alias.starttime)) end,
   case when LENGTH(minute(t_alias.starttime)) > 1
   then minute(t_alias.starttime)
   else concat('0', minute(t_alias.starttime)) end
   ) as INT)
    as starttimekey
  FROM testdb.test_table t_alias
    """
    parser = Parser(query)
    assert parser.columns_aliases == {
        "UniqueId": "testdb.test_table.id",
        "datekey": "testdb.test_table.date",
        "starttimekey": "testdb.test_table.starttime",
    }


def test_nested_function():
    query = """
        SELECT a * b
        FROM c
        WHERE b = (SELECT MAX(b) FROM c);
    """
    parser = Parser(query)

    assert parser.columns == ["a", "b"]
    assert parser.tables == ["c"]
