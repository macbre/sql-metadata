from sql_metadata import Parser


def test_column_aliases():
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
        "C2Count",
        "C2",
        "Start1",
        "End1",
        "ContractID",
        "StartDate",
        "EndDate",
        "sq.BusinessSource",
        "data_contracts_report.BusinessSource",
        "CountOfConsultants",
    ]
