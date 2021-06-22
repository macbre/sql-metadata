from sql_metadata import Parser


def test_multiple_subqueries():
    query = """
SELECT main_qry.*,
       subdays.DAYS_OFFER1,
       subdays.DAYS_OFFER2,
       subdays.DAYS_OFFER3
from (
         SELECT jr.id                                                                                   as PROJECT_ID,
                5 * (DATEDIFF(ifnull(lc.creation_date, now()), jr.creation_date) DIV 7)
                    + MID('0123444401233334012222340111123400001234000123440',
                          7 * WEEKDAY(jr.creation_date) + WEEKDAY(ifnull(lc.creation_date, now())) + 1, 1) as LIFETIME,
                count(distinct
                      case when jra.application_source = 'VERAMA' then jra.id else null end)                 NUM_APPLICATIONS,
                count(distinct jra.id)                                                                   NUM_CANDIDATES,
                sum(case when jro.stage = 'DEAL' then 1 else 0 end)                                 as NUM_CONTRACTED,
                sum(ifnull(IS_INTERVIEW, 0))                                                            as NUM_INTERVIEWED,
                sum(ifnull(IS_PRESENTATION, 0))                                                         as NUM_OFFERED
         from job_request jr
                  left join job_request_application jra on jr.id = jra.job_request_id
                  left join job_request_offer jro on jro.job_request_application_id = jra.id
                  left join lifecycle lc on lc.object_id=jr.id and lc.lifecycle_object_type='JOB_REQUEST' 
                  and lc.event = 'JOB_REQUEST_CLOSED'
                  left join (SELECT jro2.job_request_application_id,
                                    max(case
                                            when jro2.first_interview_scheduled_date is not null then 1
                                            else 0 end)                                                    as IS_INTERVIEW,
                                    max(case when jro2.first_presented_date is not null then 1 else 0 end) as IS_PRESENTATION
                             from job_request_offer jro2
                             group by 1) jrah2 on jra.id = jrah2.job_request_application_id
                  left join client u on jr.client_id = u.id
         where jr.from_point_break = 0
           and u.name not in ('Test', 'Demo Client')
         group by 1, 2) main_qry
         left join (
    SELECT PROJECT_ID,
           sum(case when RowNo = 1 then days_to_offer else null end) as DAYS_OFFER1,
           sum(case when RowNo = 2 then days_to_offer else null end) as DAYS_OFFER2,
           sum(case when RowNo = 3 then days_to_offer else null end) as DAYS_OFFER3
    from (SELECT PROJECT_ID,
                 days_to_offer,
                 (SELECT count(distinct jro.job_request_application_id)
                  from job_request_offer jro
                           left join job_request_application jra2 on jro.job_request_application_id = jra2.id
                  where jra2.job_request_id = PROJECT_ID
                    and jro.first_presented_date is not null
                    and jro.first_presented_date <= InitialChangeDate
                 ) as RowNo
          from (
                   SELECT jr.id                    as PROJECT_ID,
                          5 * (DATEDIFF(jro.first_presented_date, jr.creation_date) DIV 7) +
                          MID('0123444401233334012222340111123400001234000123440',
                              7 * WEEKDAY(jr.creation_date) + WEEKDAY(jro.first_presented_date) + 1,
                              1)                   as days_to_offer,
                          jro.job_request_application_id,
                          jro.first_presented_date as InitialChangeDate
                   from presentation pr
                            left join presentation_job_request_offer pjro on pr.id = pjro.presentation_id
                            left join job_request_offer jro on pjro.job_request_offer_id = jro.id
                            left join job_request jr on pr.job_request_id = jr.id
                   where jro.first_presented_date is not null) days_sqry) days_final_qry
    group by PROJECT_ID) subdays
                   on subdays.PROJECT_ID = main_qry.PROJECT_ID
"""
    parser = Parser(query)
    assert parser.subqueries_names == [
        "jrah2",
        "main_qry",
        "days_sqry",
        "days_final_qry",
        "subdays",
    ]

    assert parser.columns_aliases == {
        "DAYS_OFFER1": ["RowNo", "days_to_offer"],
        "DAYS_OFFER2": ["RowNo", "days_to_offer"],
        "DAYS_OFFER3": ["RowNo", "days_to_offer"],
        "days_to_offer": [
            "job_request_offer.first_presented_date",
            "job_request.creation_date",
        ],
        "IS_INTERVIEW": "job_request_offer.first_interview_scheduled_date",
        "IS_PRESENTATION": "job_request_offer.first_presented_date",
        "InitialChangeDate": "job_request_offer.first_presented_date",
        "LIFETIME": ["lifecycle.creation_date", "job_request.creation_date"],
        "NUM_APPLICATIONS": [
            "job_request_application.application_source",
            "job_request_application.id",
        ],
        "NUM_CANDIDATES": "job_request_application.id",
        "NUM_CONTRACTED": "job_request_offer.stage",
        "NUM_INTERVIEWED": "IS_INTERVIEW",
        "NUM_OFFERED": "IS_PRESENTATION",
        "PROJECT_ID": "job_request.id",
        "RowNo": "job_request_offer.job_request_application_id",
    }

    assert parser.columns == [
        "job_request.id",
        "lifecycle.creation_date",
        "job_request.creation_date",
        "job_request_application.application_source",
        "job_request_application.id",
        "job_request_offer.stage",
        "job_request_application.job_request_id",
        "job_request_offer.job_request_application_id",
        "lifecycle.object_id",
        "lifecycle.lifecycle_object_type",
        "lifecycle.event",
        "job_request_offer.first_interview_scheduled_date",
        "job_request_offer.first_presented_date",
        "job_request.client_id",
        "client.id",
        "job_request.from_point_break",
        "client.name",
        "presentation.id",
        "presentation_job_request_offer.presentation_id",
        "presentation_job_request_offer.job_request_offer_id",
        "job_request_offer.id",
        "presentation.job_request_id",
    ]
    assert parser.subqueries == {
        "days_final_qry": "SELECT PROJECT_ID, days_to_offer, (SELECT count(distinct "
        "jro.job_request_application_id) from job_request_offer jro "
        "left join job_request_application jra2 on "
        "jro.job_request_application_id = jra2.id where "
        "jra2.job_request_id = PROJECT_ID and "
        "jro.first_presented_date is not null and "
        "jro.first_presented_date <= InitialChangeDate) as RowNo "
        "from (SELECT jr.id as PROJECT_ID, 5 * "
        "(DATEDIFF(jro.first_presented_date, jr.creation_date) DIV "
        "7) + "
        "MID('0123444401233334012222340111123400001234000123440', 7 "
        "* WEEKDAY(jr.creation_date) + "
        "WEEKDAY(jro.first_presented_date) + 1, 1) as "
        "days_to_offer, jro.job_request_application_id, "
        "jro.first_presented_date as InitialChangeDate from "
        "presentation pr left join presentation_job_request_offer "
        "pjro on pr.id = pjro.presentation_id left join "
        "job_request_offer jro on pjro.job_request_offer_id = "
        "jro.id left join job_request jr on pr.job_request_id = "
        "jr.id where jro.first_presented_date is not null) "
        "days_sqry",
        "days_sqry": "SELECT jr.id as PROJECT_ID, 5 * "
        "(DATEDIFF(jro.first_presented_date, jr.creation_date) DIV 7) + "
        "MID('0123444401233334012222340111123400001234000123440', 7 * "
        "WEEKDAY(jr.creation_date) + WEEKDAY(jro.first_presented_date) + "
        "1, 1) as days_to_offer, jro.job_request_application_id, "
        "jro.first_presented_date as InitialChangeDate from presentation "
        "pr left join presentation_job_request_offer pjro on pr.id = "
        "pjro.presentation_id left join job_request_offer jro on "
        "pjro.job_request_offer_id = jro.id left join job_request jr on "
        "pr.job_request_id = jr.id where jro.first_presented_date is not "
        "null",
        "jrah2": "SELECT jro2.job_request_application_id, max(case when "
        "jro2.first_interview_scheduled_date is not null then 1 else 0 end) "
        "as IS_INTERVIEW, max(case when jro2.first_presented_date is not "
        "null then 1 else 0 end) as IS_PRESENTATION from job_request_offer "
        "jro2 group by 1",
        "main_qry": "SELECT jr.id as PROJECT_ID, 5 * "
        "(DATEDIFF(ifnull(lc.creation_date, now()), jr.creation_date) DIV "
        "7) + MID('0123444401233334012222340111123400001234000123440', 7 "
        "* WEEKDAY(jr.creation_date) + WEEKDAY(ifnull(lc.creation_date, "
        "now())) + 1, 1) as LIFETIME, count(distinct case when "
        "jra.application_source = 'VERAMA' then jra.id else null end) "
        "NUM_APPLICATIONS, count(distinct jra.id) NUM_CANDIDATES, "
        "sum(case when jro.stage = 'DEAL' then 1 else 0 end) as "
        "NUM_CONTRACTED, sum(ifnull(IS_INTERVIEW, 0)) as NUM_INTERVIEWED, "
        "sum(ifnull(IS_PRESENTATION, 0)) as NUM_OFFERED from job_request "
        "jr left join job_request_application jra on jr.id = "
        "jra.job_request_id left join job_request_offer jro on "
        "jro.job_request_application_id = jra.id left join lifecycle lc "
        "on lc.object_id = jr.id and lc.lifecycle_object_type = "
        "'JOB_REQUEST' and lc.event = 'JOB_REQUEST_CLOSED' left join "
        "(SELECT jro2.job_request_application_id, max(case when "
        "jro2.first_interview_scheduled_date is not null then 1 else 0 "
        "end) as IS_INTERVIEW, max(case when jro2.first_presented_date is "
        "not null then 1 else 0 end) as IS_PRESENTATION from "
        "job_request_offer jro2 group by 1) jrah2 on jra.id = "
        "jrah2.job_request_application_id left join client u on "
        "jr.client_id = u.id where jr.from_point_break = 0 and u.name not "
        "in ('Test', 'Demo Client') group by 1, 2",
        "subdays": "SELECT PROJECT_ID, sum(case when RowNo = 1 then days_to_offer "
        "else null end) as DAYS_OFFER1, sum(case when RowNo = 2 then "
        "days_to_offer else null end) as DAYS_OFFER2, sum(case when RowNo "
        "= 3 then days_to_offer else null end) as DAYS_OFFER3 from (SELECT "
        "PROJECT_ID, days_to_offer, (SELECT count(distinct "
        "jro.job_request_application_id) from job_request_offer jro left "
        "join job_request_application jra2 on "
        "jro.job_request_application_id = jra2.id where "
        "jra2.job_request_id = PROJECT_ID and jro.first_presented_date is "
        "not null and jro.first_presented_date <= InitialChangeDate) as "
        "RowNo from (SELECT jr.id as PROJECT_ID, 5 * "
        "(DATEDIFF(jro.first_presented_date, jr.creation_date) DIV 7) + "
        "MID('0123444401233334012222340111123400001234000123440', 7 * "
        "WEEKDAY(jr.creation_date) + WEEKDAY(jro.first_presented_date) + "
        "1, 1) as days_to_offer, jro.job_request_application_id, "
        "jro.first_presented_date as InitialChangeDate from presentation "
        "pr left join presentation_job_request_offer pjro on pr.id = "
        "pjro.presentation_id left join job_request_offer jro on "
        "pjro.job_request_offer_id = jro.id left join job_request jr on "
        "pr.job_request_id = jr.id where jro.first_presented_date is not "
        "null) days_sqry) days_final_qry group by PROJECT_ID",
    }


def test_multiline_queries():
    query = """
SELECT
COUNT(1)
FROM
(SELECT
std.task_id as new_task_id
FROM
some_task_detail std
WHERE
std.STATUS = 1
) a
JOIN (
SELECT
st.task_id
FROM
some_task st
WHERE
task_type_id = 80
) as b ON a.new_task_id = b.task_id;
    """.strip()

    parser = Parser(query)
    assert parser.subqueries_names == ["a", "b"]
    assert parser.tables == ["some_task_detail", "some_task"]
    assert parser.columns_aliases_names == ["new_task_id"]
    assert parser.columns_aliases == {"new_task_id": "some_task_detail.task_id"}
    assert parser.columns == [
        "some_task_detail.task_id",
        "some_task_detail.STATUS",
        "some_task.task_id",
        "task_type_id",
    ]
    assert parser.columns_dict == {
        "join": ["some_task_detail.task_id", "some_task.task_id"],
        "select": ["some_task_detail.task_id", "some_task.task_id"],
        "where": ["some_task_detail.STATUS", "task_type_id"],
    }

    assert parser.subqueries == {
        "a": "SELECT std.task_id as new_task_id FROM some_task_detail std WHERE std.STATUS = 1",
        "b": "SELECT st.task_id FROM some_task st WHERE task_type_id = 80",
    }

    parser2 = Parser(parser.subqueries["a"])
    assert parser2.tables == ["some_task_detail"]
    assert parser2.columns == ["some_task_detail.task_id", "some_task_detail.STATUS"]


def test_resolving_columns_in_sub_queries_simple_select_with_order_by():
    query = """
    Select sub_alias, other_name from (
    select aa as sub_alias, bb as other_name from tab1
    ) sq order by other_name, sub_alias
    """

    parser = Parser(query)
    assert parser.columns == ["aa", "bb"]
    assert parser.columns_aliases == {"other_name": "bb", "sub_alias": "aa"}
    assert parser.columns_dict == {"order_by": ["bb", "aa"], "select": ["aa", "bb"]}
    assert parser.subqueries_names == ["sq"]


def test_resolving_columns_in_sub_queries_nested_subquery():
    query = """
    Select sub_alias, other_name from (
        select sub_alias, other_name from (
            select cc as sub_alias , uaua as other_name from tab1) sq2
    ) sq order by other_name
    """

    parser = Parser(query)
    assert parser.columns == ["cc", "uaua"]
    assert parser.columns_aliases == {"other_name": "uaua", "sub_alias": "cc"}
    assert parser.columns_dict == {"order_by": ["uaua"], "select": ["cc", "uaua"]}
    assert parser.subqueries_names == ["sq2", "sq"]


def test_resolving_columns_in_sub_queries_join():
    query = """
    Select sq.sub_alias, sq.other_name from (
        select tab1.aa sub_alias, tab2.us as other_name from tab1
        left join tab2 on tab1.id = tab2.other_id
    ) sq order by sq.other_name
    """

    parser = Parser(query)
    assert parser.columns == ["tab1.aa", "tab2.us", "tab1.id", "tab2.other_id"]
    assert parser.columns_aliases == {"other_name": "tab2.us", "sub_alias": "tab1.aa"}
    assert parser.columns_dict == {
        "join": ["tab1.id", "tab2.other_id"],
        "order_by": ["tab2.us"],
        "select": ["tab1.aa", "tab2.us"],
    }
    assert parser.subqueries_names == ["sq"]


def test_resolving_columns_in_sub_queries_with_join_between_sub_queries():
    query = """
    Select sq.sub_alias, sq.other_name from (
        select tab1.aa sub_alias, tab2.us as other_name from tab1
        left join tab2 on tab1.id = tab2.other_id
    ) sq 
    left join (
        select intern1 as col1, secret col2 from aa
    ) sq3 on sq.sub_alias = sq3.col1 
    order by sq.other_name, sq3.col2
    """

    parser = Parser(query)
    assert parser.columns == [
        "tab1.aa",
        "tab2.us",
        "tab1.id",
        "tab2.other_id",
        "intern1",
        "secret",
    ]
    assert parser.columns_aliases == {
        "col1": "intern1",
        "col2": "secret",
        "other_name": "tab2.us",
        "sub_alias": "tab1.aa",
    }
    assert parser.columns_dict == {
        "join": ["tab1.id", "tab2.other_id", "tab1.aa", "intern1"],
        "order_by": ["tab2.us", "secret"],
        "select": ["tab1.aa", "tab2.us", "intern1", "secret"],
    }
    assert parser.subqueries_names == ["sq", "sq3"]


def test_resolving_columns_in_sub_queries_union():
    query = """
    Select sq.sub_alias, sq.other_name from (
        select tab1.aa sub_alias, tab2.us as other_name from tab1
        left join tab2 on tab1.id = tab2.other_id
    ) sq 
    union all 
    select sq3.col1, sq3.col2 from 
    (select tab12.col1, concat(tab23.ab, ' ', tab23.bc) as col2 
    from tab12 left join tab23 on tab12.id = tab23.zorro) sq3
    """

    parser = Parser(query)
    assert parser.columns_aliases == {
        "col2": ["tab23.ab", "tab23.bc"],
        "other_name": "tab2.us",
        "sub_alias": "tab1.aa",
    }
    assert parser.columns == [
        "tab1.aa",
        "tab2.us",
        "tab1.id",
        "tab2.other_id",
        "tab12.col1",
        "tab23.ab",
        "tab23.bc",
        "tab12.id",
        "tab23.zorro",
    ]

    assert parser.columns_dict == {
        "join": ["tab1.id", "tab2.other_id", "tab12.id", "tab23.zorro"],
        "select": ["tab1.aa", "tab2.us", "tab12.col1", "tab23.ab", "tab23.bc"],
    }
    assert parser.subqueries_names == ["sq", "sq3"]


def test_resolving_columns_in_sub_queries_functions():
    query = """
    Select sub_alias, other_name from (
    select concat(aa, ' ', uu, au) as sub_alias, bb * price as other_name from tab1
    ) sq order by other_name
    """

    parser = Parser(query)
    assert parser.columns == ["aa", "uu", "au", "bb", "price"]
    assert parser.columns_aliases == {
        "other_name": ["bb", "price"],
        "sub_alias": ["aa", "uu", "au"],
    }
    assert parser.columns_dict == {
        "order_by": ["bb", "price"],
        "select": ["aa", "uu", "au", "bb", "price"],
    }
    assert parser.subqueries_names == ["sq"]


def test_readme_query():
    parser = Parser(
        """
        SELECT COUNT(1) FROM
        (SELECT std.task_id FROM some_task_detail std WHERE std.STATUS = 1) a
        JOIN (SELECT st.task_id FROM some_task st WHERE task_type_id = 80) b
        ON a.task_id = b.task_id;
        """
    )
    assert parser.subqueries == {
        "a": "SELECT std.task_id FROM some_task_detail std WHERE std.STATUS = 1",
        "b": "SELECT st.task_id FROM some_task st WHERE task_type_id = 80",
    }
    assert parser.subqueries_names == ["a", "b"]
    assert parser.columns == [
        "some_task_detail.task_id",
        "some_task_detail.STATUS",
        "some_task.task_id",
        "task_type_id",
    ]
    assert parser.columns_dict == {
        "join": ["some_task_detail.task_id", "some_task.task_id"],
        "select": ["some_task_detail.task_id", "some_task.task_id"],
        "where": ["some_task_detail.STATUS", "task_type_id"],
    }
