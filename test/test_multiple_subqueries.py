from sql_metadata import Parser


def test_multiple_subqueries():
    query = """
select main_qry.*,
       subdays.DAYS_OFFER1,
       subdays.DAYS_OFFER2,
       subdays.DAYS_OFFER3
from (
         select jr.id                                                                                   as PROJECT_ID,
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
                  left join (select jro2.job_request_application_id,
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
    select PROJECT_ID,
           sum(case when RowNo = 1 then days_to_offer else null end) as DAYS_OFFER1,
           sum(case when RowNo = 2 then days_to_offer else null end) as DAYS_OFFER2,
           sum(case when RowNo = 3 then days_to_offer else null end) as DAYS_OFFER3
    from (select PROJECT_ID,
                 days_to_offer,
                 (select count(distinct jro.job_request_application_id)
                  from job_request_offer jro
                           left join job_request_application jra2 on jro.job_request_application_id = jra2.id
                  where jra2.job_request_id = PROJECT_ID
                    and jro.first_presented_date is not null
                    and jro.first_presented_date <= InitialChangeDate
                 ) as RowNo
          from (
                   select jr.id                    as PROJECT_ID,
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
    assert parser.columns == [
        "main_qry.*",
        "subdays.DAYS_OFFER1",  # subquery nested resolve?
        "subdays.DAYS_OFFER2",  # subquery nested resolve?
        "subdays.DAYS_OFFER3",  # subquery nested resolve?
        "job_request.id",
        "lifecycle.creation_date",
        "job_request.creation_date",
        "job_request_application.application_source",
        "job_request_application.job_request_id",
        "job_request_offer.job_request_application_id",
        "job_request_application.id",
        "lifecycle.object_id",
        "lifecycle.lifecycle_object_type",
        "lifecycle.event",
        "job_request_offer.first_interview_scheduled_date",
        "jrah2.job_request_application_id",  # subquery nested resolve?
        "job_request.client_id",
        "client.id",
        "job_request.from_point_break",
        "client.name",
        "PROJECT_ID",  # recursive search?
        "RowNo",  # subquery name?
        "days_to_offer",  # should be resoled?
        "job_request_offer.first_presented_date",
        "InitialChangeDate",  # alias of other column
        "presentation.id",
        "presentation_job_request_offer.presentation_id",
        "presentation_job_request_offer.job_request_offer_id",
        "job_request_offer.id",
        "presentation.job_request_id",
        "subdays.PROJECT_ID",  # subquery nested resolve?
        "main_qry.PROJECT_ID",  # subquery nested resolve?
    ]
    assert parser.columns_without_subqueries == [
        "job_request.id",
        "lifecycle.creation_date",
        "job_request.creation_date",
        "job_request_application.application_source",
        "job_request_application.job_request_id",
        "job_request_offer.job_request_application_id",
        "job_request_application.id",
        "lifecycle.object_id",
        "lifecycle.lifecycle_object_type",
        "lifecycle.event",
        "job_request_offer.first_interview_scheduled_date",
        "job_request.client_id",
        "client.id",
        "job_request.from_point_break",
        "client.name",
        "PROJECT_ID",  # recursive search?
        "RowNo",  # subquery name?
        "days_to_offer",  # should be resoled?
        "job_request_offer.first_presented_date",
        "InitialChangeDate",  # alias of other column
        "presentation.id",
        "presentation_job_request_offer.presentation_id",
        "presentation_job_request_offer.job_request_offer_id",
        "job_request_offer.id",
        "presentation.job_request_id",
    ]
    assert parser.subqueries == {
        "days_final_qry": "select PROJECT_ID, days_to_offer, (select count(distinct "
        "jro.job_request_application_id) from job_request_offer jro "
        "left join job_request_application jra2 on "
        "jro.job_request_application_id = jra2.id where "
        "jra2.job_request_id = PROJECT_ID and "
        "jro.first_presented_date is not null and "
        "jro.first_presented_date <= InitialChangeDate) as RowNo "
        "from (select jr.id as PROJECT_ID, 5 * "
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
        "days_sqry": "select jr.id as PROJECT_ID, 5 * "
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
        "jrah2": "select jro2.job_request_application_id, max(case when "
        "jro2.first_interview_scheduled_date is not null then 1 else 0 end) "
        "as IS_INTERVIEW, max(case when jro2.first_presented_date is not "
        "null then 1 else 0 end) as IS_PRESENTATION from job_request_offer "
        "jro2 group by 1",
        "main_qry": "select jr.id as PROJECT_ID, 5 * "
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
        "(select jro2.job_request_application_id, max(case when "
        "jro2.first_interview_scheduled_date is not null then 1 else 0 "
        "end) as IS_INTERVIEW, max(case when jro2.first_presented_date is "
        "not null then 1 else 0 end) as IS_PRESENTATION from "
        "job_request_offer jro2 group by 1) jrah2 on jra.id = "
        "jrah2.job_request_application_id left join client u on "
        "jr.client_id = u.id where jr.from_point_break = 0 and u.name not "
        "in ('Test', 'Demo Client') group by 1, 2",
        "subdays": "select PROJECT_ID, sum(case when RowNo = 1 then days_to_offer "
        "else null end) as DAYS_OFFER1, sum(case when RowNo = 2 then "
        "days_to_offer else null end) as DAYS_OFFER2, sum(case when RowNo "
        "= 3 then days_to_offer else null end) as DAYS_OFFER3 from (select "
        "PROJECT_ID, days_to_offer, (select count(distinct "
        "jro.job_request_application_id) from job_request_offer jro left "
        "join job_request_application jra2 on "
        "jro.job_request_application_id = jra2.id where "
        "jra2.job_request_id = PROJECT_ID and jro.first_presented_date is "
        "not null and jro.first_presented_date <= InitialChangeDate) as "
        "RowNo from (select jr.id as PROJECT_ID, 5 * "
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
    assert parser.columns == [
        "some_task_detail.task_id",
        "some_task_detail.STATUS",
        "some_task.task_id",
        "task_type_id",
        "a.new_task_id",
        "b.task_id",
    ]
    assert parser.columns_without_subqueries == [
        "some_task_detail.task_id",
        "some_task_detail.STATUS",
        "some_task.task_id",
        "task_type_id",
    ]
    assert parser.columns_dict == {
        "join": ["a.new_task_id", "b.task_id"],
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
