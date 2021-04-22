import sqlparse

from sql_metadata import Parser

import pathlib

dir_path = pathlib.Path(__file__).parent.absolute()


def test_complex_query_aliases():
    sql_filename = f"{dir_path}/test.sql"
    with open(sql_filename, "r", encoding="latin-1") as content_file:
        content = content_file.read()
    content = sqlparse.format(content, strip_comments=True).strip()
    assert {
               "C": "EBH_DM_CRM.CRM_CUSTOMER",
               "RB_ACCT": "KMR_FINANCIAL_INDEX_STAGE_01",
               "OD_FAC": "KMR_FINANCIAL_INDEX_STAGE_07",
               "CARD": "KMR_FINANCIAL_INDEX_STAGE_02",
               "CL_LOAN": "KMR_FINANCIAL_INDEX_STAGE_03",
               "CL_INV": "KMR_FINANCIAL_INDEX_STAGE_05",
               "BEF_ZRT": "KMR_FINANCIAL_INDEX_STAGE_04",
               "AR": "SCHEMA.CRM_ARRANGEMENT",
               "L": "SCHEMA.CRM_LOAN",
               "CF": "SCHEMA.CRM_CASH_FLOW",
               "CA": "SCHEMA.CRM_REL_CUSTOMER_ARRANGEMENT",
               "LPRE": "SCHEMA.CRM_LOAN",
               "P": "SCHEMA.CRM_PRODUCT",
               "PD": "SCHEMA.CRM_PAST_DUE",
               "DAT": "SCHEMA.CRM_DATE",
               "BLL": "SCHEMA.CRM_BALANCE_LOAN_LOAN",
               "E": "SCHEMA.CRM_EXCHANGE_RATE",
               "LE": "SCHEMA.CRM_LEASING",
               "BLE": "SCHEMA.CRM_BALANCE_LEASING",
               "SCB": "SCHEMA.CRM_RL_SUBSIDIARY_COMPANIE_BAT",
               "SZCH": "KMR_FINANCIAL_INDEX_STAGE_06",
           } == Parser(content).tables_aliases
