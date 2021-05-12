"""
This module provides a temporary compatibility layer for legacy API dating back to 1.x version.

Change your old imports:

from sql_metadata import get_query_columns, get_query_tables

into:

from sql_metadata.compat import get_query_columns, get_query_tables

"""
# pylint:disable=missing-function-docstring
from typing import List, Optional, Tuple

import sqlparse

from sql_metadata import Parser


def preprocess_query(query: str) -> str:
    return Parser(query).query


def get_query_tokens(query: str) -> List[sqlparse.sql.Token]:
    pass


def get_query_columns(query: str) -> List[str]:
    return Parser(query).columns


def get_query_tables(query: str) -> List[str]:
    return Parser(query).tables


def get_query_limit_and_offset(query: str) -> Optional[Tuple[int, int]]:
    return Parser(query).limit_and_offset


def generalize_sql(query: Optional[str] = None) -> Optional[str]:
    if query is None:
        return None

    return Parser(query).generalize
