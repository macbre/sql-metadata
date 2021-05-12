"""
This module provides a temporary compatibility layer for legacy API dating back to 1.x version.

Change your old imports:

from sql_metadata import get_query_columns, get_query_tables

into:

from sql_metadata.compat import get_query_columns, get_query_tables

"""
from typing import List, Optional, Tuple

import sqlparse


def preprocess_query(query: str) -> str:
    pass


def get_query_tokens(query: str)  -> List[sqlparse.sql.Token]:
    pass


def get_query_columns(query: str) -> List[str]:
    pass


def get_query_tables(query: str) -> List[str]:
    pass


def get_query_limit_and_offset(query: str) -> Optional[Tuple[int, int]]:
    pass


def generalize_sql(sql: Optional[str]) -> Optional[str]:
    pass
