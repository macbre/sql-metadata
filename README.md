# sql-metadata

[![PyPI](https://img.shields.io/pypi/v/sql_metadata.svg)](https://pypi.python.org/pypi/sql_metadata)
[![Tests](https://github.com/macbre/sql-metadata/actions/workflows/python-ci.yml/badge.svg)](https://github.com/macbre/sql-metadata/actions/workflows/python-ci.yml)
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
[![Maintenance](https://img.shields.io/badge/maintained%3F-yes-green.svg)](https://github.com/macbre/sql-metadata/graphs/commit-activity)
[![Downloads](https://pepy.tech/badge/sql-metadata/month)](https://pepy.tech/project/sql-metadata)

Uses tokenized query returned by [`python-sqlparse`](https://github.com/andialbrecht/sqlparse) and generates query metadata.
**Extracts column names and tables** used by the query. Provides a helper for **normalization of SQL queries** and **tables aliases resolving**.

Supported queries syntax:

* MySQL
* PostgreSQL
* [Apache Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML)

## Usage

```
pip install sql-metadata
```

### Extracting raw sql-metadata tokens

```python
from sql_metadata import Parser

# extract raw sql-metadata tokens
Parser("SELECT * FROM foo").tokens
# ['SELECT', '*', 'FROM', 'foo']
```

### Extracting columns from query

```python
from sql_metadata import Parser

# get columns from query - for more examples see `tests/test_getting_columns.py`
Parser("SELECT test, id FROM foo, bar").columns
# ['test', 'id']

Parser("INSERT /* VoteHelper::addVote xxx */  INTO `page_vote` (article_id,user_id,`time`) VALUES ('442001','27574631','20180228130846')").columns
# ['article_id', 'user_id', 'time']

parser = Parser("SELECT a.* FROM product_a.users AS a JOIN product_b.users AS b ON a.ip_address = b.ip_address")

# note that aliases are auto-resolved
parser.columns
# ['product_a.*', 'product_a.users.ip_address', 'product_b.users.ip_address']

# note that you can also extract columns with their place in the query
# which will return dict with lists divided into select, where, order_by, join, insert and update
parser.columns_dict
# {'select': ['product_a.users.*'], 'join': ['product_a.users.ip_address', 'product_b.users.ip_address']}
```

### Extracting tables from query

```python
from sql_metadata import Parser

# get tables from query - for more examples see `tests/test_getting_tables.py`
Parser("SELECT a.* FROM product_a.users AS a JOIN product_b.users AS b ON a.ip_address = b.ip_address").tables
# ['product_a.users', 'product_b.users']

Parser("SELECT test, id FROM foo, bar").tables
# ['foo', 'bar']

# you can also extract aliases of the tables as a dictionary
parser = Parser("SELECT f.test FROM foo AS f")

# get table aliases
parser.tables_aliases
# {'f': 'foo'}

# note that aliases are auto-resolved for columns
parser.columns
# ["foo.test"]
```

### Extracting values from query
```python
from sql_metadata import Parser

parser = Parser(
    "INSERT /* VoteHelper::addVote xxx */  INTO `page_vote` (article_id,user_id,`time`) " 
    "VALUES ('442001','27574631','20180228130846')"
)
# extract values from query
parser.values
# ["442001", "27574631", "20180228130846"]

# extract a dictionary with column-value pairs
parser.values_dict
#{"article_id": "442001", "user_id": "27574631", "time": "20180228130846"}

# if column names are not set auto-add placeholders
parser = Parser(
    "INSERT IGNORE INTO `table` VALUES (9, 2.15, '123', '2017-01-01');"
)
parser.values
# [9, 2.15, "123", "2017-01-01"]

parser.values_dict
#{"column_1": 9, "column_2": 2.15, "column_3": "123", "column_4": "2017-01-01"}
```


### Extracting limit and offset
```python
from sql_metadata import Parser

Parser('SELECT foo_limit FROM bar_offset LIMIT 50 OFFSET 1000').limit_and_offset
# (50, 1000)

Parser('SELECT foo_limit FROM bar_offset limit 2000,50').limit_and_offset
# (50, 2000)
```

### Extracting with names

```python
from sql_metadata import Parser

parser = Parser(
    """
WITH
    database1.tableFromWith AS (SELECT aa.* FROM table3 as aa 
                                left join table4 on aa.col1=table4.col2),
    test as (select * from table3)
SELECT
  "xxxxx"
FROM
  database1.tableFromWith alias
LEFT JOIN database2.table2 ON ("tt"."ttt"."fff" = "xx"."xxx")
"""
)

# get names/ aliases of with statements
parser.with_names
# ["database1.tableFromWith", "test"]

# note that names of with statements do not appear in tables
parser.tables
# ["table3", "table4", "database2.table2"]
```

See `tests` file for more examples of a bit more complex queries.

### Queries normalization and comments extraction

```python
from sql_metadata import Parser
parser = Parser('SELECT /* Test */ foo FROM bar WHERE id in (1, 2, 56)')

# generalize query
parser.generalize
# 'SELECT foo FROM bar WHERE id in (XYZ)'

# remove comments
parser.remove_comments
# 'SELECT foo FROM bar WHERE id in (1, 2, 56)'

# extract comments
parser.comments
# ['/* Test */']
```

See `test/test_normalization.py` file for more examples of a bit more complex queries.

## Stargazers over time

[![Stargazers over time](https://starchart.cc/macbre/sql-metadata.svg)](https://starchart.cc/macbre/sql-metadata)
