# sql-metadata

[![PyPI](https://img.shields.io/pypi/v/sql_metadata.svg)](https://pypi.python.org/pypi/sql_metadata)
[![Tests](https://github.com/macbre/sql-metadata/actions/workflows/python-ci.yml/badge.svg)](https://github.com/macbre/sql-metadata/actions/workflows/python-ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/macbre/sql-metadata/badge.svg?branch=master&1)](https://coveralls.io/github/macbre/sql-metadata?branch=master)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Maintenance](https://img.shields.io/badge/maintained%3F-yes-green.svg)](https://github.com/macbre/sql-metadata/graphs/commit-activity)
[![Downloads](https://pepy.tech/badge/sql-metadata/month)](https://pepy.tech/project/sql-metadata)

Uses [`sqlglot`](https://github.com/tobymao/sqlglot) to parse SQL queries and extract metadata.

**Extracts column names and tables** used by the query. 
Automatically conduct **column alias resolution**, **sub queries aliases resolution** as well as **tables aliases resolving**.

Provides also a helper for **normalization of SQL queries**.

Supported queries syntax:

* MySQL
* PostgreSQL
* Sqlite
* MSSQL
* [Apache Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML)

(note that listed backends can differ quite substantially but should work in regard of query types supported by `sql-metadata`)

You can test the capabilities of `sql-metadata` with an interactive demo: [https://sql-app.infocruncher.com/](https://sql-app.infocruncher.com/)

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
# ['product_a.users.*', 'product_a.users.ip_address', 'product_b.users.ip_address']

# note that you can also extract columns with their place in the query
# which will return dict with lists divided into select, where, order_by, group_by, join, insert and update
parser.columns_dict
# {'select': ['product_a.users.*'], 'join': ['product_a.users.ip_address', 'product_b.users.ip_address']}
```

### Extracting columns aliases from query

```python
from sql_metadata import Parser
parser = Parser("SELECT a, (b + c - u) as alias1, custome_func(d) alias2 from aa, bb order by alias1")

# note that columns list do not contain aliases of the columns
parser.columns
# ["a", "b", "c", "u", "d"]

# but you can still extract aliases names
parser.columns_aliases_names
# ["alias1", "alias2"]

# aliases are resolved to the columns which they refer to
parser.columns_aliases
# {"alias1": ["b", "c", "u"], "alias2": "d"}

# you can also extract aliases used by section of the query in which they are used
parser.columns_aliases_dict
# {"order_by": ["alias1"], "select": ["alias1", "alias2"]}

# the same applies to aliases used in queries section when you extract columns_dict
# here only the alias is used in order by but it's resolved to actual columns
assert parser.columns_dict == {'order_by': ['b', 'c', 'u'],
                               'select': ['a', 'b', 'c', 'u', 'd']}
```

### Extracting output column names

```python
from sql_metadata import Parser

# output_columns returns the ordered list of names that the SELECT would produce,
# preserving aliases (unlike `columns`, which resolves aliases back to real columns)
Parser("SELECT a, b AS c FROM t").output_columns
# ['a', 'c']

# works with function calls, window functions, computed aliases
Parser("""SELECT
    id,
    UPPER(email) AS email_upper,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY created_at) AS rn
FROM users""").output_columns
# ['id', 'email_upper', 'rn']

# SELECT * stays as '*'
Parser("SELECT * FROM t").output_columns
# ['*']

# non-SELECT queries return an empty list
Parser("CREATE TABLE t (id INT)").output_columns
# []
```

### Detecting query type

```python
from sql_metadata import Parser, QueryType

Parser("SELECT * FROM foo").query_type
# <QueryType.SELECT: 'SELECT'>

# QueryType is a str-enum, so it compares equal to both strings and enum values
Parser("INSERT INTO foo VALUES (1)").query_type == QueryType.INSERT   # True
Parser("INSERT INTO foo VALUES (1)").query_type == "INSERT"           # True

# REPLACE INTO is reported distinctly from INSERT
Parser("REPLACE INTO foo VALUES (1)").query_type
# <QueryType.REPLACE: 'REPLACE'>

# Supported types: SELECT, INSERT, REPLACE, UPDATE, DELETE,
# CREATE, ALTER, DROP, TRUNCATE, MERGE
```

### Handling invalid queries

```python
from sql_metadata import Parser, InvalidQueryDefinition

# structurally invalid SQL raises `InvalidQueryDefinition` (a subclass of
# `ValueError`, so existing `except ValueError` handlers keep working)
try:
    Parser("").query_type
except InvalidQueryDefinition as exc:
    print(exc)  # "Empty queries are not supported!"

try:
    Parser("THIS IS NOT SQL").query_type
except InvalidQueryDefinition as exc:
    print(exc)  # "Not supported query type!"
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

### Extracting values from insert query
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
    test as (SELECT * from table3)
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

# get definition of with queries
# (sqlglot normalises keyword casing and spacing when rendering the body SQL)
parser.with_queries
# {"database1.tableFromWith": "SELECT aa.* FROM table3 AS aa LEFT JOIN table4 ON aa.col1 = table4.col2",
#  "test": "SELECT * FROM table3"}

# note that names of with statements do not appear in tables
parser.tables
# ["table3", "table4", "database2.table2"]
```

### Extracting sub-queries

```python
from sql_metadata import Parser

parser = Parser(
"""
SELECT COUNT(1) FROM
(SELECT std.task_id FROM some_task_detail std WHERE std.STATUS = 1) a
JOIN (SELECT st.task_id FROM some_task st WHERE task_type_id = 80) b
ON a.task_id = b.task_id;
"""
)

# get sub-queries dictionary
# (sqlglot normalises keyword casing — implicit table aliases become explicit `AS`)
parser.subqueries
# {"a": "SELECT std.task_id FROM some_task_detail AS std WHERE std.STATUS = 1",
#  "b": "SELECT st.task_id FROM some_task AS st WHERE task_type_id = 80"}


# get names/ aliases of sub-queries / derived tables
parser.subqueries_names
# ["a", "b"]

# note that columns coming from sub-queries are resolved to real columns
parser.columns
#["some_task_detail.task_id", "some_task_detail.STATUS", "some_task.task_id", 
# "task_type_id"]

# same applies for columns_dict, note the join columns are resolved
parser.columns_dict
#{'join': ['some_task_detail.task_id', 'some_task.task_id'],
# 'select': ['some_task_detail.task_id', 'some_task.task_id'],
# 'where': ['some_task_detail.STATUS', 'task_type_id']}

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
parser.without_comments
# 'SELECT foo FROM bar WHERE id in (1, 2, 56)'

# extract comments
parser.comments
# ['/* Test */']
```

See `test/test_normalization.py` file for more examples of a bit more complex queries.

## Migrating from `sql_metadata` 1.x / 2.x

The `sql_metadata.compat` module (previously provided for v1 → v2 migration) has been **removed in v3**.  Port your code to the class-based `Parser` API shown in the examples above:

| Old v1 helper                 | v3 replacement                            |
|-------------------------------|-------------------------------------------|
| `generalize_sql(sql)`         | `Parser(sql).generalize`                  |
| `get_query_columns(sql)`      | `Parser(sql).columns`                     |
| `get_query_tables(sql)`       | `Parser(sql).tables`                      |
| `get_query_limit_and_offset(sql)` | `Parser(sql).limit_and_offset`        |
| `get_query_tokens(sql)`       | `Parser(sql).tokens`                      |
| `preprocess_query(sql)`       | `Parser(sql).query`                       |

For v2 → v3 users, the public `Parser` API is unchanged except:

* The parsing engine is now [sqlglot](https://github.com/tobymao/sqlglot), which may normalise the *casing* and *spacing* of rendered CTE/subquery bodies (see the `with_queries` / `subqueries` examples above).
* Malformed SQL now raises `InvalidQueryDefinition` (a `ValueError` subclass) instead of a plain `ValueError` — existing `except ValueError:` handlers continue to work.

## Authors and contributors

Created and maintained by [@macbre](https://github.com/macbre) with a great contributions from [@collerek](https://github.com/collerek) and the others.

* aborecki (https://github.com/aborecki)
* collerek (https://github.com/collerek)
* dylanhogg (https://github.com/dylanhogg)
* macbre (https://github.com/macbre)

## Stargazers over time

[![Stargazers over time](https://starchart.cc/macbre/sql-metadata.svg)](https://starchart.cc/macbre/sql-metadata)
