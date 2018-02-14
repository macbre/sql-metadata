# sql-metadata

[![PyPI](https://img.shields.io/pypi/v/sql_metadata.svg)](https://pypi.python.org/pypi/sql_metadata)
[![Build Status](https://travis-ci.org/macbre/sql-metadata.svg?branch=master)](https://travis-ci.org/macbre/sql-metadata)

Uses tokenized query returned by [`python-sqlparse`](https://github.com/andialbrecht/sqlparse) and generates query metadata. Extracts column names and tables used by the query.

### Usage

```
pip install sql_metadata
```

```python
>>> import sql_metadata

>>> sql_metadata.get_query_tokens("SELECT * FROM foo")
[<DML 'SELECT' at 0x7F14FFDEB808>, <Wildcard '*' at 0x7F14FFDEB940>, <Keyword 'FROM' at 0x7F14FFDEBBB0>, <Name 'foo' at 0x7F14FFDEB9A8>]

>>> sql_metadata.get_query_columns("SELECT test, id FROM foo, bar")
[u'test', u'id']

>>> sql_metadata.get_query_tables("SELECT test, id FROM foo, bar")
[u'foo', u'bar']

>>> sql_metadata.get_query_limit_and_offset('SELECT foo_limit FROM bar_offset LIMIT 50 OFFSET 1000')
(50, 1000)

>>> sql_metadata.get_query_limit_and_offset('SELECT foo_limit FROM bar_offset limit 2000,50')
(50, 2000)
```

> See `test/test_query.py` file for more examples of a bit more complex queries.
