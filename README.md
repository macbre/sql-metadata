# sql-metadata
Uses tokenized query returned by [`python-sqlparse`](https://github.com/andialbrecht/sqlparse) and generates query metadata. Extracts column names and tables used by the query.

### Usage

```python
>>> import sql_metadata

>>> sql_metadata.get_query_tokens("SELECT * FROM foo")
[<DML 'SELECT' at 0x7F14FFDEB808>, <Wildcard '*' at 0x7F14FFDEB940>, <Keyword 'FROM' at 0x7F14FFDEBBB0>, <Name 'foo' at 0x7F14FFDEB9A8>]

>>> sql_metadata.get_query_columns("SELECT test, id FROM foo, bar")
[u'test', u'id']

>>> sql_metadata.get_query_tables("SELECT test, id FROM foo, bar")
[u'foo', u'bar']
```

> See `test/test_query.py` file for more examples of a bit more complex queries.
