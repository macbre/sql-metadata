"""Regression tests for malformed input that used to crash the parser.

sqlglot in best-effort (WARN) mode can build a partial AST and then raise while
assembling it, e.g. an ``AttributeError`` on a node whose key is ``None`` for a
few-character string like ``"{ ="``. That escaped DialectParser and every public
accessor crashed with a raw ``AttributeError`` instead of reporting an invalid
query.
"""

import pytest

from sql_metadata import InvalidQueryDefinition, Parser


@pytest.mark.parametrize("query", ["{ =", "SELECT { =", "x { ="])
def test_bracket_equals_does_not_crash(query):
    # query_type / tables validate the AST, so they surface the invalid query
    # as InvalidQueryDefinition rather than an AttributeError.
    with pytest.raises(InvalidQueryDefinition):
        Parser(query).query_type

    with pytest.raises(InvalidQueryDefinition):
        Parser(query).tables

    # The best-effort accessors must simply come back empty, not crash.
    assert Parser(query).columns == []
    assert Parser(query).columns_dict == {}
