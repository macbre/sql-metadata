"""SQL dialect detection and custom sqlglot dialect classes.

Provides heuristic-based dialect detection for SQL queries and custom
dialect classes for MSSQL hash-variables and TSQL bracket notation.
"""

from sqlglot import Dialect
from sqlglot.dialects.tsql import TSQL
from sqlglot.tokens import Tokenizer


class HashVarDialect(Dialect):
    """Custom sqlglot dialect that treats ``#WORD`` as identifiers.

    MSSQL uses ``#`` to prefix temporary table names (e.g. ``#temp``)
    and some template engines use ``#VAR#`` placeholders.  The default
    sqlglot tokenizer treats ``#`` as an unknown single-character token;
    this dialect moves it into ``VAR_SINGLE_TOKENS`` so it becomes part
    of a ``VAR`` token instead.

    Used by :func:`detect_dialects` when hash-variables are detected
    in the SQL.
    """

    class Tokenizer(Tokenizer):
        """Tokenizer subclass that includes ``#`` in variable tokens."""

        SINGLE_TOKENS = {**Tokenizer.SINGLE_TOKENS}
        SINGLE_TOKENS.pop("#", None)
        VAR_SINGLE_TOKENS = {*Tokenizer.VAR_SINGLE_TOKENS, "#"}


class BracketedTableDialect(TSQL):
    """TSQL dialect for queries containing ``[bracketed]`` identifiers.

    sqlglot's TSQL dialect correctly interprets square-bracket quoting,
    which the default dialect does not.  This thin subclass exists so that
    :func:`detect_dialects` can return a concrete class that
    ``TableExtractor`` can later ``isinstance``-check to enable
    bracket-preserving table name construction.
    """


def detect_dialects(sql: str) -> list:
    """Choose an ordered list of sqlglot dialects to try for *sql*.

    Inspects the SQL for dialect-specific syntax and returns a list
    of dialect identifiers (``None`` = default, ``"mysql"``, or a
    custom :class:`Dialect` subclass) to try in order.  The first
    dialect whose result passes degradation checks wins.

    Heuristics:

    * ``#WORD`` → :class:`HashVarDialect` (MSSQL temp tables).
    * Back-ticks → ``"mysql"``.
    * Square brackets or ``TOP`` → :class:`BracketedTableDialect`.
    * ``UNIQUE`` → try default, MySQL, Oracle.
    * ``LATERAL VIEW`` → ``"spark"`` (Hive).

    :param sql: Cleaned SQL string.
    :type sql: str
    :returns: Ordered list of dialects to attempt.
    :rtype: list
    """
    from sql_metadata.comments import _has_hash_variables

    upper = sql.upper()
    # #WORD variables (MSSQL) — use custom dialect that treats # as identifier
    if _has_hash_variables(sql):
        return [HashVarDialect, None, "mysql"]
    if "`" in sql:
        return ["mysql", None]
    if "[" in sql or " TOP " in upper:
        return [BracketedTableDialect, None, "mysql"]
    if " UNIQUE " in upper:
        return [None, "mysql", "oracle"]
    if "LATERAL VIEW" in upper:
        return ["spark", None, "mysql"]
    return [None, "mysql"]
