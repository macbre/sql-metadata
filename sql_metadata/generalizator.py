"""Produce a generalised (anonymised) version of a SQL query.

Replaces string literals with ``X``, numbers with ``N``, and
multi-value ``IN (...)`` / ``VALUES (...)`` lists with ``(XYZ)`` so
that structurally identical queries can be grouped for analysis
(e.g. slow-query log aggregation).  Based on MediaWiki's
``DatabaseBase::generalizeSQL``.
"""

import re

from sql_metadata._comments import strip_comments


class Generalizator:
    """Produce a generalised form of a SQL query.

    Strips comments, removes string literals and numeric values, and
    collapses repeated ``LIKE`` / ``IN`` / ``VALUES`` clauses.  Designed
    for grouping structurally identical queries in monitoring and logging
    pipelines.

    Used by :attr:`Parser.generalize`, which delegates to
    :attr:`Generalizator.generalize`.

    :param sql: Raw SQL query string to generalise.
    :type sql: str
    """

    def __init__(self, sql: str = ""):
        """Initialise with the raw SQL string.

        :param sql: SQL query to generalise.
        :type sql: str
        """
        self._raw_query = sql

    # SQL queries normalization (#16)
    @staticmethod
    def _normalize_likes(sql: str) -> str:
        """Normalise and collapse repeated ``LIKE`` clauses.

        Strips ``%`` wildcards, replaces ``LIKE '...'`` with ``LIKE X``,
        and collapses consecutive ``or/and ... LIKE X`` clauses into a
        single instance with ``...`` suffix.

        :param sql: SQL string with LIKE clauses.
        :type sql: str
        :returns: SQL with LIKE clauses normalised.
        :rtype: str
        """
        sql = sql.replace("%", "")

        # LIKE '%bot'
        sql = re.sub(r"LIKE '[^\']+'", "LIKE X", sql)

        # or all_groups LIKE X or all_groups LIKE X
        matches = re.finditer(r"(or|and) [^\s]+ LIKE X", sql, flags=re.IGNORECASE)
        matches = [match.group(0) for match in matches] if matches else None

        if matches:
            for match in set(matches):
                sql = re.sub(
                    r"(\s?" + re.escape(match) + ")+", " " + match + " ...", sql
                )

        return sql

    @property
    def without_comments(self) -> str:
        """Return the SQL with all comments removed.

        Delegates to :func:`strip_comments` from ``_comments.py``.

        :returns: Comment-free SQL string.
        :rtype: str
        """
        return strip_comments(self._raw_query)

    @property
    def generalize(self) -> str:
        """Return a generalised version of the SQL query.

        Applies the following transformations in order:

        1. Strip comments.
        2. Remove double-quotes.
        3. Collapse multiple spaces.
        4. Normalise ``LIKE`` clauses.
        5. Replace escaped characters.
        6. Replace string literals with ``X``.
        7. Collapse whitespace to single spaces.
        8. Replace numbers with ``N``.
        9. Collapse ``IN (...)`` / ``VALUES (...)`` lists to ``(XYZ)``.

        :returns: Generalised SQL string, or ``""`` for empty input.
        :rtype: str
        """
        if self._raw_query == "":
            return ""

        # MW comments
        # e.g. /* CategoryDataService::getMostVisited N.N.N.N */
        sql = self.without_comments
        sql = sql.replace('"', "")

        # multiple spaces
        sql = re.sub(r"\s{2,}", " ", sql)

        # handle LIKE statements
        sql = self._normalize_likes(sql)

        sql = re.sub(r"\\\\", "", sql)
        sql = re.sub(r"\\'", "", sql)
        sql = re.sub(r'\\"', "", sql)
        sql = re.sub(r"'[^\']*'", "X", sql)
        sql = re.sub(r'"[^\"]*"', "X", sql)

        # All newlines, tabs, etc replaced by single space
        sql = re.sub(r"\s+", " ", sql)

        # All numbers => N
        sql = re.sub(r"-?[0-9]+", "N", sql)

        # WHERE foo IN ('880987','882618','708228','522330')
        sql = re.sub(
            r" (IN|VALUES)\s*\([^,]+,[^)]+\)", " \\1 (XYZ)", sql, flags=re.IGNORECASE
        )

        return sql.strip()
