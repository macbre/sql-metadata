"""Custom exceptions for the sql-metadata package."""


class InvalidQueryDefinition(ValueError):
    """Raised when the SQL query is structurally invalid or unsupported."""
