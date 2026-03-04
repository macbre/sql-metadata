# Agent Notes for sql-metadata Repository

This file contains important information about the sql-metadata repository for AI agents and developers working on the codebase.

## Project Overview

**sql-metadata** is a Python library that parses SQL queries and extracts metadata such as:
- Tables referenced in queries
- Columns used
- Query types (SELECT, INSERT, UPDATE, etc.)
- WITH clause (CTE) definitions
- Subqueries and aliases

**Technology Stack:**
- Python 3.10+
- sqlparse library for tokenization
- Poetry for dependency management
- pytest for testing
- flake8 and pylint for linting

## Repository Structure

```
sql-metadata/
├── sql_metadata/          # Main package
│   ├── parser.py         # Core Parser class
│   ├── token.py          # SQLToken and EmptyToken classes
│   ├── keywords_lists.py # SQL keyword definitions
│   └── __init__.py
├── test/                 # Test suite
│   ├── test_with_statements.py
│   ├── test_getting_tables.py
│   ├── test_getting_columns.py
│   └── ... (30+ test files)
├── pyproject.toml        # Poetry configuration
├── Makefile             # Common commands
├── .flake8              # Flake8 configuration
└── README.md
```

## Development Workflow

### Setup
```bash
poetry install          # Install dependencies
```

### Testing
```bash
make test              # Run all tests with pytest
poetry run pytest -vv  # Verbose test output
poetry run pytest -x   # Stop on first failure
poetry run pytest test/test_with_statements.py::test_name  # Run specific test
```

### Linting
```bash
make lint              # Run flake8 and pylint
poetry run flake8 sql_metadata/
poetry run pylint sql_metadata/
```

### Code Formatting
```bash
make format            # Run black formatter
```

### Coverage
```bash
make coverage          # Run tests with coverage report
```

**Important:** The project has a 100% test coverage requirement (`fail_under = 100` in pyproject.toml).

## Code Quality Standards

### Flake8 Configuration (.flake8)
- Max line length: Not explicitly set (defaults apply)
- Max complexity: 8 (C901 error for complexity > 8)
- Exceptions: Use `# noqa: C901` for complex but necessary functions

### Complexity Suppression Pattern
When a function legitimately needs higher complexity, suppress the warning:
```python
@property
def complex_method(self) -> Type:  # noqa: C901
    """Method with necessary complexity"""
```

Examples in codebase:
- `parser.py:134`: `tokens` property
- `parser.py:450`: `with_names` property
- `parser.py:822`: `_resolve_nested_query` method

### Pylint
The Parser class has `# pylint: disable=R0902` to suppress "too many instance attributes" warnings.

## Parser Architecture

### Core Class: `Parser`
Located in `sql_metadata/parser.py`

The Parser class uses sqlparse to tokenize SQL and then processes tokens to extract metadata.

**Key Properties (lazy evaluation):**
- `tokens` - Tokenized SQL
- `tables` - Tables referenced in query
- `columns` - Columns referenced
- `with_names` - CTE (Common Table Expression) names
- `with_queries` - CTE definitions
- `query_type` - Type of SQL query
- `subqueries` - Subquery definitions

**Important Pattern:** Most properties cache their results:
```python
@property
def example(self):
    if self._example is not None:
        return self._example
    # ... computation ...
    self._example = result
    return self._example
```

### Token Processing

The parser processes `SQLToken` objects which have properties like:
- `value` - The token text
- `normalized` - Uppercased token value
- `next_token` - Next token in sequence
- `previous_token` - Previous token
- `next_token_not_comment` - Next non-comment token
- `is_as_keyword` - Boolean flag
- `is_with_query_end` - Boolean flag for WITH clause boundaries
- `token_type` - Type classification

### WITH Statement Parsing

Located in `parser.py:450` (`with_names` property)

**Key Logic:**
1. Iterates through tokens looking for "WITH" keywords
2. Enters a while loop that stays in WITH block until finding ending keywords
3. Processes each CTE by finding "AS" keywords and extracting names
4. Advances through tokens until finding `is_with_query_end`
5. Checks if at end of WITH block using `WITH_ENDING_KEYWORDS`

**WITH_ENDING_KEYWORDS** (from `keywords_lists.py`):
- UPDATE
- SELECT
- DELETE
- REPLACE
- INSERT

**Common Pitfall:** Malformed SQL with consecutive AS keywords (e.g., `WITH a AS (...) AS b`) can cause infinite loops if not properly detected and handled.

**Solution Pattern:** After processing a WITH clause, always check if the next token is another AS keyword (which indicates malformed SQL) and raise `ValueError("This query is wrong")`.

## Error Handling Patterns

### Malformed SQL Detection

The codebase has established patterns for handling malformed SQL:

1. **Detect the malformed pattern early**
2. **Raise `ValueError("This query is wrong")`** - This is the standard error message
3. **Use pytest.raises in tests:**
```python
parser = Parser(malformed_query)
with pytest.raises(ValueError, match="This query is wrong"):
    parser.tables
```

Examples:
- `test_with_statements.py:500-528`: Tests for malformed WITH queries
- `parser.py:679`: Detection in `_handle_with_name_save`

### Infinite Loop Prevention

When processing tokens in loops:
1. Always ensure the token advances in each iteration
2. Check for malformed patterns before looping back
3. Have clear exit conditions

Pattern:
```python
while condition and token.next_token:
    if some_pattern:
        # ... process ...
        if exit_condition:
            break
        else:
            # Always advance token to prevent infinite loop
            token = token.next_token
    else:
        token = token.next_token
```

## Testing Patterns

### Test Organization
Tests are organized by feature/SQL clause:
- `test_with_statements.py` - WITH clause (CTEs)
- `test_getting_tables.py` - Table extraction
- `test_getting_columns.py` - Column extraction
- `test_query_type.py` - Query type detection
- Database-specific: `test_mssql_server.py`, `test_postgress.py`, `test_hive.py`, etc.

### Test Naming Convention
```python
def test_descriptive_name():
    """Optional docstring explaining the test"""
    query = """SQL query here"""
    parser = Parser(query)
    assert parser.tables == ["expected", "tables"]
```

### Testing Malformed SQL
```python
def test_malformed_case():
    # Comment explaining what's being tested and why
    # Include issue reference if applicable: # https://github.com/macbre/sql-metadata/issues/XXX
    query = """Malformed SQL"""
    parser = Parser(query)
    with pytest.raises(ValueError, match="This query is wrong"):
        parser.tables
```

### Test Coverage
- Every new feature needs tests
- Every bug fix needs a test that would have caught the bug
- Coverage must remain at 100%

## Git Workflow

### Commit Message Format
Following the established pattern:

```
Brief description of change 

Resolves #issue-number.

More detailed explanation of what was wrong and why.

The issue was: [explain the problem]

This fix:
- Bullet point 1
- Bullet point 2
- Bullet point 3

Co-Authored-By: Claude <noreply@anthropic.com>
```

### Branch Naming
- Feature: `feature/description`
- Bug fix: `fix/description`
- Example: `fix/parser-tables-hangs`

### Recent Commits (as of 2026-03-04)
```
1fbfee4 Drop Python 3.9 support (#604)
d0e6fc6 Parser.columns drops column named 'source' when it is the last column in a SELECT statement (#603)
```

## Common Issues and Solutions

### Issue: Parser Hangs/Infinite Loop

**Symptoms:** Parser never returns when calling `.tables` or other properties

**Common Causes:**
1. Token not advancing in a while loop
2. Malformed SQL not detected early enough
3. Missing exit condition in nested loops

**Solution Checklist:**
- [ ] Ensure token advances in all loop branches
- [ ] Check for malformed SQL patterns and raise ValueError
- [ ] Verify exit conditions are reachable
- [ ] Add timeout test to verify fix

### Issue: Flake8 Complexity Warning (C901)

**When it happens:** Function exceeds complexity threshold of 8

**Solutions:**
1. Refactor to reduce complexity (preferred)
2. Use `# noqa: C901` if complexity is necessary (see examples in codebase)

### Issue: Tests Pass Locally but Coverage Fails

**Cause:** Missing test coverage for new code paths

**Solution:**
```bash
poetry run pytest -vv --cov=sql_metadata --cov-report=term-missing
```
This shows which lines are not covered.

## Important Files

### `sql_metadata/parser.py`
- **Lines 134-200:** Token processing and initialization
- **Lines 450-482:** WITH clause parsing (with_names property)
- **Lines 484-580:** WITH queries extraction
- **Lines 669-700:** `_handle_with_name_save` helper method
- **Lines 822+:** Nested query resolution

### `sql_metadata/keywords_lists.py`
Defines SQL keyword sets:
- `WITH_ENDING_KEYWORDS` (line 40)
- `SUBQUERY_PRECEDING_KEYWORDS`
- `TABLE_ADJUSTMENT_KEYWORDS`
- `KEYWORDS_BEFORE_COLUMNS`
- `SUPPORTED_QUERY_TYPES`

### `test/test_with_statements.py`
Comprehensive tests for WITH clause parsing:
- Valid multi-CTE queries
- CTEs with column definitions
- Nested WITH statements
- Malformed SQL detection (lines 500-540)

## Debugging Tips

### Running Single Test with Timeout
```bash
timeout 5 poetry run pytest test/test_file.py::test_name -vv
```

### Testing Infinite Loop Fix
```bash
timeout 3 poetry run python -c "from sql_metadata import Parser; Parser(query).tables"
```
If it times out, there's still an infinite loop.

### Inspecting Token Flow
Add debug prints in parser.py:
```python
print(f"Token: {token.value}, Next: {token.next_token.value if token.next_token else None}")
```

## Dependencies

### Production
- **sqlparse** (>=0.4.1, <0.6.0): SQL tokenization

### Development
- **pytest** (^8.4.2): Testing framework
- **pytest-cov** (^7.0.0): Coverage reporting
- **black** (^25.11): Code formatting
- **flake8** (^7.3.0): Linting
- **pylint** (^3.3.9): Advanced linting
- **coverage** (^7.10): Coverage measurement

## Version Information

- **Current Version:** 2.19.0
- **Python Support:** ^3.10 (Python 3.9 support dropped in #604)
- **License:** MIT
- **Homepage:** https://github.com/macbre/sql-metadata

## Known Patterns to Follow

### 1. Property Caching
Always cache property results:
```python
@property
def my_property(self):
    if self._my_property is not None:
        return self._my_property
    self._my_property = self._compute_property()
    return self._my_property
```

### 2. Token Advancement Safety
In loops, ensure every branch advances:
```python
while condition:
    if pattern_match:
        # ... process ...
        if should_exit:
            flag = False
        else:
            token = token.next_token  # MUST advance
    else:
        token = token.next_token  # MUST advance
```

### 3. Error Messages
Use consistent error messages:
- `"This query is wrong"` - for malformed SQL
- Keep messages simple and consistent with existing patterns

### 4. Test Comments
Reference issues in test comments:
```python
def test_issue_fix():
    # Test for issue #556 - malformed WITH query causes infinite loop
    # https://github.com/macbre/sql-metadata/issues/556
```

## Quick Reference Commands

```bash
# Setup
poetry install

# Test
make test                                    # All tests
poetry run pytest test/test_with_statements.py -vv  # Specific file
poetry run pytest -x                         # Stop on first failure
poetry run pytest -k "test_name"            # Run by name pattern

# Quality
make lint                                    # Lint check
make format                                  # Format code
make coverage                               # Coverage report

# Debug
poetry run python -c "from sql_metadata import Parser; print(Parser('SELECT * FROM t').tables)"
```

## Notes for Future Work

### Potential Improvements
1. Consider refactoring `with_names` property to reduce complexity below 8
2. Add more detailed error messages for different types of malformed SQL
3. Consider extracting token advancement logic into helper methods

### Technical Debt
- Poetry dev-dependencies section is deprecated (migrate to poetry.group.dev.dependencies)
- Consider adding type hints more comprehensively
- Some test files could be consolidated

## Last Updated
2026-03-04 - Initial creation after fixing issue #556 (infinite loop in WITH statement parsing)
