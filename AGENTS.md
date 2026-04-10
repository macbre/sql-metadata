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
- sqlglot library for SQL parsing and AST construction
- Poetry for dependency management
- pytest for testing
- ruff for linting and formatting

## Repository Structure

```
sql-metadata/
├── sql_metadata/                  # Main package
│   ├── parser.py                 # Public facade — Parser class
│   ├── ast_parser.py             # ASTParser — thin orchestrator, composes SqlCleaner + DialectParser
│   ├── sql_cleaner.py            # SqlCleaner — raw SQL preprocessing (no sqlglot dependency)
│   ├── dialect_parser.py         # DialectParser — dialect detection, parsing, quality validation
│   ├── column_extractor.py       # ColumnExtractor — single-pass DFS column/alias extraction
│   ├── table_extractor.py        # TableExtractor — table extraction with position sorting
│   ├── nested_resolver.py        # NestedResolver — CTE/subquery names, bodies, resolution
│   ├── query_type_extractor.py   # QueryTypeExtractor — query type detection
│   ├── comments.py               # Comment extraction/stripping (pure functions)
│   ├── keywords_lists.py         # QueryType enum
│   ├── utils.py                  # UniqueList, last_segment, shared helpers
│   ├── generalizator.py          # Query anonymisation
│   └── __init__.py               # Exports: Parser, QueryType
├── test/                          # Test suite (25 test files)
│   ├── test_with_statements.py
│   ├── test_getting_tables.py
│   ├── test_getting_columns.py
│   └── ...
├── ARCHITECTURE.md               # Detailed architecture docs with Mermaid diagrams
├── pyproject.toml                # Poetry configuration
├── Makefile                      # Common commands
└── README.md
```

## Architecture Overview

The v3 architecture uses sqlglot to build an AST, then walks it with specialised extractor classes composed by a thin `Parser` facade. See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed module deep dives, traced walkthroughs, and Mermaid diagrams.

### Pipeline

```
Raw SQL → SqlCleaner (preprocessing)
       → DialectParser (dialect detection, sqlglot.parse())
       → sqlglot AST (cached by ASTParser)
       → TableExtractor (tables, table aliases)
       → ColumnExtractor (columns, column aliases — single-pass DFS)
       → NestedResolver (CTE/subquery names + bodies, column resolution)
       → Final metadata (cached on Parser)
```

### Key Design Patterns

- **Composition over inheritance** — `Parser` composes `ASTParser`, `TableExtractor`, `ColumnExtractor`, `NestedResolver`, `QueryTypeExtractor`
- **Lazy evaluation with caching** — properties compute on first access, cache the result
- **Single-pass DFS** — `ColumnExtractor` walks AST in `arg_types` key order (mirrors SQL text order)
- **Multi-dialect retry** — `ASTParser` tries several sqlglot dialects, picks first non-degraded result
- **Graceful regex fallbacks** — degrades to regex when sqlglot parse fails

### Class Responsibilities

| Class | Owns | Does NOT own |
|-------|------|-------------|
| `Parser` | Facade, caching, regex fallbacks, value extraction | No extraction logic |
| `ASTParser` | Orchestration, lazy AST caching | No preprocessing, no parsing |
| `SqlCleaner` | Raw SQL preprocessing (REPLACE rewrite, comment strip, CTE normalisation) | No AST, no sqlglot |
| `DialectParser` | Dialect detection, sqlglot parsing, parse-quality validation | No preprocessing |
| `ColumnExtractor` | Column names, column aliases (during DFS walk) | CTE/subquery name extraction (standalone) |
| `TableExtractor` | Table names, table aliases, position sorting | Nothing else |
| `NestedResolver` | CTE/subquery names, CTE/subquery bodies, column resolution | Column extraction |
| `QueryTypeExtractor` | Query type detection | Nothing else |

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
make lint              # Run ruff check with auto-fix
poetry run ruff check --fix sql_metadata
```

### Code Formatting
```bash
make format            # Run ruff formatter
poetry run ruff format .
```

### Type Checking
```bash
poetry run mypy sql_metadata
```

### Coverage
```bash
make coverage          # Run tests with coverage report
poetry run pytest -vv --cov=sql_metadata --cov-report=term-missing
```

**Important:** The project has a 100% test coverage requirement (`fail_under = 100` in pyproject.toml).

### Verification after changes
After making code changes, always run all three checks:
```bash
poetry run pytest -vv --cov=sql_metadata --cov-report=term-missing  # tests + coverage
poetry run mypy sql_metadata                                         # type checking
poetry run ruff check sql_metadata                                   # linting
```

## Code Quality Standards

### Ruff Configuration (pyproject.toml)
- Max line length: 88
- Max complexity: 8 (C901 error for complexity > 8)
- Enabled rule sets: E, F, W (pycodestyle/pyflakes), C90 (mccabe), I (isort)
- Exceptions: Use `# noqa: C901` for complex but necessary functions

## Error Handling Patterns

### Malformed SQL Detection

The codebase has established patterns for handling malformed SQL:

1. **Detect the malformed pattern early**
2. **Raise `ValueError("This query is wrong")`** — This is the standard error message
3. **Use pytest.raises in tests:**
```python
parser = Parser(malformed_query)
with pytest.raises(ValueError, match="This query is wrong"):
    parser.tables
```

## Testing Patterns

### Test Organization
Tests are organized by feature/SQL clause:
- `test_with_statements.py` — WITH clause (CTEs)
- `test_getting_tables.py` — Table extraction
- `test_getting_columns.py` — Column extraction
- `test_query_type.py` — Query type detection
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

### Test Comments
Reference issues in test comments:
```python
def test_issue_fix():
    # Test for issue #556 - malformed WITH query causes infinite loop
    # https://github.com/macbre/sql-metadata/issues/556
```

## Git Workflow

### Commit Message Format
Following the established pattern:

```
Brief description of change

Resolves #issue-number.

Co-Authored-By: Claude <noreply@anthropic.com>
```

### Branch Naming
- Feature: `feature/description`
- Bug fix: `fix/description`

## Dependencies

### Production
- **sqlglot** (^30.0.3): SQL parsing and AST construction

### Development
- **pytest** (^9.0.2): Testing framework
- **pytest-cov** (^7.1.0): Coverage reporting
- **ruff** (^0.11): Linting and formatting
- **coverage** (^7.13): Coverage measurement

## Version Information

- **Current Version:** 2.20.0
- **Python Support:** ^3.10
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

### 2. Error Messages
Use consistent error messages:
- `"This query is wrong"` — for malformed SQL
- `"Empty queries are not supported!"` — for empty input
- Keep messages simple and consistent with existing patterns

### 3. Prefer sqlglot over manual parsing
Always use sqlglot AST features (node types, `find_all`, `arg_types` traversal) rather than regex or manual string parsing when possible.

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

## Debugging Tips

### Inspecting the AST
```python
from sql_metadata import Parser
p = Parser("SELECT a FROM t")
print(p._ast_parser.ast)        # sqlglot AST tree
print(repr(p._ast_parser.ast))  # Detailed node repr
```

### Running Single Test with Timeout
```bash
timeout 5 poetry run pytest test/test_file.py::test_name -vv
```

## Last Updated
2026-03-31 — Rewritten for v3 architecture (sqlglot-based, class extractors)
