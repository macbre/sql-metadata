# Architecture

sql-metadata v3 is a Python library that parses SQL queries and extracts metadata (tables, columns, aliases, CTEs, subqueries, etc.). It delegates SQL parsing to [sqlglot](https://github.com/tobymao/sqlglot) for AST construction, then walks the resulting tree with specialised extractors.

## Module Map

| Module | Role | Key Class/Function |
|--------|------|--------------------|
| [`parser.py`](sql_metadata/parser.py) | Public facade — composes all extractors via lazy properties | `Parser` |
| [`ast_parser.py`](sql_metadata/ast_parser.py) | Thin orchestrator — composes SqlCleaner + DialectParser, caches AST | `ASTParser` |
| [`sql_cleaner.py`](sql_metadata/sql_cleaner.py) | Raw SQL preprocessing (no sqlglot dependency) | `SqlCleaner`, `CleanResult` |
| [`dialect_parser.py`](sql_metadata/dialect_parser.py) | Dialect detection, sqlglot parsing, parse-quality validation | `DialectParser`, `HashVarDialect`, `BracketedTableDialect` |
| [`column_extractor.py`](sql_metadata/column_extractor.py) | Single-pass DFS column/alias extraction | `ColumnExtractor` |
| [`table_extractor.py`](sql_metadata/table_extractor.py) | Table extraction with position-based sorting | `TableExtractor` |
| [`nested_resolver.py`](sql_metadata/nested_resolver.py) | CTE/subquery name and body extraction, nested column resolution | `NestedResolver` |
| [`query_type_extractor.py`](sql_metadata/query_type_extractor.py) | Query type detection from AST root node | `QueryTypeExtractor` |
| [`comments.py`](sql_metadata/comments.py) | Comment extraction/stripping via tokenizer gaps | `extract_comments`, `strip_comments` |
| [`keywords_lists.py`](sql_metadata/keywords_lists.py) | `QueryType` enum | — |
| [`utils.py`](sql_metadata/utils.py) | `UniqueList` (deduplicating list), `last_segment`, `DOT_PLACEHOLDER` | — |
| [`exceptions.py`](sql_metadata/exceptions.py) | Custom exception hierarchy | `InvalidQueryDefinition` |
| [`generalizator.py`](sql_metadata/generalizator.py) | Query anonymisation for log aggregation | `Generalizator` |

---

## High-Level Pipeline

```mermaid
flowchart TB
    SQL["Raw SQL string"]

    subgraph AST_CONSTRUCTION["ASTParser (ast_parser.py)"]
        direction TB
        PP["SqlCleaner\n(sql_cleaner.py)"]
        DP["DialectParser\n(dialect_parser.py)"]
        PP --> DP
    end

    SQL --> AST_CONSTRUCTION
    AST_CONSTRUCTION --> AST["sqlglot AST"]

    subgraph EXTRACTION["Parallel Extractors"]
        direction TB
        TE["TableExtractor\n(table_extractor.py)"]
        CE["ColumnExtractor\n(column_extractor.py)"]
        QT["QueryTypeExtractor\n(query_type_extractor.py)"]
    end

    AST --> EXTRACTION

    TE --> TA["tables, tables_aliases"]
    CE --> COLS["columns, aliases"]
    QT --> QTR["query_type"]

    TA --> NR
    COLS --> NR

    subgraph RESOLVE["NestedResolver (nested_resolver.py)"]
        direction TB
        NR["Resolve subquery.column\nreferences"]
        NE["Extract CTE/subquery\nnames and bodies"]
    end

    RESOLVE --> FINAL["Final metadata\n(cached on Parser)"]

    COM["comments.py"] -.-> AST_CONSTRUCTION
    COM -.-> FINAL
```

The `Parser` class ([`parser.py`](sql_metadata/parser.py)) is a thin facade that orchestrates these components through lazy cached properties. No extraction work happens until a property like `.columns` or `.tables` is first accessed.

---

## Module Deep Dives

### Parser — the facade

**File:** [`parser.py`](sql_metadata/parser.py) | **Class:** `Parser`

The constructor (`__init__`) stores the raw SQL and initialises ~20 cache fields to `None`. It creates an `ASTParser` instance (lazy — no parsing yet) and defers everything else.

**Composition:**

```mermaid
flowchart LR
    P["Parser"]
    P --> AP["ASTParser\n(self._ast_parser)"]
    P --> TE["TableExtractor\n(created per .tables call)"]
    P --> CE["ColumnExtractor\n(via extract_all())"]
    P --> NR["NestedResolver\n(self._resolver, lazy)"]
    P --> QTE["QueryTypeExtractor\n(via extract_query_type())"]
```

**Public properties:**

| Property | Returns | Triggers |
|----------|---------|----------|
| `query` | Preprocessed SQL (normalised quoting) | — |
| `query_type` | `QueryType` enum | `QueryTypeExtractor(ast, raw_query).extract()` |
| `tokens` | `List[str]` of token strings | sqlglot tokenizer |
| `columns` | Column names | AST parse → TableExtractor → `ColumnExtractor.extract()` → NestedResolver |
| `columns_dict` | Columns by clause section | `.columns` |
| `columns_aliases` | `{alias: target_column}` | `.columns` |
| `columns_aliases_names` | List of alias names | `.columns` |
| `columns_aliases_dict` | Aliases by clause section | `.columns` |
| `tables` | Table names | AST parse → TableExtractor |
| `tables_aliases` | `{alias: real_table}` | AST parse → TableExtractor |
| `with_names` | CTE names | AST parse → NestedResolver |
| `with_queries` | `{cte_name: body_sql}` | NestedResolver |
| `subqueries` | `{subquery_name: body_sql}` | NestedResolver |
| `subqueries_names` | Subquery aliases (innermost first) | AST parse → NestedResolver |
| `limit_and_offset` | `(limit, offset)` tuple | AST parse (regex fallback) |
| `values` | Literal values from INSERT | AST parse |
| `values_dict` | `{column: value}` pairs | `.values` + `.columns` |
| `comments` | Comment strings | sqlglot tokenizer |
| `without_comments` | SQL sans comments | sqlglot tokenizer |
| `generalize` | Anonymised SQL | Generalizator |

**Caching pattern** — every property checks its cache field first:

```python
@property
def tables(self) -> List[str]:
    if self._tables is not None:
        return self._tables
    # ... compute and cache ...
    self._tables = result
    return self._tables
```

**Regex fallbacks** — when `sqlglot.parse()` fails (raises `InvalidQueryDefinition`), the parser falls back to regex extraction for columns (`_extract_columns_regex`) and LIMIT/OFFSET (`_extract_limit_regex`) rather than propagating the error. `InvalidQueryDefinition` is a `ValueError` subclass defined in [`exceptions.py`](sql_metadata/exceptions.py) — catching `ValueError` still works for external callers.

---

### ASTParser — Orchestrator

**File:** [`ast_parser.py`](sql_metadata/ast_parser.py) | **Class:** `ASTParser`

Thin orchestrator that composes `SqlCleaner` and `DialectParser`. Instantiated once per `Parser` — actual parsing is deferred until `.ast` is first accessed. Exposes `.ast`, `.dialect`, `.is_replace`, and `.cte_name_map` properties.

---

### SqlCleaner — Raw SQL Preprocessing

**File:** [`sql_cleaner.py`](sql_metadata/sql_cleaner.py) | **Class:** `SqlCleaner`

Pure string transformations with no sqlglot dependency. `SqlCleaner.clean(sql)` returns a `CleanResult` namedtuple with the cleaned SQL, `is_replace` flag, and CTE name map.

#### Preprocessing pipeline

```mermaid
flowchart LR
    A["1. REPLACE INTO\n→ INSERT INTO"] --> B["2. SELECT...INTO\nvars stripped"]
    B --> C["3. Strip\ncomments"]
    C --> D["4. Normalise\nqualified CTE names"]
    D --> E["5. Strip DB2\nisolation clauses"]
    E --> F["6. Strip outer\nparentheses"]
```

| Step | Why | Example |
|------|-----|---------|
| REPLACE INTO rewrite | sqlglot parses `REPLACE INTO` as opaque `Command` | `REPLACE INTO t` → `INSERT INTO t` (flag set) |
| SELECT...INTO strip | Prevents sqlglot from treating variables as tables | `SELECT x INTO @v FROM t` → `SELECT x FROM t` |
| Comment stripping | Uses `strip_comments_for_parsing()` from `comments.py` | `SELECT /* hi */ 1` → `SELECT 1` |
| CTE name normalisation | sqlglot can't parse `WITH db.name AS (...)` | `db.cte` → `db__DOT__cte` (reverse map stored) |
| DB2 isolation clauses | Removes trailing `WITH UR/CS/RS/RR` | `SELECT 1 WITH UR` → `SELECT 1` |
| Outer paren stripping | sqlglot can't parse `((UPDATE ...))` | `((UPDATE t SET x=1))` → `UPDATE t SET x=1` |

---

### DialectParser — Dialect Detection and Parsing

**File:** [`dialect_parser.py`](sql_metadata/dialect_parser.py) | **Class:** `DialectParser`

Combines dialect heuristics, `sqlglot.parse()` calls, and parse-quality validation. `DialectParser().parse(clean_sql)` returns `(ast, dialect)`.

**Custom dialects (defined in same file):**

- `HashVarDialect` — treats `#` as part of identifiers for MSSQL temp tables (`#temp`) and template variables (`#VAR#`)
- `BracketedTableDialect` — TSQL subclass for `[bracket]` quoting; also signals `TableExtractor` to preserve brackets in output

#### Dialect detection

`_detect_dialects(sql)` inspects the SQL for syntax hints and returns an ordered list of dialects to try:

```mermaid
flowchart TD
    SQL["Cleaned SQL"]
    SQL --> H{"#WORD\nvariables?"}
    H -->|Yes| HD["[HashVarDialect, None, mysql]"]
    H -->|No| BT{"Backticks?"}
    BT -->|Yes| MY["[mysql, None]"]
    BT -->|No| BR{"Brackets\nor TOP?"}
    BR -->|Yes| BD["[BracketedTableDialect, None, mysql]"]
    BR -->|No| UN{"UNIQUE?"}
    UN -->|Yes| UO["[None, mysql, oracle]"]
    UN -->|No| LV{"LATERAL VIEW?"}
    LV -->|Yes| SP["[spark, None, mysql]"]
    LV -->|No| DF["[None, mysql]"]
```

#### Multi-dialect retry

`_try_dialects` iterates through the dialect list. For each dialect:

1. Parse with `sqlglot.parse()` (warnings suppressed)
2. Check for degradation via `_is_degraded` — phantom tables (`IGNORE`, `""`), keyword-as-column names (`UNIQUE`, `DISTINCT`)
3. If degraded and not the last dialect, try the next one
4. If all fail, raise `InvalidQueryDefinition` (a `ValueError` subclass from [`exceptions.py`](sql_metadata/exceptions.py))

---

### ColumnExtractor — columns and aliases

**File:** [`column_extractor.py`](sql_metadata/column_extractor.py) | **Class:** `ColumnExtractor`

Performs a single-pass depth-first walk of the AST in `arg_types` key order (which mirrors left-to-right SQL text order). Collects columns and column aliases into a `_Collector` accumulator. Returns an `ExtractionResult` frozen dataclass — consumed directly by `Parser.columns` and friends.

`Parser` calls `ColumnExtractor` directly (no wrapper functions):

```python
extractor = ColumnExtractor(ast, table_aliases, cte_name_map)
result = extractor.extract()  # returns ExtractionResult
result.columns        # UniqueList of column names
result.columns_dict   # columns by clause section
result.alias_map      # {alias: target_column}
```

#### Data flow

```mermaid
flowchart TB
    AST["sqlglot AST"] --> EXT["ColumnExtractor.extract()"]
    TA["table_aliases\n(from TableExtractor)"] --> EXT
    EXT --> WALK["_walk() — DFS in\narg_types key order"]
    WALK --> COLL["_Collector\n(mutable accumulator)"]
    COLL --> RES["ExtractionResult\n(frozen dataclass)"]
```

#### DFS dispatch

The walk visits each node and routes it through `_dispatch_leaf`, which calls a specialised handler or inline branch depending on the node type:

| AST Node Type | Routing | What happens |
|---------------|---------|-------------|
| `exp.Star` | inline in `_dispatch_leaf` | Adds `*` (skips if inside a function like `COUNT(*)`) |
| `exp.ColumnDef` | inline in `_dispatch_leaf` | Adds column name for CREATE TABLE DDL |
| `exp.Identifier` | inline in `_dispatch_leaf` | Adds column if in JOIN USING context |
| `exp.CTE` | `_handle_cte` | Records CTE name, processes column definitions |
| `exp.Column` | `_handle_column` | Main handler — resolves table alias, builds full name |
| `exp.Subquery` (aliased) | inline in `_dispatch_leaf` | Records subquery name and depth for ordering |

**Special processing** in `_process_child_key`:
- SELECT expressions → `_handle_select_exprs` → iterates expressions, detects aliases
- INSERT schema → `_handle_insert_schema` → extracts column list from `INSERT INTO t(col1, col2)`
- JOIN USING → `_handle_join_using` → extracts column identifiers

**Error handling** — `_handle_cte` raises `InvalidQueryDefinition` if a `WITH` clause contains an alias-less CTE (invalid SQL).

#### Clause classification

`_classify_clause` maps each `arg_types` key to a `columns_dict` section:

| Key | Section |
|-----|---------|
| `expressions` (under `Select`) | `"select"` |
| `expressions` (under `Update`) | `"update"` |
| `where` | `"where"` |
| `group` | `"group_by"` |
| `order` | `"order_by"` |
| `having` | `"having"` |
| `on`, `using` | `"join"` |

#### Alias handling

`_handle_alias` processes `SELECT expr AS alias`:

1. If the aliased expression contains a subquery → walk it recursively, extract its SELECT columns as the alias target
2. If the expression has columns → add them, then register the alias mapping (unless it's a self-alias like `SELECT col AS col`)
3. If no columns (e.g., `SELECT 1 AS num`) → register the alias with no target

#### Date-part function filtering

`_is_date_part_unit` prevents extracting unit keywords as columns in functions like `DATEADD(day, 1, col)` — `day` is a keyword, not a column reference.

---

### TableExtractor — tables and table aliases

**File:** [`table_extractor.py`](sql_metadata/table_extractor.py) | **Class:** `TableExtractor`

Walks the AST for `exp.Table` nodes, builds fully-qualified table names, and sorts results by each table identifier's character position recorded by sqlglot's tokenizer.

#### Extraction flow

```mermaid
flowchart TB
    AST["sqlglot AST"] --> CREATE{"exp.Create?"}
    CREATE -->|Yes| TARGET["_extract_create_target()\nTarget goes first"]
    CREATE -->|No| SKIP["skip"]
    TARGET --> COLLECT
    SKIP --> COLLECT["_table_nodes()\nfind_all(exp.Table), cached"]
    COLLECT --> FILTER["Filter out CTE names"]
    FILTER --> SORT["Sort by _table_start_position()\n(Identifier.meta['start'])"]
    SORT --> FINAL["UniqueList:\nCREATE target + sorted tables"]
```

**Key algorithms:**

- **Name construction** — `_table_full_name` assembles `catalog.db.name`, with special handling for bracket mode (TSQL, via `_bracketed_full_name`) and double-dot notation (`catalog..name`, detected by `db == ""` in the AST).
- **Position sorting** — `_table_start_position` reads each table identifier's character offset from sqlglot's tokenizer (`Identifier.meta['start']`). No regex scan of the raw SQL is needed — the AST already carries source positions.
- **CTE filtering** — table names matching known CTE names are excluded, so only real tables appear in the output.
- **CREATE target placement** — for `CREATE TABLE ... AS SELECT` statements, the target table is extracted via `_extract_create_target` and prepended to the result regardless of its source position.

**Alias extraction** — `extract_aliases(tables)` walks the cached `exp.Table` nodes looking for aliases, keeping only those whose fully-qualified name appears in *tables*:

```sql
SELECT * FROM users u JOIN orders o ON u.id = o.user_id
--                   ^            ^
--              alias="u"    alias="o"
-- Result: {"u": "users", "o": "orders"}
```

---

### NestedResolver — CTE/subquery names, bodies, and resolution

**File:** [`nested_resolver.py`](sql_metadata/nested_resolver.py) | **Class:** `NestedResolver`

Handles the complete "look inside nested queries" concern. Created lazily by `Parser._get_resolver()`, which passes the `Parser` class itself as a `parser_factory` callable (dependency injection) so the resolver can instantiate sub-parsers without importing `Parser` at module load time.

#### Four responsibilities

**1. Name extraction** — extract CTE and subquery names from the AST:

- `extract_cte_names(cte_name_map)` — instance method, walks `exp.CTE` nodes and collects their aliases (with the reverse CTE name map applied to restore dots that `SqlCleaner` replaced with `__DOT__`).
- `extract_subqueries(ast)` — static method, single post-order walk that returns `(names, bodies)` together. Innermost subqueries appear first. Aliased subqueries keep their alias; unaliased ones get synthetic `subquery_N` names.

Called directly by `Parser.with_names` and `Parser.subqueries_names`.

**2. Body extraction** — render CTE/subquery AST nodes back to SQL:

- `extract_cte_bodies(cte_name_map)` — finds `exp.CTE` nodes in the AST and renders each body via `_PreservingGenerator`.
- Subquery bodies are produced alongside their names by `extract_subqueries` — no separate body-extraction method.
- `_PreservingGenerator` — custom sqlglot `Generator` that preserves function signatures sqlglot would normalise: keeps `IFNULL` instead of rewriting to `COALESCE`, keeps `DIV` instead of `CAST(... / ... AS INT)`, renders `DATE_ADD`/`DATE_SUB`, and preserves `IS NOT NULL` / `NOT IN` idioms.

**3. Column resolution** — `resolve()` runs two phases:

```mermaid
flowchart TB
    INPUT["columns from ColumnExtractor"]
    INPUT --> P1["Phase 1: _resolve_sub_queries()\nReplace subquery.column refs\nwith actual columns"]
    P1 --> P2["Phase 2: _resolve_unqualified_through_nested()\nDrop bare names that are\naliases in nested queries"]
    P2 --> OUTPUT["Resolved columns"]
```

Phase 1 example:
```sql
SELECT sq.name FROM (SELECT name FROM users) sq
-- "sq.name" → resolved through subquery → "name"
```

Phase 2 example:
```sql
WITH cte AS (SELECT id, name AS label FROM users)
SELECT label FROM cte
-- "label" is an alias inside the CTE → dropped from columns, added to aliases
```

**4. Recursive sub-Parser instantiation** — when resolving `subquery.column`, the resolver invokes `self._parser_factory(body_sql)` to build a new `Parser` for each nested body (cached in `_subqueries_parsers` / `_with_parsers`). The full pipeline runs recursively for each CTE/subquery, but the dependency is injected rather than imported.

#### Alias resolution with cycle detection

`resolve_column_alias` (public) and its private helper `_resolve_column_alias` follow alias chains with a `visited` set to prevent infinite loops:

```python
# a → b → c (resolves to "c")
# a → b → a (cycle detected, stops at "a")
```

---

### QueryTypeExtractor

**File:** [`query_type_extractor.py`](sql_metadata/query_type_extractor.py) | **Class:** `QueryTypeExtractor`

Maps the AST root node type to a `QueryType` enum value via `_SIMPLE_TYPE_MAP`:

| AST Node | QueryType |
|----------|-----------|
| `exp.Select`, `exp.Union`, `exp.Intersect`, `exp.Except` | `SELECT` |
| `exp.Insert` | `INSERT` |
| `exp.Update` | `UPDATE` |
| `exp.Delete` | `DELETE` |
| `exp.Create` | `CREATE` |
| `exp.Alter` | `ALTER` |
| `exp.Drop` | `DROP` |
| `exp.TruncateTable` | `TRUNCATE` |
| `exp.Merge` | `MERGE` |

Special handling:
- A bare `exp.With` root (a `WITH` clause with no main statement) raises `InvalidQueryDefinition` — it is not valid SQL on its own.
- `exp.Command` → `_resolve_command_type` inspects the command's `this` attribute and maps `CREATE` back to `QueryType.CREATE` so dialect-specific DDL that degrades to an opaque command still returns a useful type.
- `REPLACE INTO` → `Parser` forwards the `ASTParser.is_replace` flag into the extractor's constructor; when the AST is `exp.Insert` and `is_replace` is true, the extractor returns `QueryType.REPLACE` directly.
- Empty / comment-only SQL → `_raise_for_none_ast` distinguishes "no parseable content" (`"Empty queries are not supported!"`) from "had content but sqlglot produced no AST" (`"Could not parse the query — the SQL syntax appears to be invalid"`), both raised as `InvalidQueryDefinition`.

---

### Comments

**File:** [`comments.py`](sql_metadata/comments.py)

A collection of pure stateless functions (no class). Exploits the fact that sqlglot's tokenizer skips comments — comments live in the *gaps* between consecutive token positions.

**Algorithm:**

1. Tokenize the SQL with the appropriate tokenizer
2. For each gap between token `[i].end` and token `[i+1].start`, scan for comment delimiters (`--`, `/* */`, `#`)
3. Collect or strip the matches

**Tokenizer selection** — `_choose_tokenizer`:
- If SQL contains `#` used as a comment (not a variable) → MySQL tokenizer (treats `#` as comment delimiter)
- Otherwise → default sqlglot tokenizer
- `_has_hash_variables` distinguishes `#temp` (MSSQL) and `#VAR#` (template) from `# comment` (MySQL)

**Two stripping variants:**
- `strip_comments` — public API, preserves `#VAR` references
- `strip_comments_for_parsing` — internal, always strips `#` comments (needed before `sqlglot.parse()`)

---

### Supporting Modules

**[`keywords_lists.py`](sql_metadata/keywords_lists.py):**
- `QueryType` — string enum (`str, Enum`) for direct comparison (`parser.query_type == "SELECT"`)

**[`utils.py`](sql_metadata/utils.py):**
- `UniqueList` — deduplicating list with O(1) membership checks via internal `set`. Used everywhere to collect columns, tables, aliases.
- `last_segment` — returns the last dot-separated segment of a qualified name (e.g. ``"schema.table.column"`` → ``"column"``).
- `DOT_PLACEHOLDER` — encoding constant for qualified CTE names (``__DOT__``).

**[`exceptions.py`](sql_metadata/exceptions.py):**
- `InvalidQueryDefinition` — a `ValueError` subclass raised whenever the SQL is structurally invalid (empty, unparseable, unsupported query type, alias-less CTE, or all dialects degraded). Inheriting from `ValueError` keeps existing `except ValueError:` handlers working while giving callers a specific type to catch.

**[`generalizator.py`](sql_metadata/generalizator.py)** — anonymises SQL for log aggregation: strips comments, replaces literals with `X`, numbers with `N`, collapses `IN(...)` lists to `(XYZ)`.

---

## Traced Walkthrough

Let's trace `Parser("SELECT a AS x FROM t").columns_aliases` step by step.

```mermaid
sequenceDiagram
    participant User
    participant Parser
    participant ASTParser
    participant sqlglot
    participant TableExtractor
    participant ColumnExtractor
    participant NestedResolver

    User->>Parser: .columns_aliases
    Parser->>Parser: .columns (not cached yet)

    Note over Parser: Need AST and table_aliases

    Parser->>ASTParser: .ast (first access)
    ASTParser->>ASTParser: SqlCleaner.clean()
    Note over ASTParser: No REPLACE, no comments,<br/>no qualified CTEs
    ASTParser->>ASTParser: DialectParser().parse()
    Note over ASTParser: No special syntax →<br/>[None, "mysql"]
    ASTParser->>sqlglot: sqlglot.parse(sql, dialect=None)
    sqlglot-->>ASTParser: exp.Select AST

    Parser->>Parser: .tables_aliases
    Parser->>TableExtractor: extract_aliases(tables)
    Note over TableExtractor: No aliases on "t"
    TableExtractor-->>Parser: {}

    Parser->>ColumnExtractor: ColumnExtractor(ast, {}, {}).extract()
    Note over ColumnExtractor: _walk() DFS begins

    Note over ColumnExtractor: Visit Select node →<br/>_walk_children()
    Note over ColumnExtractor: key="expressions" + Select →<br/>_handle_select_exprs()
    Note over ColumnExtractor: expr[0] is Alias "x" →<br/>_handle_alias()
    Note over ColumnExtractor: inner is Column "a" →<br/>_flat_columns() → ["a"]<br/>add_column("a", "select")<br/>add_alias("x", "a", "select")
    Note over ColumnExtractor: key="from" →<br/>skip (Table, not Column)

    ColumnExtractor-->>Parser: ExtractionResult (frozen dataclass)

    Note over Parser: result.columns=["a"]<br/>result.alias_map={"x": "a"}

    Parser->>NestedResolver: resolve(columns, ...)
    Note over NestedResolver: No subqueries or CTEs<br/>→ columns unchanged

    NestedResolver-->>Parser: (["a"], {...}, {"x": "a"})

    Parser-->>User: {"x": "a"}
```

**What happened:**

1. **`Parser.__init__`** — stored raw SQL, created `ASTParser` (lazy)
2. **`.columns_aliases`** accessed → triggers `.columns` (not cached)
3. **`.columns`** needs the AST → accesses `self._ast_parser.ast`
4. **`ASTParser.ast`** (first access) → `SqlCleaner.clean()` → `DialectParser().parse()` → `sqlglot.parse()`
5. **`.tables_aliases`** needed for column extraction → `TableExtractor.extract_aliases()` → `{}` (no aliases on `t`)
6. **`ColumnExtractor(ast, {}, {}).extract()`** → DFS walk:
   - Visits `Select` node, key `"expressions"` → `_handle_select_exprs()`
   - Finds `Alias(Column("a"), "x")` → `_handle_alias()` → records column `"a"` in select section, alias `"x"` → `"a"`
   - Key `"from"` → finds `Table("t")`, not a column node, skipped
7. **`NestedResolver.resolve()`** — no subqueries or CTEs, columns pass through unchanged
8. **Result cached** — `_columns = ["a"]`, `_columns_aliases = {"x": "a"}`

---

## Dependency Graph

```mermaid
flowchart TB
    INIT["__init__.py"]
    INIT --> P["parser.py"]
    INIT --> EXC["exceptions.py"]

    P --> AST["ast_parser.py"]
    P --> EXT["column_extractor.py"]
    P --> TAB["table_extractor.py"]
    P --> RES["nested_resolver.py"]
    P --> QT["query_type_extractor.py"]
    P --> COM["comments.py"]
    P --> GEN["generalizator.py"]
    P --> KW["keywords_lists.py"]
    P --> UT["utils.py"]

    AST --> SC["sql_cleaner.py"]
    AST --> DP["dialect_parser.py"]

    SC --> COM
    SC --> EXC
    DP --> COM
    DP --> EXC
    DP -.->|"sqlglot.parse()"| SG["sqlglot"]
    TAB --> DP

    EXT -.-> SG
    EXT --> UT
    EXT --> EXC
    TAB -.-> SG
    RES -.-> SG
    RES --> UT
    RES -.->|"parser_factory\n(injected by Parser)"| P
    QT -.-> SG
    QT --> KW
    QT --> EXC
    COM -.->|"Tokenizer"| SG
    GEN --> COM

    style SG fill:#f0f0f0,stroke:#999
```

`nested_resolver.py` needs `Parser` to recursively analyse CTE/subquery bodies, but importing `Parser` at module load would create a cycle (`parser.py` already imports `NestedResolver`). Instead, `Parser._get_resolver()` passes the `Parser` class itself into `NestedResolver.__init__` as a `parser_factory` callable — pure dependency injection. The only `parser.py` reference in `nested_resolver.py` is a `TYPE_CHECKING`-guarded import for type hints.

---

## Key Design Patterns

**Lazy evaluation with caching** — every `Parser` property computes on first access and caches the result. This means you pay zero cost for properties you never access.

**Composition over inheritance** — `Parser` doesn't subclass anything meaningful. It composes `ASTParser` (which itself composes `SqlCleaner` and `DialectParser`), `TableExtractor`, `ColumnExtractor`, `NestedResolver`, and `QueryTypeExtractor` as separate concerns.

**Single-pass DFS extraction** — `ColumnExtractor` walks the AST exactly once in `arg_types` key order. Because sqlglot's `arg_types` keys are ordered to mirror left-to-right SQL text, the walk naturally processes clauses in source order.

**Multi-dialect retry with degradation detection** — rather than guessing one dialect, `DialectParser` tries several in order and picks the first that doesn't produce a degraded result (phantom tables, keyword-as-column names).

**Graceful regex fallbacks** — when the AST parse fails entirely, the parser degrades to regex-based extraction for columns (INSERT INTO pattern) and LIMIT/OFFSET rather than raising an error.

**Recursive sub-parsing via dependency injection** — `NestedResolver` creates fresh `Parser` instances for CTE/subquery bodies using a `parser_factory` callable injected by `Parser._get_resolver()`. This reuses the entire pipeline recursively (with caching to avoid re-parsing the same body twice) without introducing a module-level import cycle.
