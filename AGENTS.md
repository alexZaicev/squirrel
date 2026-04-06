# AGENTS.md

## Project overview

Squirrel is a fluent SQL query builder library for Go. It is **not** an ORM. It builds SQL strings (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CASE`) from composable, chainable method calls and can optionally execute them against a `database/sql` runner.

- **Module path:** `github.com/Masterminds/squirrel`
- **Go version:** `go 1.25.7` (specified in `go.mod`)
- **License:** MIT
- **Status:** Maintenance mode — bug fixes only, no new features.

## Build and test commands

- Run all tests: `go test ./...`
- Run a specific test: `go test -run TestName ./...`
- Run tests with race detector: `go test -race ./...`
- Verify the module builds: `go build ./...`
- Format code: `gofmt -w .`
- Vet code: `go vet ./...`

### Makefile targets

The project includes a `Makefile` with convenient targets:

| Target               | Command                                              |
|----------------------|------------------------------------------------------|
| `make fmt`           | Format with `gofumpt`, `gci`, and `goimports`        |
| `make lint`          | Run `golangci-lint` with project config              |
| `make unit`          | Run tests with `-race`, coverage, and print total %  |
| `make integration`   | Run integration tests against SQLite, MySQL, and PostgreSQL |

### Integration tests

Integration tests live in the `integration/` subdirectory and have their own `go.mod`. They require live database connections (MySQL, PostgreSQL, SQLite) and are not part of the default `go test ./...` run. Use `make integration` or run them manually with driver flags (see `Makefile`).

## Architecture and key concepts

### Core interface

The central interface is `Sqlizer`, defined in `squirrel.go`:

```go
type Sqlizer interface {
    ToSql() (string, []interface{}, error)
}
```

Every builder and expression type implements `Sqlizer`. The internal `rawSqlizer` interface (`toSqlRaw()`) is used for nested queries to avoid double-replacing placeholders.

### Runner interfaces

Execution is abstracted through several interfaces in `squirrel.go` and `squirrel_ctx.go`:

- `Execer` / `ExecerContext` — wraps `Exec` / `ExecContext`
- `Queryer` / `QueryerContext` — wraps `Query` / `QueryContext`
- `QueryRower` / `QueryRowerContext` — wraps `QueryRow` / `QueryRowContext`
- `BaseRunner` — combines `Execer` + `Queryer`
- `Runner` — combines `Execer` + `Queryer` + `QueryRower`
- `RunnerContext` — combines `Runner` + context-aware variants
- `StdSql` — mirrors `*sql.DB` methods; use `WrapStdSql()` to adapt
- `StdSqlCtx` — extends `StdSql` with context methods; use `WrapStdSqlCtx()` to adapt
- `Preparer` / `DBProxy` — for prepared-statement caching via `StmtCache`

### Builder pattern

All builders (`SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`, `CaseBuilder`) use the **immutable builder pattern** via the `github.com/lann/builder` package. Each builder is a type alias over `builder.Builder`:

```go
type SelectBuilder builder.Builder
```

Builder methods return a new copy — they never mutate. Each builder has a corresponding private data struct (e.g., `selectData`, `insertData`) registered with `builder.Register()` in an `init()` function.

### File organization

Each SQL statement type follows a consistent file structure:

| File pattern           | Purpose                                          |
|------------------------|--------------------------------------------------|
| `<type>.go`            | Data struct, `ToSql()`, builder methods          |
| `<type>_ctx.go`        | Context-aware methods (`ExecContext`, etc.)       |
| `<type>_test.go`       | Unit tests for the builder                       |
| `<type>_ctx_test.go`   | Tests for context-aware methods                  |

Shared utilities:
- `squirrel.go` — Core interfaces (`Sqlizer`, `Execer`, `Queryer`, `Runner`, `StdSql`), helper functions (`ExecWith`, `QueryWith`, `DebugSqlizer`, `WrapStdSql`), sentinel errors (`RunnerNotSet`, `RunnerNotQueryRunner`)
- `squirrel_ctx.go` — Context-aware interfaces (`ExecerContext`, `QueryerContext`, `RunnerContext`, `StdSqlCtx`) and helpers (`ExecContextWith`, `QueryContextWith`, `WrapStdSqlCtx`)
- `expr.go` — Expression types: `Eq`, `NotEq`, `Lt`, `Gt`, `LtOrEq`, `GtOrEq`, `Like`, `NotLike`, `ILike`, `NotILike`, `And`, `Or`; functions: `Expr()`, `ConcatExpr()`, `Alias()`
- `placeholder.go` — `PlaceholderFormat` interface and implementations: `Question`, `Dollar`, `Colon`, `AtP`; utility function `Placeholders(count)`
- `where.go` — `wherePart` implementation
- `part.go` — Generic `part` struct, `newPart`, `nestedToSql`, and `appendToSql` helper
- `row.go` — `RowScanner` interface and `Row` wrapper
- `case.go` — `CaseBuilder`, `caseData`, `whenPart`, and `sqlizerBuffer` helper
- `statement.go` — `StatementBuilderType` and package-level convenience functions (`Select()`, `Insert()`, `Replace()`, `Update()`, `Delete()`, `Case()`)
- `stmtcacher.go` — `Preparer`, `DBProxy`, and `StmtCache` for caching prepared statements
- `stmtcacher_ctx.go` / `stmtcacher_noctx.go` — Build-tag split for Go >= 1.8 context support (`NewStmtCache` constructor lives here)

### Build tags

Context-aware files use both the new and legacy build-tag formats for compatibility:

```go
//go:build go1.8
// +build go1.8
```

The `stmtcacher_noctx.go` file uses the inverse constraint (`!go1.8`) and provides a fallback `NewStmtCache` without context support. Always preserve both tag formats when editing these files.

### Placeholder handling

Placeholders default to `?` (MySQL-style). Use `PlaceholderFormat(sq.Dollar)` for PostgreSQL `$1, $2, ...` style. Double question marks `??` are an escape sequence for a literal `?`. When adding nested subqueries, use `rawSqlizer` / `toSqlRaw()` to prevent double-replacement.

The `Placeholders(count int) string` function generates a comma-separated list of `?` placeholders (e.g., `Placeholders(3)` → `"?,?,?"`).

### Key builder features by type

**All builders** share these methods:
- `ToSql()` — build the SQL string and args
- `MustSql()` — like `ToSql()` but panics on error
- `PlaceholderFormat()` — set placeholder style
- `RunWith()` — set a database runner
- `Prefix()` / `PrefixExpr()` — add SQL before the statement
- `Suffix()` / `SuffixExpr()` — add SQL after the statement

**SelectBuilder** notable methods:
- `Distinct()`, `Options()` — add SELECT options
- `Columns()`, `Column()`, `RemoveColumns()` — manage result columns
- `From()`, `FromSelect()` — set FROM clause (supports subqueries)
- `Join()`, `LeftJoin()`, `RightJoin()`, `InnerJoin()`, `CrossJoin()`, `JoinClause()`
- `Where()`, `GroupBy()`, `Having()`
- `OrderBy()`, `OrderByClause()` — simple or complex ORDER BY
- `Limit()`, `RemoveLimit()`, `Offset()`, `RemoveOffset()`
- `Scan()` — shortcut for `QueryRow().Scan()`

**InsertBuilder** notable methods:
- `Into()`, `Columns()`, `Values()`
- `SetMap()` — set columns and values from a `map[string]interface{}`
- `Select()` — `INSERT ... SELECT` support
- `Options()` — add keywords like `IGNORE` before INTO
- `statementKeyword()` (private) — used by `Replace()` to change `INSERT` to `REPLACE`

**UpdateBuilder** notable methods:
- `Table()`, `Set()`, `SetMap()`
- `From()`, `FromSelect()` — PostgreSQL-style `UPDATE ... FROM`
- `Where()`, `OrderBy()`, `Limit()`, `Offset()`

**DeleteBuilder** notable methods:
- `From()`, `Where()`, `OrderBy()`, `Limit()`, `Offset()`
- `Query()` — useful with `RETURNING` clauses

## Code style

- All code is in the single `squirrel` package (no sub-packages except `integration`).
- Use `gofmt` formatting — no custom style rules beyond standard Go conventions.
- Exported types and functions must have Go doc comments.
- Error messages are lowercase and do not end with punctuation (standard Go convention).
- Use `bytes.Buffer` for SQL string assembly.
- Use `fmt.Errorf` for errors (no wrapped errors — the project predates `errors.Is`/`errors.As`).
- Builder methods return the builder type to enable chaining.
- Private data struct fields use PascalCase (required by `github.com/lann/builder` — it uses reflection).
- Use `interface{}` (not `any`) for compatibility with Go 1.14.

## Testing instructions

- Tests use `github.com/stretchr/testify v1.2.2` — specifically `assert.NoError`, `assert.Equal`, `assert.Nil`, etc.
- Test functions follow `TestXxxYyy(t *testing.T)` naming with table-driven tests where appropriate.
- Database interactions are mocked using the `DBStub` struct in `squirrel_test.go` — never require a real database for unit tests.
- The `DBStub` records the last SQL and args passed, enabling assertions against generated queries.
- Tests verify both the generated SQL string and the bound argument slice.
- Always run `go test ./...` (or `make unit`) after making changes and verify all tests pass.
- When adding new features or modifying existing ones, add or update the corresponding tests.
- Tests should cover both the happy path and error cases (e.g., missing required fields).

## Dependencies

- `github.com/lann/builder` — Immutable builder pattern via reflection. Central to the architecture; do not replace.
- `github.com/lann/ps` — Persistent data structures (transitive dependency of `lann/builder`).
- `github.com/stretchr/testify v1.2.2` — Test assertions (test-only).
- `github.com/davecgh/go-spew` — Pretty-printing (transitive, test-only via testify).
- `github.com/pmezard/go-difflib` — Diff output (transitive, test-only via testify).

Do not add new dependencies without strong justification. This is a maintenance-mode library.

## Common pitfalls

- Builder data struct fields **must** be PascalCase (exported) because `lann/builder` accesses them via reflection.
- `builder.Set`, `builder.Append`, and `builder.Extend` return `interface{}` and must be type-asserted back to the builder type.
- All builder `init()` functions must call `builder.Register()` — forgetting this causes runtime panics.
- When nesting `SelectBuilder` as a subquery (e.g., `FromSelect`, `INSERT ... SELECT`), the inner query's placeholder format must be reset to `Question` to prevent double-replacement by the outer query. `FromSelect` does this automatically; `InsertBuilder.Select()` does **not**.
- `[]byte` and `[]uint8` are indistinguishable in Go — `Eq{"col": []uint8{1,2,3}}` will **not** produce an `IN` clause because `database/sql` treats `[]byte` as a single value.
- The `DebugSqlizer` function is for debugging only — its output is not guaranteed to be valid SQL and must never be used for execution.
- Empty `Eq{}` evaluates to `(1=1)` (true) and empty `And{}` also evaluates to `(1=1)`. Empty `Or{}` evaluates to `(1=0)` (false).
- `setRunWith` automatically wraps `StdSql` / `StdSqlCtx` implementations via `WrapStdSql` / `WrapStdSqlCtx` — callers don't need to wrap manually when using `RunWith`.

## Security considerations

- Never interpolate user input directly into SQL strings. Always use parameterized placeholders (`?`).
- `DebugSqlizer` output should never be executed — it inlines arguments for display purposes only.
- When modifying placeholder replacement logic, ensure escaped `??` sequences are handled correctly to prevent injection vectors.
