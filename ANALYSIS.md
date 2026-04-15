# Squirrel Library — Thorough Analysis

---

## 1. Missing Core Features

### 1.1 ✅ `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` Support — **DONE**
~~There is no way to compose set operations. Users must fall back to raw `Suffix`/`Prefix` hacks or string concatenation.~~

**Implemented** (April 2026) via a new `UnionBuilder` type following the same immutable builder pattern as all other builders. Files added: `union.go`, `union_ctx.go`, `union_test.go`, `union_ctx_test.go`, `integration/union_test.go`. Convenience functions `Union()`, `UnionAll()`, `Intersect()`, `Except()` added to `statement.go`.

> **GitHub [#308](https://github.com/Masterminds/squirrel/issues/308)** — "Support UNION operator" (11 comments, opened 2022-02-24). The most-requested feature by comment count. Multiple users need UNION/UNION ALL for pagination CTEs, report queries, and combining result sets.

### 1.2 ✅ `INSERT ... ON CONFLICT` (PostgreSQL) / `ON DUPLICATE KEY UPDATE` (MySQL) — "Upsert" — **DONE**
~~The library has no upsert support. This is one of the most commonly needed write patterns. Users currently have to build it with raw `Suffix("ON CONFLICT ...")`, which is fragile, untyped, and error-prone — particularly for **multi-row inserts** where the suffix approach breaks down. A first-class `OnConflict` / `OnDuplicateKeyUpdate` builder clause on `InsertBuilder` would be very valuable.~~

**Implemented** (April 2026) via new builder methods on `InsertBuilder`. PostgreSQL support: `OnConflictColumns()`, `OnConflictOnConstraint()`, `OnConflictDoNothing()`, `OnConflictDoUpdate()`, `OnConflictDoUpdateMap()`, `OnConflictWhere()`. MySQL support: `OnDuplicateKeyUpdate()`, `OnDuplicateKeyUpdateMap()`. Shared helper `appendSetClauses` for SET clause generation. Values can be literals or `Sqlizer` expressions (e.g., `Expr("EXCLUDED.col")`, `Expr("VALUES(col)")`, subqueries). Full unit and integration test coverage for SQLite, PostgreSQL, and MySQL.

> **GitHub [#372](https://github.com/Masterminds/squirrel/issues/372)** — "Upsert/On Conflict support" (opened 2023-12-25). Specifically calls out the impossibility of using the `Suffix` workaround with multi-row inserts. Follow-up to older closed issue #83.

### 1.3 ✅ `RETURNING` Clause (First-class) — **DONE**
~~PostgreSQL, SQLite (3.35+), and MariaDB all support `RETURNING`. Currently users must use `Suffix("RETURNING id")`, which has no type safety and doesn't participate in placeholder numbering. A dedicated `.Returning("col1", "col2")` method on `InsertBuilder`, `UpdateBuilder`, and `DeleteBuilder` would be a significant improvement.~~

**Implemented** (April 2026) via a new `Returning(columns ...string)` builder method on `InsertBuilder`, `UpdateBuilder`, and `DeleteBuilder`. Each builder's data struct gained a `Returning []string` field. The RETURNING clause is emitted after the main statement body (after ON CONFLICT/ON DUPLICATE KEY for INSERT, after OFFSET for UPDATE/DELETE) and before any Suffixes, ensuring correct SQL clause ordering. Multiple `Returning()` calls accumulate columns via `builder.Extend`. Supports single columns, multiple columns, `*`, and works correctly with all placeholder formats (Question, Dollar). Full unit test coverage in `insert_test.go`, `update_test.go`, `delete_test.go` and integration test coverage in `integration/insert_test.go`, `integration/update_test.go`, `integration/delete_test.go` (tested against SQLite; MySQL tests correctly skipped).

> **GitHub [#348](https://github.com/Masterminds/squirrel/issues/348)** — "No way to add options between INTO and VALUES on INSERT" (opened 2022-12-21). MS SQL requires `OUTPUT INSERTED.ID` *between* `INTO` and `VALUES` — neither `Suffix` nor `Prefix` can handle this. A generic mid-query clause mechanism or dedicated `Returning`/`Output` method is needed.

### 1.4 ✅ Common Table Expressions (CTEs) — `WITH` Clause — **DONE**
~~CTEs are standard SQL (SQL:1999) supported by PostgreSQL, MySQL 8+, SQLite 3.8.3+, and SQL Server. The current `Prefix("WITH cte AS (...")` workaround is awkward and error-prone, especially for recursive CTEs or multiple CTEs. A `With` / `WithRecursive` builder would be a major usability gain.~~

**Implemented** (April 2026) via a new `CteBuilder` type following the same immutable builder pattern as all other builders. Files added: `cte.go`, `cte_ctx.go`, `cte_test.go`, `cte_ctx_test.go`. Convenience functions `With()`, `WithRecursive()`, `WithColumns()`, `WithRecursiveColumns()` added to `statement.go`. Also added `toSQLRaw()` methods to `InsertBuilder`, `UpdateBuilder`, and `DeleteBuilder` (via `insertData`, `updateData`, `deleteData`) so that nested placeholder handling works correctly for all statement types used as CTE main statements.

**Key features:**
- **Single and multiple CTEs:** Chain `.With(name, query)` to define multiple CTEs in one `WITH` clause.
- **Recursive CTEs:** `WithRecursive(name, query)` marks the clause as `WITH RECURSIVE` (SQL standard: RECURSIVE is clause-level).
- **Column lists:** `WithColumns(name, columns, query)` and `WithRecursiveColumns(name, columns, query)` for `WITH cte(col1, col2) AS (...)` syntax.
- **Any main statement:** `.Statement(sqlizer)` accepts any `Sqlizer` — `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`, `UnionBuilder`, or other `CteBuilder`.
- **Correct placeholder handling:** Inner CTE queries and the main statement use `nestedToSQL` / `toSQLRaw` to prevent double placeholder replacement. Works correctly with `Dollar`, `Colon`, and `AtP` formats.
- **Full runner support:** `Exec()`, `Query()`, `QueryRow()`, `Scan()`, and all `Context` variants.
- **Suffix support:** `.Suffix()` / `.SuffixExpr()` for appending clauses like `FOR UPDATE`.

> **GitHub [#271](https://github.com/Masterminds/squirrel/issues/271)** — "Does squirrel support, or plan to support, common table expressions" (8 comments, opened 2020-12-31). Long-standing request with community discussion. Users resort to fragile `Prefix` workarounds.

### 1.5 ✅ Subqueries in Expression Position (`WHERE col IN (SELECT ...)`) — **DONE**
~~While `FromSelect` exists for the `FROM` clause, there is no ergonomic way to use a `SelectBuilder` as a subquery inside `Eq`, `NotEq`, or general `WHERE IN (subquery)` expressions. Users must construct this manually with `Expr("col IN (?)", subquery)`.~~

**Implemented** (April 2026) by detecting `Sqlizer` values in `Eq`/`NotEq` and `Lt`/`Gt`/`LtOrEq`/`GtOrEq` expression types. When a value implements `Sqlizer` (e.g. `SelectBuilder`), it is expanded as a subquery using `nestedToSQL` (which calls `toSQLRaw()` to prevent double placeholder replacement).

**Behavior:**
- `Eq{"col": subquery}` → `col IN (SELECT ...)`
- `NotEq{"col": subquery}` → `col NOT IN (SELECT ...)`
- `Lt{"col": subquery}` → `col < (SELECT ...)` (scalar subquery)
- `Gt{"col": subquery}` → `col > (SELECT ...)` (scalar subquery)
- `LtOrEq{"col": subquery}` → `col <= (SELECT ...)` (scalar subquery)
- `GtOrEq{"col": subquery}` → `col >= (SELECT ...)` (scalar subquery)

**Placeholder handling:** Uses `nestedToSQL` which calls `toSQLRaw()` on the inner query, preventing double placeholder replacement. Works correctly with all placeholder formats (`Question`, `Dollar`, `Colon`, `AtP`). Mixed expressions (e.g., `Eq{"active": true, "user_id": subquery}`) correctly accumulate args from both literal values and subqueries.

**Files modified:** `expr.go`, `expr_test.go`, `integration/expr_test.go`. Full unit test coverage including `Eq`, `NotEq`, `Lt`, `Gt`, `LtOrEq`, `GtOrEq` with subqueries, multi-key expressions, nested `And`/`Or` conditions inside subqueries, integration with `SelectBuilder.Where()`, and Dollar placeholder numbering. Integration tests cover: `Eq`/`NotEq` subquery IN/NOT IN, empty-result subqueries, all-rows subqueries, mixed literal+subquery keys, cross-table subqueries, doubly-nested subqueries, subqueries combined with `And`/`Or`, scalar comparison subqueries (`Lt`/`Gt`/`LtOrEq`/`GtOrEq` with `AVG`/`MIN`/`MAX`), and placeholder correctness for all formats (`Question`, `Dollar`, `Colon`, `AtP`). Tested against SQLite, MySQL, and PostgreSQL.

> **GitHub [#299](https://github.com/Masterminds/squirrel/issues/299)** — "Subquery in the WHERE condition" (5 comments, opened 2021-11-07). Explicit request for `WHERE col IN (SELECT ...)` support with conditional subquery building.
>
> **GitHub [#258](https://github.com/Masterminds/squirrel/issues/258)** — "Select in where clause" (7 comments, opened 2020-08-07). Same need — `WHERE post.id IN (SELECT ...)`. Multiple users confirm this is a gap.
>
> **GitHub [#265](https://github.com/Masterminds/squirrel/issues/265)** — "PostgreSQL :: insert into A (id, val) VALUES ((select x from y where a = ?), 'bbb')" (opened 2020-11-26). Subquery-as-value in INSERT — related gap.

### 1.6 ✅ `NOT` Expression — **DONE**
~~There is no `Not` expression type. Users must write raw SQL strings (`Expr("NOT (...)")`) to negate conditions. A `Not{Sqlizer}` wrapper would be a natural complement to the existing `And` and `Or` conjunction types.~~

**Implemented** (April 2026) via a new `Not` struct type in `expr.go`. `Not` wraps a single `Sqlizer` condition and negates it with `NOT (...)`. It is a natural complement to the existing `And` and `Or` conjunction types.

**Behavior:**
- `Not{Cond: Eq{"active": true}}` → `NOT (active = ?)`
- `Not{Cond: Or{Eq{"a": 1}, Eq{"b": 2}}}` → `NOT ((a = ? OR b = ?))`
- `Not{Cond: Like{"name": "%irrel"}}` → `NOT (name LIKE ?)`
- `Not{Cond: Not{Cond: expr}}` → `NOT (NOT (expr))` (double negation)
- `Not{Cond: nil}` → `(1=1)` (identity — no condition means no negation)
- Composable with `And`/`Or`: `And{Eq{"x": 1}, Not{Cond: Eq{"y": 2}}}` → `(x = ? AND NOT (y = ?))`
- Works correctly inside `SelectBuilder.Where()` and with all placeholder formats (`Question`, `Dollar`, `Colon`, `AtP`)

**Placeholder handling:** Uses `nestedToSQL` internally, which calls `toSQLRaw()` on builders that implement `rawSqlizer`, preventing double placeholder replacement when used with subqueries.

**Files modified:** `expr.go`, `expr_test.go`, `integration/expr_test.go`. Full unit test coverage including basic negation, `Not` with `Or`/`And`/`Like`/`Expr`, nil condition, double negation, composition inside `And`/`Or`, and usage in `SelectBuilder.Where()`. Integration tests cover: `Not` with `Eq`, `Like`, `Or`, `And`+`Not` composition, double negation, `Not` with subquery, nil condition, and Dollar placeholder correctness.

### 1.7 ✅ `BETWEEN` Expression — **DONE**
~~`BETWEEN` is standard SQL and there's no expression type for it. Users must construct it with `Expr("col BETWEEN ? AND ?", lo, hi)`. A `Between{"col": [2]interface{}{lo, hi}}` type would be consistent with the existing `Eq`, `Lt`, etc. helpers.~~

> **GitHub [#340](https://github.com/Masterminds/squirrel/issues/340)** — "Add sq.Between feature" (opened 2022-11-05). Direct request for a `Between` expression type.

**Implemented** (April 2026) via two new `map[string]any` types in `expr.go`: `Between` and `NotBetween`. They follow the same map-based pattern as `Eq`, `Lt`, `Like`, etc.

**Behavior:**
- `Between{"age": [2]interface{}{18, 65}}` → `age BETWEEN ? AND ?`
- `NotBetween{"age": [2]interface{}{18, 65}}` → `age NOT BETWEEN ? AND ?`
- Multiple keys: `Between{"a": [2]interface{}{1, 10}, "b": [2]interface{}{20, 30}}` → `a BETWEEN ? AND ? AND b BETWEEN ? AND ?` (keys sorted alphabetically)
- Empty map: `Between{}` → `(1=1)` (consistent with `Eq{}`)
- Values must be 2-element arrays or slices — wrong size, non-array/slice, or nil values produce descriptive errors
- Composable with `And`/`Or`/`Not`: `And{Eq{"active": true}, Between{"age": [2]interface{}{18, 65}}}` → `(active = ? AND age BETWEEN ? AND ?)`
- Works correctly inside `SelectBuilder.Where()` and with all placeholder formats (`Question`, `Dollar`, `Colon`, `AtP`)

**Files modified:** `expr.go`, `expr_test.go`, `integration/expr_test.go`. Full unit test coverage including basic usage, `NotBetween`, empty map, multiple keys, string values, slice values, nil error, wrong-size error, non-array error, usage in `SelectBuilder.Where()`, Dollar placeholder correctness, and composition with `And`. Integration tests cover: single column, boundary inclusivity, `NotBetween`, combination with `Eq`, string values, no-match, multiple keys, and Dollar placeholders for both `Between` and `NotBetween`.

### 1.8 ✅ `EXISTS` / `NOT EXISTS` Subquery Helper — **DONE**
~~These are extremely common in correlated subqueries. Currently requires:~~
```go
// Old fragile approach — no longer necessary:
Expr("EXISTS (?)", subQuery)
```
~~A dedicated `Exists(SelectBuilder)` / `NotExists(SelectBuilder)` helper would be safer and clearer.~~

**Implemented** (April 2026) via two exported constructor functions in `expr.go`: `Exists(Sqlizer) Sqlizer` and `NotExists(Sqlizer) Sqlizer`. They return a private `existsExpr` struct that implements `Sqlizer`.

**Behavior:**
- `Exists(sub)` → `EXISTS (SELECT ...)`
- `NotExists(sub)` → `NOT EXISTS (SELECT ...)`
- Accepts any `Sqlizer`, not just `SelectBuilder` — works with `Expr(...)` for raw SQL subqueries too
- `Exists(nil)` / `NotExists(nil)` → returns a descriptive error (`"exists operator requires a non-nil subquery"`)
- Composable with `And`/`Or`/`Not`: `And{Eq{"active": true}, Exists(sub)}` → `(active = ? AND EXISTS (SELECT ...))`
- Works correctly inside `SelectBuilder.Where()` and with all placeholder formats (`Question`, `Dollar`, `Colon`, `AtP`)

**Placeholder handling:** Uses `nestedToSQL` internally, which calls `toSQLRaw()` on builders that implement `rawSqlizer`, preventing double placeholder replacement when used with `Dollar` or other numbered formats. Placeholders are numbered sequentially across outer and inner queries.

**Files modified:** `expr.go`, `expr_test.go`, `integration/expr_test.go`. Unit tests cover: basic `Exists`, basic `NotExists`, subquery with args, nil subquery error, usage in `SelectBuilder.Where()`, Dollar placeholder correctness, composition with `And`/`Or`/`Not`, correlated subqueries, and `Expr`-based subqueries. Integration tests cover: correlated `EXISTS`, correlated `NOT EXISTS`, `EXISTS` with parameterized conditions, combination with `Eq`, `NOT EXISTS` combined with conditions, and Dollar placeholder correctness for both `Exists` and `NotExists`.

### 1.9 ✅ `JOIN ... USING` Convenience — **DONE**
~~All join helpers assume `ON` clauses via freeform strings. A `JoinUsing("table", "col1", "col2")` convenience would reduce boilerplate for the common case.~~

**Implemented** (April 2026) via two complementary approaches:

1. **Convenience methods:** Six new `*JoinUsing` methods on `SelectBuilder`: `JoinUsing()`, `LeftJoinUsing()`, `RightJoinUsing()`, `InnerJoinUsing()`, `CrossJoinUsing()`, and `FullJoinUsing()`. Each method takes a table name and one or more column names and generates a `JOIN table USING (col1, col2, ...)` clause. These delegate to `JoinClause()` internally.

2. **Structured `JoinExpr` builder:** A new `JoinExpr(table)` constructor in `join.go` that returns a `JoinBuilder` interface (implemented by unexported `joinExprBuilder`). Chainable methods: `.Type()` (set join type via `JoinType` constants), `.As()` (alias), `.On()` (raw ON conditions with placeholders), `.OnExpr()` (Sqlizer-based ON conditions — reuse `Eq`, `Gt`, `Between`, etc.), `.Using()` (USING columns), `.SubQuery()` (join against a subquery). Pass the result to `SelectBuilder.JoinClause()`. This eliminates raw string concatenation for all join patterns.

**Files modified:** `select.go`, `select_test.go`, `join.go`, `join_test.go`, `example_test.go`, `integration/join_test.go`. Unit tests cover `FullJoin` with and without args, `FullJoinUsing` with single and multiple columns, `JoinExpr` with `JoinFull` type. Runnable `Example*` functions for godoc. Integration tests (skipped on MySQL) cover FULL OUTER JOIN preserving both sides, WHERE filtering, placeholder args in ON clause, FULL OUTER JOIN USING with unmatched rows, and Dollar placeholder correctness.

### 1.10 ✅ `FULL OUTER JOIN` — **DONE**
~~Only `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`, and `CROSS JOIN` are provided. `FULL OUTER JOIN` is missing — it's standard SQL supported by all major databases except MySQL (which supports it from 8.0.31+ via workarounds).~~

**Implemented** (April 2026) via a new `FullJoin(join string, rest ...any)` method on `SelectBuilder`. Follows the same pattern as existing join methods — delegates to `JoinClause("FULL OUTER JOIN " + join, rest...)`. Also includes `FullJoinUsing` as part of the JOIN ... USING convenience feature (§1.9) and `JoinType` constant `JoinFull` for use with the `JoinExpr` structured builder.

**Files modified:** `select.go`, `select_test.go`, `join.go`, `join_test.go`, `example_test.go`, `integration/join_test.go`. Unit tests cover `FullJoin` with and without args, `FullJoinUsing` with single and multiple columns, `JoinExpr` with `JoinFull` type. Runnable `Example*` functions for godoc. Integration tests (skipped on MySQL) cover FULL OUTER JOIN preserving both sides, WHERE filtering, placeholder args in ON clause, FULL OUTER JOIN USING with unmatched rows, and Dollar placeholder correctness.

### 1.11 ✅ JOIN Support in DELETE / UPDATE Builders — **DONE**
~~MySQL `DELETE...JOIN` and `UPDATE...JOIN` are common patterns. There was no `Join()` method on `DeleteBuilder` or `UpdateBuilder`. Users had to fall back to raw `Suffix`/`Prefix` hacks or string concatenation for multi-table deletes/updates.~~

**Implemented** (April 2026) by adding `Joins []Sqlizer` fields to both `deleteData` and `updateData`, along with a `UsingParts []Sqlizer` field on `deleteData` for PostgreSQL `DELETE ... USING` syntax.

**UpdateBuilder** — the join clause is emitted between the table name and `SET`:
- `UPDATE t1 JOIN t2 ON t1.id = t2.t1_id SET t1.name = ? WHERE ...`
- Methods: `JoinClause()`, `Join()`, `LeftJoin()`, `RightJoin()`, `InnerJoin()`, `CrossJoin()`, `FullJoin()`, `JoinUsing()`, `LeftJoinUsing()`, `RightJoinUsing()`, `InnerJoinUsing()`, `CrossJoinUsing()`, `FullJoinUsing()`
- Compatible with structured `JoinExpr` builder via `JoinClause()`
- Coexists with PostgreSQL-style `From()` / `FromSelect()` — JOIN comes before SET, FROM comes after

**DeleteBuilder** — two complementary approaches for different SQL dialects:
- **MySQL-style JOINs:** `DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...` — same methods as UpdateBuilder. The table name is automatically duplicated in the MySQL multi-table DELETE syntax.
- **PostgreSQL-style USING:** `DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id AND ...` — new `Using(tables ...string)` method. Multiple tables produce comma-separated list: `USING t2, t3`
- Both approaches emit their clauses after `FROM table` and before `WHERE`

**Placeholder handling:** All join args participate in placeholder numbering. Works correctly with `Question`, `Dollar`, `Colon`, and `AtP` formats. Placeholder numbering flows sequentially: join args → SET args → FROM args → WHERE args.

**Files modified:** `update.go`, `delete.go`, `update_test.go`, `delete_test.go`, `example_test.go`, `integration/update_test.go`, `integration/delete_test.go`. Unit tests cover: all join types (Join, LeftJoin, RightJoin, InnerJoin, CrossJoin, FullJoin), JoinUsing variants, placeholder args in ON clauses, Dollar placeholder numbering, multiple joins, JoinExpr with JoinClause, JOIN+FROM coexistence (UpdateBuilder), USING single/multiple tables, USING+Dollar, JOIN+RETURNING, USING+RETURNING. Example tests for godoc. Integration tests: SQL generation for Dollar/JoinExpr/JoinUsing on both builders; execution tests for MySQL (skipped on SQLite/PostgreSQL); USING tests for PostgreSQL (skipped on others).

### 1.12 ✅ Parameterized `LIMIT` / `OFFSET` — **DONE**
~~`Limit` and `Offset` format the values as literal strings (`fmt.Sprintf("%d", limit)`) directly into SQL rather than using placeholders. This means the query string changes with different limit/offset values, defeating prepared-statement caching. Parameterized limits would allow statement reuse.~~

**Implemented** (April 2026) by changing the `Limit` and `Offset` fields in all builder data structs (`selectData`, `updateData`, `deleteData`, `unionData`) from `string` to `*uint64`. The `toSQLRaw()` methods now emit `LIMIT ?` / `OFFSET ?` with the value as a bound argument, instead of formatting the value directly into the SQL string.

**Key benefits:**
- **Prepared-statement caching:** The SQL string is now identical regardless of limit/offset values (`SELECT * FROM users LIMIT ? OFFSET ?`), enabling database drivers and connection pools to reuse prepared statements across different page sizes.
- **Consistent parameterization:** LIMIT/OFFSET values participate in placeholder numbering for all formats (`Question`, `Dollar`, `Colon`, `AtP`). For example, with Dollar: `SELECT * FROM users WHERE active = $1 LIMIT $2 OFFSET $3`.
- **Backward compatible API:** The `Limit(uint64)` and `Offset(uint64)` method signatures are unchanged. `RemoveLimit()` and `RemoveOffset()` continue to work as before.
- **Zero is a valid value:** `Limit(0)` emits `LIMIT ?` with arg `0` (previously emitted `LIMIT 0` as a literal). `nil` (no Limit called, or after RemoveLimit) omits the clause entirely.
- **Subquery correctness:** Parameterized LIMIT/OFFSET in nested subqueries (e.g., `FromSelect`) work correctly with placeholder renumbering — inner `?` placeholders get renumbered by the outer query's placeholder format.

**Builders affected:** `SelectBuilder`, `UpdateBuilder`, `DeleteBuilder`, `UnionBuilder`.

**Files modified:** `select.go`, `update.go`, `delete.go`, `union.go`, `select_test.go`, `update_test.go`, `delete_test.go`, `union_test.go`, `example_test.go`, `integration/select_test.go`, `integration/delete_test.go`, `integration/union_test.go`. Full unit test coverage including parameterized output, all placeholder formats, zero values, subqueries, RemoveLimit/RemoveOffset, prepared-statement reuse verification, and large values. Integration tests (SQLite) cover all existing LIMIT/OFFSET scenarios plus new parameterized-specific tests.

> **GitHub [#355](https://github.com/Masterminds/squirrel/issues/355)** — "Limit and Offset use prepare statement placeholder" (3 comments, opened 2023-04-20). Users explicitly request `LIMIT ?` / `OFFSET ?` with args for prepared statement reuse.
>
> **GitHub [#231](https://github.com/Masterminds/squirrel/issues/231)** — "Interface for management Limit, Offset" (opened 2020-02-08). Requests Sqlizer-based limit/offset for more flexibility.

### 1.13 ✅ `UPDATE ... FROM (VALUES ...)` — Bulk Updates (PostgreSQL) — **DONE**
~~There is no way to perform bulk updates using inline VALUES lists. Users must construct this manually with raw `Suffix`/`Prefix` hacks or execute multiple individual UPDATE statements. PostgreSQL's `UPDATE ... FROM (VALUES ...)` pattern is the standard way to efficiently update multiple rows with different values in a single statement.~~

**Implemented** (April 2026) via a new `FromValues(values [][]interface{}, alias string, columns ...string)` method on `UpdateBuilder` and a new `valuesExpr` internal type in `expr.go`.

**Behavior:**
- `FromValues(rows, "v", "id", "name")` → `FROM (VALUES (?, ?), (?, ?)) AS v(id, name)`
- Supports any number of rows and columns
- Column names are optional — `FromValues(rows, "v")` → `FROM (VALUES (...)) AS v`
- Values can be literals or `Sqlizer` expressions (e.g., `Expr("NOW()")`)
- Works with all placeholder formats (`Question`, `Dollar`, `Colon`, `AtP`)
- Placeholder numbering flows correctly: SET args → FROM VALUES args → WHERE args
- Composable with `Returning()`, mixed regular `Set()` values, and other clauses
- `FromValues` replaces any previously set `From` / `FromSelect` (they all set the same `From` field)

**Internal implementation:** The `valuesExpr` type implements both `Sqlizer` and `rawSqlizer` interfaces. The `rawSqlizer` implementation ensures correct placeholder numbering when nested inside the outer UPDATE query — inner `?` placeholders are preserved for the outer `ReplacePlaceholders` pass.

**Error handling:**
- Empty rows (`[][]interface{}{}`) → descriptive error: "values expression must have at least one row"
- Empty alias → descriptive error: "values expression must have an alias"

**Example:**
```go
sq.Update("employees").
    Set("name", sq.Expr("v.name")).
    Set("salary", sq.Expr("v.salary")).
    FromValues(
        [][]interface{}{{1, "Alice", 50000}, {2, "Bob", 60000}},
        "v", "id", "name", "salary",
    ).
    Where("employees.id = v.id").
    PlaceholderFormat(sq.Dollar)
// UPDATE employees SET name = v.name, salary = v.salary
//   FROM (VALUES ($1, $2, $3), ($4, $5, $6)) AS v(id, name, salary)
//   WHERE employees.id = v.id
```

**Files modified:** `expr.go`, `update.go`, `select.go`, `expr_test.go`, `update_test.go`, `select_test.go`, `insert_test.go`, `example_test.go`, `integration/update_test.go`, `integration/select_test.go`, `integration/insert_test.go`. Unit tests cover: basic usage, single row, Dollar/Colon/AtP placeholders, no columns, empty rows error, empty alias error, multiple SET clauses, RETURNING composition, mixed Set values with VALUES expressions. Expression-level tests cover: `valuesExpr` basic output, no columns, empty rows error, empty alias error, `rawSqlizer` interface verification, `toSQLRaw()` output, and `Sqlizer` values inside rows. SelectBuilder tests cover: basic FROM VALUES, single row, Dollar placeholders, WHERE filtering, no columns, error cases, and JOIN composition. InsertBuilder composition tests cover: INSERT...SELECT FROM VALUES basic, Dollar/Colon/AtP placeholders, WHERE filtering, NOT EXISTS pattern, RETURNING, and ON CONFLICT composition. Integration tests (PostgreSQL only; skipped on SQLite/MySQL): bulk update multiple rows, single row update, no-match scenario, Dollar placeholder SQL verification, mixed regular Set with VALUES expressions, SELECT FROM VALUES with WHERE/JOIN, INSERT...SELECT FROM VALUES basic and single row, INSERT...SELECT with NOT EXISTS deduplication pattern, and Dollar placeholder SQL verification for all builders.

**Also added `SelectBuilder.FromValues()`** — sets the FROM clause of a SELECT to a VALUES expression. This enables `SELECT ... FROM (VALUES ...)` patterns and naturally composes with `InsertBuilder.Select()` for `INSERT ... SELECT FROM (VALUES ...)`:

```go
// SELECT FROM VALUES — standalone
sq.Select("v.id", "v.name").
    FromValues([][]interface{}{{1, "Alice"}, {2, "Bob"}}, "v", "id", "name")
// SELECT v.id, v.name FROM (VALUES (?, ?), (?, ?)) AS v(id, name)

// INSERT ... SELECT FROM VALUES — bulk insert with deduplication
sq.Insert("employees").Columns("id", "name").
    Select(
        sq.Select("v.id", "v.name").
            FromValues([][]interface{}{{1, "Alice"}, {2, "Bob"}}, "v", "id", "name").
            Where(sq.NotExists(sq.Select("1").From("employees e").Where("e.id = v.id"))),
    )
// INSERT INTO employees (id,name)
//   SELECT v.id, v.name FROM (VALUES (?, ?), (?, ?)) AS v(id, name)
//   WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.id = v.id)
```

> **GitHub [#332](https://github.com/Masterminds/squirrel/issues/332)** — "INSERT ... SELECT FROM (VALUES ...)" (opened 2022-08-15). Now fully supported via `SelectBuilder.FromValues()` composed with `InsertBuilder.Select()`.

---

## 2. Critical Security Issues

### 2.1 ✅ MITIGATED — SQL Injection via Unparameterized Table & Column Names — **DONE**

~~**This is the most serious issue in the library.** Multiple builder methods directly interpolate user-supplied strings into SQL without any sanitization or parameterization.~~

**Mitigated** (April 2026) via a three-pronged approach that preserves full backward compatibility while giving users the tools to safely handle dynamic identifiers:

1. **Prominent WARNING documentation** added to all affected methods (`From()`, `Into()`, `Table()`, `Columns()`, `Set()`, `Join()`, `LeftJoin()`, `RightJoin()`, `InnerJoin()`, `CrossJoin()`, `FullJoin()`, `GroupBy()`, `OrderBy()`, `Options()`) explicitly stating that unsanitized user input must NEVER be passed to them and pointing to safe alternatives.

2. **Identifier quoting/validation helpers** in `ident.go`:
   - `QuoteIdent(name string) (Ident, error)` — ANSI SQL double-quote escaping. Wraps any string safely (even malicious ones like `users; DROP TABLE users; --` → `"users; DROP TABLE users; --"`). Handles schema-qualified names (`"public"."users"`).
   - `ValidateIdent(name string) (Ident, error)` — Strict regex validation against `^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`. Rejects anything with spaces, semicolons, quotes, etc. Returns unquoted identifier.
   - `MustQuoteIdent` / `MustValidateIdent` — Panic variants for known-safe literals.
   - `QuoteIdents` / `ValidateIdents` — Batch variants.
   - `ErrInvalidIdentifier` — Sentinel error for `errors.Is()` checking.
   - `Ident` type implements `Sqlizer`, so it can be used anywhere a `Sqlizer` is accepted.

3. **Safe builder methods** that accept `Ident` values instead of raw strings:
   - **SelectBuilder:** `SafeFrom(Ident)`, `SafeColumns(...Ident)`, `SafeGroupBy(...Ident)`, `SafeOrderBy(...Ident)`, `SafeOrderByDir(Ident, OrderDir)`, `SafeDistinctOn(...Ident)`
   - **InsertBuilder:** `SafeInto(Ident)`, `SafeColumns(...Ident)`
   - **UpdateBuilder:** `SafeTable(Ident)`, `SafeSet(Ident, any)`
   - **DeleteBuilder:** `SafeFrom(Ident)`
   - `OrderDir` type with `Asc` / `Desc` constants for type-safe sort direction.

**Design rationale:** The existing API was **not broken** — all original methods retain their exact signatures and behavior. The `Ident` type is an opaque struct (not a type alias) that can only be created via `QuoteIdent` or `ValidateIdent`, preventing accidental unsafe usage. Two strategies are provided: `QuoteIdent` for maximum flexibility (wraps any string safely) and `ValidateIdent` for strictness (rejects anything that doesn't look like an identifier).

**Files added:** `ident.go`, `ident_test.go`, `integration/ident_test.go`.
**Files modified:** `select.go`, `insert.go`, `update.go`, `delete.go` (WARNING docs + Safe* methods).

**Example — before (unsafe):**
```go
userInput := "users; DROP TABLE users; --"
sq.Select("*").From(userInput).ToSQL()
// Produces: SELECT * FROM users; DROP TABLE users; --  ← SQL INJECTION
```

**Example — after (safe):**
```go
userInput := "users; DROP TABLE users; --"
table, err := sq.QuoteIdent(userInput)  // safely quotes
if err != nil { /* handle */ }
sq.Select("*").SafeFrom(table).ToSQL()
// Produces: SELECT * FROM "users; DROP TABLE users; --"  ← SAFE (treated as identifier)
```

**Example — strict validation:**
```go
col, err := sq.ValidateIdent(userSortColumn)
if err != nil { /* reject — contains invalid characters */ }
sq.Select("*").From("users").SafeOrderByDir(col, sq.Desc)
```

Full unit test coverage (46 tests in `ident_test.go`) including all Safe* methods, injection attempt handling, edge cases, combined queries, and placeholder format compatibility. Integration tests (17 tests in `integration/ident_test.go`) against SQLite covering SafeFrom, SafeColumns, SafeOrderByDir, SafeGroupBy, SafeInto, SafeTable, SafeSet, SafeFrom (delete), and combined queries.

> **GitHub [#328](https://github.com/Masterminds/squirrel/issues/328)** — "OrderBy column name placeholder" (opened 2022-08-06). User asks exactly this: "is there a way in squirrel to safely build an ORDER BY clause with column name coming from user input?" — **now solved** via `SafeOrderBy` / `SafeOrderByDir` with `QuoteIdent` / `ValidateIdent`.
>
> **GitHub [#294](https://github.com/Masterminds/squirrel/issues/294)** — "How to set dynamic parameters for the From field" (opened 2021-08-29). User resorts to `fmt.Sprintf` in `From()`, creating an injection vector. — **now solved** via `SafeFrom` with `QuoteIdent`.
>
> **GitHub [#387](https://github.com/Masterminds/squirrel/issues/387)** — "Add link to safe-squirrel" (opened 2025-02-16). A community fork ([bored-engineer/safe-squirrel](https://github.com/bored-engineer/safe-squirrel)) that enforces safe usage via Go's type system to prevent SQL injection. — **now addressed** natively via the `Ident` type system.

### 2.2 🔴 CRITICAL — `DebugSqlizer` Output Can Be Mistaken for Executable SQL

`DebugSqlizer` inlines argument values using `fmt.Sprintf("'%v'", args[i])` (line 169 of `squirrel.go`). This has two problems:

1. **No escaping of single quotes in values.** If an argument contains a `'` character, the output becomes syntactically valid-looking SQL that is actually malformed or injectable:
   ```go
   DebugSqlizer(Expr("name = ?", "O'Brien"))
   // Produces: name = 'O'Brien'  ← broken SQL / injection vector
   ```
2. Despite the doc comment warning, the function name doesn't scream "unsafe" and there is **no compile-time or runtime guard** against someone passing its output to `db.Exec()`. This is a latent injection vector waiting to happen.

**Recommendations:**
- Escape single quotes in the formatted values (double them: `'` → `''`).
- Rename or add a clearly-unsafe alias like `UnsafeDebugSQL`.
- Return a distinct type (not `string`) that cannot be accidentally passed to `db.Exec()`.

### 2.3 🟡 HIGH — `??` Escape Handling Inconsistency Between `placeholder.go` and `DebugSqlizer`

The `??` escape logic is duplicated between `replacePositionalPlaceholders` (placeholder.go:88-113) and `DebugSqlizer` (squirrel.go:148-173). The two implementations have subtle differences:

- In `DebugSqlizer` (line 155): `if len(sql[p:]) > 1 && sql[p:p+2] == "??"` — the check `len(sql[p:]) > 1` means it will enter the branch, but then line 158 has a dead check `if len(sql[p:]) == 1` which can never be true (since we already confirmed `> 1`). This is a logic bug that could cause an off-by-one or unexpected behavior with edge-case inputs at the end of a SQL string.
- In `placeholder.go` (line 97-100): the same pattern exists — `len(sql[p:]) > 1` followed by `len(sql[p:]) == 1` which is dead code.

While unlikely to be exploitable in practice today, duplicated security-critical logic with dead code paths is a maintenance hazard and should be consolidated into a single, well-tested function.

> **GitHub [#322](https://github.com/Masterminds/squirrel/issues/322)** — "Redundant check in placeholder" (opened 2022-06-01). Community member independently identified the dead code branch in `placeholder.go`. Still unfixed.

### 2.4 🟡 HIGH — `StmtCache` Grows Without Bound (Denial of Service)

`StmtCache` (stmtcacher.go) caches prepared statements in an unbounded `map[string]*sql.Stmt`. If an application generates dynamic queries (e.g., varying `IN` clause sizes, dynamic column lists), the cache grows forever, leaking memory and potentially file descriptors (each `*sql.Stmt` holds a server-side prepared statement). There is a `Clear()` method, but:

- There is no eviction policy (LRU, TTL, max-size).
- There is no documentation warning about unbounded growth.
- This is a **denial-of-service vector** in long-running services.

### 2.5 🟡 MEDIUM — `StatementKeyword` Injection in `InsertBuilder`

`InsertBuilder.statementKeyword()` (insert.go:296) is unexported, but `StatementBuilderType.Replace()` calls it with a hardcoded `"REPLACE"`. If someone were to expose this or use reflection to set `StatementKeyword` to an arbitrary string, it would be written directly into the SQL. The field should be validated against an allow-list (`INSERT`, `REPLACE`).

### 2.6 🟡 MEDIUM — Placeholder Replacement Doesn't Respect SQL String Literals

`replacePositionalPlaceholders` does a naive `strings.Index(sql, "?")` scan. It does not understand SQL string literals or quoted identifiers. A `?` inside a SQL string literal (e.g., `WHERE name = 'what?'`) would be incorrectly treated as a placeholder and replaced with `$1`. The `??` escape is a workaround, but it requires users to know about it and manually double every `?` in their string constants — a fragile contract.

> **GitHub [#379](https://github.com/Masterminds/squirrel/issues/379)** — "Where with parametrized INTERVAL" (9 comments, opened 2024-05-28). User tries `INTERVAL '? DAYS'` — the `?` inside a string literal is consumed by placeholder replacement, causing runtime errors. Demonstrates the real-world impact of this issue.

---

## 3. Outstanding Bug Fixes (from GitHub Issues)

### 3.1 ✅ FIXED — `Or{Eq{"a": 1, "b": 2}, Eq{"c": 3, "d": 4}}` Produces Wrong Precedence — **DONE**

> **GitHub [#269](https://github.com/Masterminds/squirrel/issues/269)** — "[BUG] Missing brackets when using several sq.Eq inside sq.Or" (2 comments, opened 2020-12-11).

~~When a multi-key `Eq` (which produces `a = ? AND b = ?`) is placed inside an `Or`, the generated SQL lacks parentheses around each `Eq` group:~~
```sql
-- Before (broken): WHERE (col1 = ? AND col2 = ? OR col1 = ? AND col2 = ?)
-- After  (fixed):  WHERE ((col1 = ? AND col2 = ?) OR (col1 = ? AND col2 = ?))
```

**Fixed** (April 2026) by wrapping multi-key output in parentheses when the map-based expression type produces more than one AND-joined condition. The fix applies to all four map-based expression types: `Eq`/`NotEq`, `Lt`/`Gt`/`LtOrEq`/`GtOrEq`, `Like`/`NotLike`/`ILike`/`NotILike`, and `Between`/`NotBetween`.

**Behavior:**
- Single-key maps are **unchanged** — `Eq{"a": 1}` still produces `a = ?` (no parentheses)
- Multi-key maps are now parenthesized — `Eq{"a": 1, "b": 2}` produces `(a = ? AND b = ?)`
- This ensures correct precedence when used inside `Or{}`: `Or{Eq{"a": 1, "b": 2}, Eq{"c": 3}}` → `((a = ? AND b = ?) OR c = ?)`
- Works correctly with all placeholder formats (`Question`, `Dollar`, `Colon`, `AtP`)

**Files modified:** `expr.go`, `expr_test.go`, `where_test.go`, `integration/expr_test.go`. Full unit test coverage including `Or` with multi-key `Eq`, `NotEq`, `Lt`/`Gt`, `Between`, mixed multi-key/single-key, `And` with multi-key, `SelectBuilder.Where()` integration, Dollar placeholders, and single-key non-regression. Integration tests cover basic multi-key `Eq` inside `Or`, no-match scenarios, mixed multi-key/single-key, multi-key `Lt`/`Gt`, `NotEq`, and Dollar placeholder correctness.

### 3.2 ✅ FIXED — Multiple `Distinct()` Calls Produce Invalid SQL — **DONE**

> **GitHub [#281](https://github.com/Masterminds/squirrel/issues/281)** — "[BUG] Multiple calls to Distinct() result in invalid SQL" (opened 2021-04-13).

~~`Distinct()` appends `"DISTINCT"` to the Options slice every time it is called. Calling `.Distinct().Distinct()` produces `SELECT DISTINCT DISTINCT ...`, which is invalid SQL. The method should be idempotent — either deduplicate options or use a boolean flag.~~

**Fixed** (April 2026) by replacing the `Options`-based approach with a dedicated `Distinct bool` field on `selectData`. The `Distinct()` method now uses `builder.Set(b, "Distinct", true)` instead of `builder.Extend(b, "Options", ...)`, making it fully idempotent — calling `Distinct()` any number of times always produces a single `DISTINCT` keyword.

**Behavior:**
- `Select("id").From("t").Distinct().Distinct().Distinct()` → `SELECT DISTINCT id FROM t`
- `Distinct()` and `Options()` are independent — `Distinct().Options("SQL_NO_CACHE")` → `SELECT DISTINCT SQL_NO_CACHE ...`
- The `DISTINCT` keyword is emitted before any `Options` in the generated SQL, matching standard SQL syntax

**Files modified:** `select.go`, `select_test.go`, `example_test.go`, `integration/select_test.go`. Unit tests cover idempotent `Distinct()` calls (single, double, triple), `Distinct()` combined with `Options()`, and interaction ordering. Example test demonstrates the idempotent behavior. Integration test verifies that multiple `Distinct()` calls produce valid SQL that executes correctly against a real database.

### 3.3 ✅ FIXED — `nil` Or/And Clause Silently Produces Wrong WHERE — **DONE**

> **GitHub [#382](https://github.com/Masterminds/squirrel/issues/382)** — "Incorrect SQL query when nil Or clause" (opened 2024-10-07).

~~Passing a `nil` `sq.Or` to `Where()` produces `WHERE (1=0)`, which filters out **all** rows. This is because `Or{}.ToSql()` returns `(1=0)` (the identity for OR). When a dynamically-constructed filter returns `nil`, the user expects "no filter" — not "match nothing". This is a silent data-loss bug.~~

**Fixed** (April 2026) by changing `conj.join()` in `expr.go` to return empty SQL for nil/empty conjunctions instead of the mathematical identity values (`(1=1)` for AND, `(1=0)` for OR). Empty SQL is silently omitted from `WHERE` clauses by the existing `appendToSQL` / `appendPrefixedToSQL` infrastructure.

**Root cause:** The `conj.join()` method returned `(1=0)` (the OR identity) when `Or` was nil or empty. While mathematically correct, this was a silent data-loss bug when used in `Where()` — `WHERE (1=0)` filters out **all** rows. The typical pattern that triggered this was dynamically building filters:
```go
var filters sq.Or
if someCondition {
    filters = append(filters, sq.Eq{"col": val})
}
query.Where(filters) // if no conditions → WHERE (1=0) → zero rows!
```

**Behavior after fix:**
- `nil` `Or` / `And` in `Where()` → no `WHERE` clause emitted → all rows returned (no filter)
- Empty `Or{}` / `And{}` in `Where()` → no `WHERE` clause emitted → all rows returned (no filter)
- `nil` `Or` / `And` combined with other `Where()` conditions → only the real conditions appear
- `Or{}.ToSQL()` → `""` (empty SQL, no args) instead of `"(1=0)"`
- `And{}.ToSQL()` → `""` (empty SQL, no args) instead of `"(1=1)"`
- Non-empty `Or` / `And` behavior is completely unchanged

**Companion fixes:**
- `appendToSQL` in `part.go` — fixed separator tracking to use a `first` boolean instead of the raw loop index `i`. This prevents a leading separator (`" AND x = ?"` instead of `"x = ?"`) when early parts produce empty SQL.
- `appendPrefixedToSQL` — new helper function in `part.go` that buffers parts before writing the keyword prefix (`WHERE`, `HAVING`). The keyword is only written if parts produce non-empty SQL. Used by `selectData`, `updateData`, `deleteData`, and `insertData` (conflict WHERE) to prevent dangling `WHERE ` in the output.

**Files modified:** `expr.go`, `part.go`, `select.go`, `update.go`, `delete.go`, `insert.go`, `expr_test.go`, `where_test.go`, `integration/expr_test.go`. Full unit test coverage including nil `Or`/`And`, empty `Or{}`/`And{}`, nil `Or`/`And` in `Where()`, nil followed by real conditions, real conditions followed by nil, Dollar placeholder correctness, and `appendToSQL` separator correctness with empty first parts. Integration tests cover nil/empty `Or`/`And` producing no filter (returning all rows), and combined with real conditions.

### 3.4 ✅ FIXED — Dollar Placeholder Misnumbering with Subqueries in `UpdateBuilder.Set` — **DONE**

> **GitHub [#326](https://github.com/Masterminds/squirrel/issues/326)** — "UpdateBuilder.Set with subquery produces wrong dollar parameter placeholders" (opened 2022-07-25).

~~`Update("t").Set("a", 1).Set("b", Select("x").From("y").Where("z = ?", 2)).Where("id = ?", 3).PlaceholderFormat(Dollar).ToSQL()` produced misnumbered placeholders like `$1, $1, $2` instead of the correct `$1, $2, $3`.~~

**Fixed** (April 2026). The root cause was that `updateData.toSQLRaw()` called `vs.ToSQL()` on `Sqlizer` values in SET clauses, which applied the placeholder format (e.g., Dollar `$1, $2, ...`) on the inner subquery. When the outer `ToSQL()` then applied `ReplacePlaceholders` on the full SQL string, it numbered only the remaining `?` placeholders, causing duplicate/misnumbered positional parameters.

The fix replaces `vs.ToSQL()` with `nestedToSQL(vs)` in the SET clause handling, which calls `toSQLRaw()` on builders that implement the `rawSqlizer` interface. This keeps inner placeholders as `?` so the outer `ReplacePlaceholders` pass numbers everything sequentially. The same fix was applied to `appendSetClauses()` (shared helper used by ON CONFLICT DO UPDATE and ON DUPLICATE KEY UPDATE), `appendValuesToSQL()`, and `appendSelectToSQL()` in `insert.go` which had the identical bug pattern.

**Affected patterns (now all correct):**
- `Update("t").Set("col", Select(...))` — subquery as SET value
- `Update("t").SetMap(map[string]any{"col": Select(...)})` — subquery in SetMap
- `Update("t").Set("col", Expr("(SELECT ...)", args...))` — Expr-wrapped subquery
- `Update("t").Set("col", Case().When(...).Else(...))` — CaseBuilder as SET value
- Multiple subqueries in SET: `Set("a", sub1).Set("b", sub2)` — sequential numbering
- Mixed scenarios: SET subquery + WHERE subquery + FromSelect — all numbered correctly
- All positional placeholder formats: Dollar (`$1`), Colon (`:1`), AtP (`@p1`)

**Files modified:** `update.go`, `insert.go`, `update_test.go`, `integration/update_test.go`. Unit tests cover: single subquery with Dollar, multiple subqueries with Dollar, Colon format, AtP format, Expr-wrapped subquery, CaseBuilder with Dollar, SetMap with subquery and Dollar, mixed FromSelect + Set subquery, and Where with Eq subquery + Set subquery. Integration tests cover: Dollar/Colon/AtP placeholder SQL generation, end-to-end subquery execution against live database, SetMap with subquery, mixed Set + Where subqueries, and multiple SET subqueries execution.

### 3.5 ✅ FIXED — Misplaced Parameters with Window Functions / Complex Subqueries — **DONE**

> **GitHub [#351](https://github.com/Masterminds/squirrel/issues/351)** — "Misplaced params when using windows or subqueries" (opened 2022-12-31).
>
> **GitHub [#285](https://github.com/Masterminds/squirrel/issues/285)** — "Placeholder count is wrong with sub-queries" (opened 2021-05-19).

~~When composing multiple subqueries (via `Alias`, `Prefix`/`Suffix` wrapping, or in column expressions), parameter ordering becomes incorrect. The placeholder counter resets per-subquery rather than tracking a global index. Users must work around this with manual `Dollar.ReplacePlaceholders()` calls after `ToSql()`.~~

**Fixed** (April 2026) by adding `toSQLRaw()` methods to three expression wrapper types — `expr`, `aliasExpr`, and `concatExpr` — so they implement the `rawSqlizer` interface. This ensures that when these types are used nested inside an outer query, `nestedToSQL()` calls `toSQLRaw()` which uses `nestedToSQL()` for inner `Sqlizer` values. This prevents double placeholder formatting — inner subqueries return raw `?` placeholders and the outer query's single `ReplacePlaceholders` pass numbers everything sequentially.

**Root cause:** The three wrapper types called `.ToSQL()` directly on their inner `Sqlizer` values (e.g., `SelectBuilder`). When the inner builder had a positional placeholder format (Dollar, Colon, AtP), `.ToSQL()` applied that format (producing `$1, $2...`). The outer query's `ReplacePlaceholders` then only renumbered the remaining unformatted `?` placeholders, causing duplicate/misnumbered positional parameters.

**Types fixed:**
- `expr` (created by `Expr()`) — refactored into `toSQLInner(nested bool)` helper. `toSQLRaw()` calls `nestedToSQL(as)` for `Sqlizer` args; `ToSQL()` continues to call `as.ToSQL()` for backward compatibility at the top level.
- `aliasExpr` (created by `Alias()`) — `toSQLRaw()` calls `nestedToSQL(e.expr)` instead of `e.expr.ToSQL()`.
- `concatExpr` (created by `ConcatExpr()`) — `toSQLRaw()` calls `nestedToSQL(p)` for `Sqlizer` parts.

**Affected patterns (now all correct):**
- `Column(Alias(subquery, "alias"))` — aliased subquery as column expression
- `Column(Expr("(?) AS alias", subquery))` — Expr with Sqlizer arg in column
- `Column(ConcatExpr("COALESCE(", subquery, ", 0)"))` — ConcatExpr with subquery
- `Prefix("WITH cte AS (?)", subquery)` — Expr-wrapped subquery in prefix
- `Suffix("AND EXISTS (?)", subquery)` — Expr-wrapped subquery in suffix
- Any combination of the above in a single query with Dollar/Colon/AtP format

**Note:** The `FromSelect()` method's existing `PlaceholderFormat(Question)` workaround (line 305 of `select.go`) is now redundant but has been preserved for backward compatibility.

**Files modified:** `expr.go`, `expr_test.go`, `integration/expr_test.go`. Full unit test coverage including `Alias` with Dollar subquery (single and multiple), `Expr` with Dollar subquery (single and multiple args), `ConcatExpr` with Dollar subquery, `Alias(ConcatExpr(...))` nesting, `Prefix`/`Suffix` with Expr-wrapped subqueries, complex multi-position queries with subqueries in columns/WHERE/prefix/suffix, `rawSqlizer` interface verification for all three types, and Colon/AtP format correctness. Integration tests (SQLite) cover: aliased subquery execution, Dollar placeholder SQL generation for all patterns, multi-column aliased subqueries, `Expr` subquery in WHERE, `ConcatExpr` subquery execution, prefix/suffix subqueries, complex multi-position Dollar queries, and Colon/AtP placeholder verification.

### 3.6 ✅ FIXED — `CaseBuilder` Rejects Non-String Values (`int`) in `When`/`Then` — **DONE**

> **GitHub [#388](https://github.com/Masterminds/squirrel/issues/388)** — "expected string or Sqlizer, not int" when using CASE WHEN (opened 2025-03-10).

~~`Case("order_no").When("ORD001", 500)` fails because `newPart()` only accepts `string` or `Sqlizer`. Integer (and other non-string) literal values should be supported — either by auto-wrapping them in `Expr("?", val)` or by accepting `interface{}` in the WHEN/THEN position.~~

**Implemented** (April 2026) by modifying `newPart()` in `part.go` to handle non-string, non-Sqlizer values. The `default` case in `part.ToSQL()` now auto-wraps such values as parameterized placeholders (`"?"` with the value as a bound arg) instead of returning an error. This enables `int`, `float64`, `bool`, and other Go types to be used directly in `CaseBuilder.When()`, `CaseBuilder.Else()`, and `CaseBuilder.what()` positions.

**Examples now work:**
- `Case("order_no").When("ORD001", 500)` — int THEN value
- `Case("status").When(1, "active")` — int WHEN value
- `Case().When(Eq{"active": true}, 1).Else(0)` — int THEN and ELSE values
- `Case("score").When(1.5, "low")` — float64 WHEN value

**Files modified:** `part.go`, `case_test.go`, `integration/case_test.go`. Unit tests cover int, float64, bool, and mixed non-string values in WHEN, THEN, and ELSE positions. Integration tests verify correct query execution against real databases with int THEN/ELSE values, int WHEN values, and mixed non-string values across multiple WHEN clauses.

### 3.7 ✅ FIXED — Conditional Insert Columns/Values Produce Invalid SQL — **DONE**

> **GitHub [#336](https://github.com/Masterminds/squirrel/issues/336)** — "Conditional insert column/value results in invalid SQL" (opened 2022-10-05).

~~Building an insert incrementally — adding a column+value pair after the initial `Columns(...).Values(...)` — produces separate value groups: `VALUES ($1,$2),($3)` instead of `VALUES ($1,$2,$3)`. The builder treats each `Values()` call as a new row, making conditional column addition impossible without pre-building the complete slices.~~

**Fixed** (April 2026) by adding a new `SetColumn(column string, value interface{})` method to `InsertBuilder`. Unlike `Values()` which always appends a new row, `SetColumn` adds a column name and appends the corresponding value to every existing row. If no rows exist yet, a new single-value row is created. This enables conditional, incremental column/value building.

**Examples now work:**
```go
// Conditional column building — the core use case from #336
q := sq.Insert("test").SetColumn("a", 1).SetColumn("b", 2)
if needC {
    q = q.SetColumn("c", 3)
}
// INSERT INTO test (a,b,c) VALUES (?,?,?)  — single row, NOT VALUES (?,?),(?)
```

**Behavior:**
- `SetColumn("col", val)` with no existing rows → creates `VALUES (val)`
- `SetColumn("col", val)` with one existing row → appends `val` to that row
- `SetColumn("col", val)` with multiple existing rows → appends `val` to each row (useful for adding a constant column to a multi-row insert)
- `SetColumn` can be mixed with `Columns().Values()` — `Columns("a","b").Values(1,2).SetColumn("c",3)` → `VALUES (?,?,?)`
- `SetColumn` values can be `Sqlizer` (e.g., `Expr(...)` subqueries)
- `SafeSetColumn(Ident, interface{})` — safe counterpart accepting `Ident` for dynamic column names from user input

**Files modified:** `insert.go`, `insert_test.go`, `integration/insert_test.go`. Unit tests cover: basic incremental building, conditional addition (true and false paths), mixing with `Columns().Values()`, multi-row append, Dollar placeholder correctness, Sqlizer values, ON CONFLICT composition, RETURNING composition, single column, nil values, and `SafeSetColumn`. Integration tests (SQLite) cover: basic insert, conditional insert, skipped condition, mixed with Columns/Values, multi-row append, null values, Dollar placeholder SQL generation, and RETURNING with SetColumn.

### 3.8 ✅ FIXED — `nil` Array in `Eq` Produces `(1=0)` Instead of `IS NULL`

> **GitHub [#277](https://github.com/Masterminds/squirrel/issues/277)** — "Null array in where clause argument causes an invalid where clause (1=0)" (opened 2021-02-10).

`sq.Eq{"id": ids}` where `ids` is a `nil` `[]uint64` produces `(1=0)` (empty-IN identity) rather than `id IS NULL` or simply omitting the clause. This silently breaks queries when a filter slice hasn't been populated.

**Fixed** (April 2026) by checking `valVal.IsNil()` for slice types before checking `valVal.Len() == 0` in `Eq.toSQL()`. A nil slice now produces `col IS NULL` (for `Eq`) or `col IS NOT NULL` (for `NotEq`) instead of the empty-IN identity `(1=0)` / `(1=1)`. An explicitly empty (non-nil) slice `[]int{}` still produces `(1=0)` / `(1=1)` as before. The `IsNil()` check is guarded to only apply to `reflect.Slice` (not `reflect.Array`, which cannot be nil) to avoid a runtime panic.

**Files modified:** `expr.go`, `expr_test.go`, `integration/expr_test.go`. Unit tests cover: `Eq` with nil `[]uint64` → `IS NULL`, `NotEq` with nil `[]int` → `IS NOT NULL`, nil slice combined with other keys. Integration tests cover: nil slice `Eq` returning NULL-category rows, nil slice `NotEq` returning non-NULL rows.

### 3.9 ✅ FIXED — `Where()` with Raw String + Slice Arg Doesn't Expand

> **GitHub [#383](https://github.com/Masterminds/squirrel/issues/383)** — "Where with raw sql string and slice arg" (2 comments, opened 2024-10-22).

`Where("id NOT IN ?", []int{1,2,3})` produces `id NOT IN '[1 2 3]'` (Go's `%v` of the slice) instead of expanding to `id NOT IN (?,?,?)`. The `wherePart` for raw strings doesn't introspect slice args the way `Eq` does.

**Fixed** (April 2026) by adding `expandWhereArgs()` to `where.go`. When a raw string where-part contains slice or array arguments (excluding `[]byte`, which `database/sql` treats as a single value), the function walks the SQL string placeholder-by-placeholder and expands each slice arg into `(?,?,?)` with the individual elements as bound args. Escaped `??` sequences are preserved. Scalar args pass through unchanged.

**Files modified:** `where.go`, `where_test.go`, `integration/select_test.go`. Unit tests cover: `NOT IN` with slice, `IN` with slice, mixed scalar + slice, empty slice, single-element slice, `[]byte` not expanded, escaped `??` preserved, combined with OR auto-parenthesization. Integration tests cover: `NOT IN` exclusion, `IN` inclusion, mixed scalar + slice against live SQLite.

### 3.10 ✅ FIXED — `Where()` Doesn't Auto-Parenthesize Raw OR Expressions

> **GitHub [#380](https://github.com/Masterminds/squirrel/issues/380)** — "Auto-parenthesis for Where()" (opened 2024-07-21).

`.Where("a = ? OR b = ?", 1, 2)` combined with another `.Where(...)` produces `WHERE a = ? OR b = ? AND c = ?` — the lack of auto-parenthesization around each `Where()` clause can cause unexpected operator precedence. Other query builders (e.g., GORM) wrap each clause.

**Fixed** (April 2026) by adding `needsParens()` to `where.go`. Raw string where-parts that contain a bare ` OR ` keyword (case-insensitive) are automatically wrapped in parentheses. Clauses containing only ` AND ` are **not** wrapped because the separator between Where() parts is already `AND`, so precedence is already correct. Clauses that are already fully parenthesized at the outermost level are not double-wrapped.

**Files modified:** `where.go`, `where_test.go`, `integration/select_test.go`. Unit tests cover: OR auto-parenthesized, OR combined with another Where via `appendToSQL`, simple expression not parenthesized, AND-only not parenthesized, already-parenthesized not double-wrapped, slice expansion + OR combined. Integration tests cover: OR combined with another Where (checking correct query results), OR alone, SQL generation verification for OR and AND cases.

---

## 4. Feature Requests (from GitHub Issues)

### High Priority

| Issue | Title | Rationale |
|-------|-------|-----------|
| **[#308](https://github.com/Masterminds/squirrel/issues/308)** | UNION support | Standard SQL, 11 comments, highest community demand. See §1.1. |
| **[#372](https://github.com/Masterminds/squirrel/issues/372)** | Upsert / ON CONFLICT | Essential write pattern, impossible via Suffix for multi-row. See §1.2. |
| **[#271](https://github.com/Masterminds/squirrel/issues/271)** | ✅ CTE / WITH clause | Standard SQL:1999, 8 comments. See §1.4. **Done.** |
| **[#355](https://github.com/Masterminds/squirrel/issues/355)** | ✅ Parameterized LIMIT/OFFSET | Defeats prepared-stmt caching. See §1.11. **Done.** |
| **[#299](https://github.com/Masterminds/squirrel/issues/299)** / **[#258](https://github.com/Masterminds/squirrel/issues/258)** | Subquery in WHERE IN | 12 comments combined. See §1.5. |
| **[#257](https://github.com/Masterminds/squirrel/issues/257)** | ✅ JOIN support in DELETE/UPDATE | MySQL DELETE...JOIN and UPDATE...JOIN are common patterns. **Done.** |
| **[#243](https://github.com/Masterminds/squirrel/issues/243)** | Common `Where` interface across builders | 6 comments. `SelectBuilder`, `UpdateBuilder`, `DeleteBuilder` all have `.Where()` but share no interface, preventing generic filter-application functions. |
| **[#369](https://github.com/Masterminds/squirrel/issues/369)** | `GetOrderBy` / `RemoveOrderBy` | Needed for wrapping queries in count CTEs. Similar to existing `RemoveLimit`/`RemoveOffset`/`RemoveColumns`. |
| **[#241](https://github.com/Masterminds/squirrel/issues/241)** | `JoinSelect` — join against a subquery | Like `FromSelect` but for JOINs. Current workaround (`JoinClause(subquery.Prefix("JOIN (").Suffix(")..."))`) is fragile and causes placeholder issues. |

### Medium Priority

| Issue | Title | Rationale |
|-------|-------|-----------|
| **[#340](https://github.com/Masterminds/squirrel/issues/340)** | `sq.Between` expression | Standard SQL, natural complement to Eq/Lt/Gt. See §1.7. |
| **[#348](https://github.com/Masterminds/squirrel/issues/348)** | Mid-query clause (MS SQL `OUTPUT INSERTED`) | Suffix can't handle it. Needed for MS SQL `RETURNING` equivalent. See §1.3. |
| **[#315](https://github.com/Masterminds/squirrel/issues/315)** | Named and positional placeholder back-references | `?{2}` syntax to re-reference a previous placeholder — useful for `ON CONFLICT DO UPDATE SET value = EXCLUDED.value` patterns. |
| **[#377](https://github.com/Masterminds/squirrel/issues/377)** | Mix of parameterized and raw values in `SetMap` | Users need `col_b = 42` (literal) alongside `col_a = $1` in the same insert/update. Currently impossible without Expr wrapping each raw value. |
| **[#354](https://github.com/Masterminds/squirrel/issues/354)** | Tuple IN condition — `(a, b) IN ((?, ?), (?, ?))` | No expression type. Users must build with raw `Expr`. Common in composite-key lookups. |
| **[#353](https://github.com/Masterminds/squirrel/issues/353)** | `SetMap` treats `"column1 + 1"` as a value, not expression | `SetMap(map[string]interface{}{"col": "col + 1"})` binds `"col + 1"` as a string parameter instead of treating it as SQL. Users need a way to specify raw-expression values in maps (e.g., via `Expr`). |
| **[#306](https://github.com/Masterminds/squirrel/issues/306)** | `Select FROM stored_proc(args...)` | `From()` accepts a single string. No way to pass parameterized args to a function call in `FROM`. |
| **[#365](https://github.com/Masterminds/squirrel/issues/365)** | `INSERT...SELECT` with non-string columns | `Select()` only accepts `...string` for column names. Need to pass an `int` literal (e.g., `SELECT 10, col2...`). Should accept `interface{}` or `Sqlizer`. |

### Low Priority

| Issue | Title | Rationale |
|-------|-------|-----------|
| **[#366](https://github.com/Masterminds/squirrel/issues/366)** | pgvector float-slice to vector string | Niche (pgvector extension). Can be handled in user code. |
| **[#359](https://github.com/Masterminds/squirrel/issues/359)** | `COPY FROM` / `COPY TO` support | PostgreSQL-specific bulk operation. Out of scope for a SQL builder — belongs in a driver layer. |
| **[#254](https://github.com/Masterminds/squirrel/issues/254)** | `STRAIGHT_JOIN` (MySQL/MariaDB) | Niche MySQL hint. Can be done with `JoinClause("STRAIGHT_JOIN ...")`. |
| **[#252](https://github.com/Masterminds/squirrel/issues/252)** | `CreateBuilder` (DDL) | DDL is out of scope for a DML query builder. |
| **[#356](https://github.com/Masterminds/squirrel/issues/356)** | SQL Server paging syntax (`FETCH NEXT ... ROWS ONLY`) | Can be handled with `Suffix`. SQL Server-specific. |
| **[#309](https://github.com/Masterminds/squirrel/issues/309)** | Oracle placeholder format (`:name` named params) | Oracle uses `:name` style. Currently only positional `:1` is supported via `Colon`. |
| **[#390](https://github.com/Masterminds/squirrel/issues/390)** | Schema validator | Out of scope — belongs in a migration/ORM layer, not a query builder. |
| **[#332](https://github.com/Masterminds/squirrel/issues/332)** | `INSERT ... SELECT FROM (VALUES ...)` | Niche PostgreSQL pattern. Complex to model generically. |

---

## 5. Consolidated Summary

### Security Issues

| Priority | Issue | GitHub | Type |
|----------|-------|--------|------|
| ✅ Mitigated | SQL injection via unquoted identifiers in `From`/`Table`/`Into`/`Columns`/`Set`/`Join`/`OrderBy`/`GroupBy` — Safe* methods + `QuoteIdent`/`ValidateIdent` added | [#328](https://github.com/Masterminds/squirrel/issues/328), [#294](https://github.com/Masterminds/squirrel/issues/294), [#387](https://github.com/Masterminds/squirrel/issues/387) | Security |
| 🔴 Critical | `DebugSqlizer` doesn't escape quotes — output looks like valid SQL but is injectable | — | Security |
| 🟡 High | `??` escape logic duplicated with dead code branches | [#322](https://github.com/Masterminds/squirrel/issues/322) | Security/Maintenance |
| 🟡 High | `StmtCache` unbounded growth → memory leak / DoS | — | Security |
| 🟡 Medium | Naive placeholder replacement doesn't respect SQL string literals | [#379](https://github.com/Masterminds/squirrel/issues/379) | Security |
| 🟡 Medium | `StatementKeyword` field not validated | — | Security |

### Outstanding Bugs

| Priority | Issue | GitHub | Type |
|----------|-------|--------|------|
| ✅ Fixed | Dollar placeholder misnumbering with subqueries in `Update.Set` | [#326](https://github.com/Masterminds/squirrel/issues/326) | Bug |
| ✅ Fixed | Misplaced params with window functions / multiple subqueries | [#351](https://github.com/Masterminds/squirrel/issues/351), [#285](https://github.com/Masterminds/squirrel/issues/285) | Bug |
| ✅ Fixed | `nil` Or/And clause silently produces `WHERE (1=0)` | [#382](https://github.com/Masterminds/squirrel/issues/382) | Bug |
| ✅ Fixed | `CaseBuilder` rejects non-string `int` values in When/Then | [#388](https://github.com/Masterminds/squirrel/issues/388) | Bug |
| ✅ Fixed | Conditional insert columns/values produce invalid SQL | [#336](https://github.com/Masterminds/squirrel/issues/336) | Bug |
| ✅ Fixed | Multiple `Distinct()` calls produce invalid SQL | [#281](https://github.com/Masterminds/squirrel/issues/281) | Bug |
| ✅ Fixed | Multi-key `Eq` inside `Or` missing parentheses | [#269](https://github.com/Masterminds/squirrel/issues/269) | Bug |
| ✅ Fixed | `nil` array in `Eq` produces `IS NULL` (was `(1=0)`) | [#277](https://github.com/Masterminds/squirrel/issues/277) | Bug |
| ✅ Fixed | `Where()` with raw string + slice arg now expands to `(?,?,?)` | [#383](https://github.com/Masterminds/squirrel/issues/383) | Bug |
| ✅ Fixed | `Where()` auto-parenthesizes raw OR expressions | [#380](https://github.com/Masterminds/squirrel/issues/380) | Bug |

### Feature Requests

| Priority | Issue | GitHub |
|----------|-------|--------|
| ✅ Done   | `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` | [#308](https://github.com/Masterminds/squirrel/issues/308) |
| ✅ Done   | Upsert (`ON CONFLICT` / `ON DUPLICATE KEY UPDATE`) | [#372](https://github.com/Masterminds/squirrel/issues/372) |
| ✅ Done   | CTE (`WITH` / `WITH RECURSIVE`) builder | [#271](https://github.com/Masterminds/squirrel/issues/271) |
| ✅ Done   | Parameterized `LIMIT` / `OFFSET` | [#355](https://github.com/Masterminds/squirrel/issues/355) |
| ✅ Done   | `UPDATE ... FROM (VALUES ...)` bulk updates | [#332](https://github.com/Masterminds/squirrel/issues/332) |
| ✅ Done   | Subquery in WHERE IN / expression position | [#299](https://github.com/Masterminds/squirrel/issues/299), [#258](https://github.com/Masterminds/squirrel/issues/258) |
| ✅ Done   | JOIN in DELETE / UPDATE builders | [#257](https://github.com/Masterminds/squirrel/issues/257) |
| ⭐ High   | Common `Where` interface across builders | [#243](https://github.com/Masterminds/squirrel/issues/243) |
| ⭐ High   | `RemoveOrderBy` / `GetOrderBy` | [#369](https://github.com/Masterminds/squirrel/issues/369) |
| ⭐ High   | `JoinSelect` — join against a subquery | [#241](https://github.com/Masterminds/squirrel/issues/241) |
| ✅ Done   | First-class `RETURNING` clause | [#348](https://github.com/Masterminds/squirrel/issues/348) |
| ✅ Done   | Identifier quoting helper + Safe* builder methods | [#328](https://github.com/Masterminds/squirrel/issues/328) |
| ✅ Done   | `NOT` expression helper | — |
| ✅ Done   | `EXISTS` / `NOT EXISTS` expression helpers | — |
| ✅ Done   | `BETWEEN` expression | [#340](https://github.com/Masterminds/squirrel/issues/340) |
| ⭐ Medium | Mid-query clause (MS SQL `OUTPUT INSERTED`) | [#348](https://github.com/Masterminds/squirrel/issues/348) |
| ⭐ Medium | Named/positional placeholder back-references | [#315](https://github.com/Masterminds/squirrel/issues/315) |
| ⭐ Medium | Mixed raw + parameterized values in `SetMap` | [#377](https://github.com/Masterminds/squirrel/issues/377) |
| ⭐ Medium | Tuple IN condition | [#354](https://github.com/Masterminds/squirrel/issues/354) |
| ⭐ Medium | `SetMap` raw-expression values | [#353](https://github.com/Masterminds/squirrel/issues/353) |
| ⭐ Medium | `Select FROM stored_proc(args...)` | [#306](https://github.com/Masterminds/squirrel/issues/306) |
| ⭐ Medium | `INSERT...SELECT` with non-string columns | [#365](https://github.com/Masterminds/squirrel/issues/365) |
| ✅ Done   | `FULL OUTER JOIN` | — |
| ✅ Done   | `JOIN ... USING` convenience | — |
| ⭐ Low    | pgvector float-slice formatting | [#366](https://github.com/Masterminds/squirrel/issues/366) |
| ⭐ Low    | `COPY FROM` / `COPY TO` | [#359](https://github.com/Masterminds/squirrel/issues/359) |
| ⭐ Low    | `STRAIGHT_JOIN` | [#254](https://github.com/Masterminds/squirrel/issues/254) |
| ⭐ Low    | `CreateBuilder` (DDL) | [#252](https://github.com/Masterminds/squirrel/issues/252) |
| ⭐ Low    | SQL Server paging | [#356](https://github.com/Masterminds/squirrel/issues/356) |
| ⭐ Low    | Oracle named params | [#309](https://github.com/Masterminds/squirrel/issues/309) |
| ⭐ Low    | Schema validator | [#390](https://github.com/Masterminds/squirrel/issues/390) |
| ✅ Done   | ~~`INSERT ... SELECT FROM (VALUES ...)`~~ | ✅ Now supported via `SelectBuilder.FromValues()` composed with `InsertBuilder.Select()`. |

### Maintenance Note

> **GitHub [#227](https://github.com/Masterminds/squirrel/issues/227)** — "Maintainer?" (opened 2020-01-27). The library is in maintenance mode with minimal activity. PRs merge slowly if at all. Any investment in these issues should weigh the likelihood of upstream acceptance.

---
