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

### 1.6 `NOT` Expression
There is no `Not` expression type. Users must write raw SQL strings (`Expr("NOT (...)")`) to negate conditions. A `Not{Sqlizer}` wrapper would be a natural complement to the existing `And` and `Or` conjunction types.

### 1.7 `BETWEEN` Expression
`BETWEEN` is standard SQL and there's no expression type for it. Users must construct it with `Expr("col BETWEEN ? AND ?", lo, hi)`. A `Between{"col": [2]interface{}{lo, hi}}` type would be consistent with the existing `Eq`, `Lt`, etc. helpers.

> **GitHub [#340](https://github.com/Masterminds/squirrel/issues/340)** — "Add sq.Between feature" (opened 2022-11-05). Direct request for a `Between` expression type.

### 1.8 `EXISTS` / `NOT EXISTS` Subquery Helper
These are extremely common in correlated subqueries. Currently requires:
```go
Expr("EXISTS (?)", subQuery)  // fragile
```
A dedicated `Exists(SelectBuilder)` / `NotExists(SelectBuilder)` helper would be safer and clearer.

### 1.9 `JOIN ... USING` Convenience
All join helpers assume `ON` clauses via freeform strings. A `JoinUsing("table", "col1", "col2")` convenience would reduce boilerplate for the common case.

### 1.10 `FULL OUTER JOIN`
Only `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`, and `CROSS JOIN` are provided. `FULL OUTER JOIN` is missing — it's standard SQL supported by all major databases except MySQL (which supports it from 8.0.31+ via workarounds).

### 1.11 Parameterized `LIMIT` / `OFFSET`
`Limit` and `Offset` format the values as literal strings (`fmt.Sprintf("%d", limit)`) directly into SQL rather than using placeholders. This means the query string changes with different limit/offset values, defeating prepared-statement caching. Parameterized limits would allow statement reuse.

> **GitHub [#355](https://github.com/Masterminds/squirrel/issues/355)** — "Limit and Offset use prepare statement placeholder" (3 comments, opened 2023-04-20). Users explicitly request `LIMIT ?` / `OFFSET ?` with args for prepared statement reuse.
>
> **GitHub [#231](https://github.com/Masterminds/squirrel/issues/231)** — "Interface for management Limit, Offset" (opened 2020-02-08). Requests Sqlizer-based limit/offset for more flexibility.

---

## 2. Critical Security Issues

### 2.1 🔴 CRITICAL — SQL Injection via Unparameterized Table & Column Names

**This is the most serious issue in the library.** Multiple builder methods directly interpolate user-supplied strings into SQL without any sanitization or parameterization:

| Method | File | What gets interpolated |
|--------|------|----------------------|
| `From(from string)` | `select.go:281` | Table name goes straight into SQL |
| `Into(into string)` | `insert.go:246` | Table name goes straight into SQL |
| `Table(table string)` | `update.go:221` | Table name goes straight into SQL |
| `Delete(from string)` / `From(from string)` | `delete.go:146` | Table name goes straight into SQL |
| `Join(join string, ...)` | `select.go:298` | Join clause goes straight into SQL |
| `Columns(columns ...string)` | `select.go:257`, `insert.go:250` | Column names go straight into SQL |
| `Set(column string, ...)` | `update.go:225` | Column name in SET clause is unescaped |
| `GroupBy(groupBys ...string)` | `select.go:350` | GROUP BY expressions go straight into SQL |
| `OrderBy(orderBys ...string)` | `select.go:367`, `update.go:266`, `delete.go:157` | ORDER BY expressions go straight into SQL |
| `Options(options ...string)` | `select.go:252`, `insert.go:240` | Raw SQL keywords injected |

**Example attack vector:**
```go
userInput := "users; DROP TABLE users; --"
sq.Select("*").From(userInput).ToSql()
// Produces: SELECT * FROM users; DROP TABLE users; --
```

If _any_ of these string arguments come from user input (e.g., a dynamic sort column from an API query parameter), the application is vulnerable to SQL injection. The library should:

1. **Document** prominently that these methods must NEVER receive unsanitized user input.
2. Provide **identifier quoting** helpers (e.g., `QuoteIdentifier(name string, dialect Dialect) string`) that users can wrap around dynamic identifiers.
3. Consider adding methods that accept identifiers as validated/quoted types rather than raw strings.

> **GitHub [#328](https://github.com/Masterminds/squirrel/issues/328)** — "OrderBy column name placeholder" (opened 2022-08-06). User asks exactly this: "is there a way in squirrel to safely build an ORDER BY clause with column name coming from user input?" — currently there is not.
>
> **GitHub [#294](https://github.com/Masterminds/squirrel/issues/294)** — "How to set dynamic parameters for the From field" (opened 2021-08-29). User resorts to `fmt.Sprintf` in `From()`, creating an injection vector.
>
> **GitHub [#387](https://github.com/Masterminds/squirrel/issues/387)** — "Add link to safe-squirrel" (opened 2025-02-16). A community fork ([bored-engineer/safe-squirrel](https://github.com/bored-engineer/safe-squirrel)) that enforces safe usage via Go's type system to prevent SQL injection. Its existence validates this as a real-world problem.

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

### 3.1 🔴 CRITICAL — `Or{Eq{"a": 1, "b": 2}, Eq{"c": 3, "d": 4}}` Produces Wrong Precedence

> **GitHub [#269](https://github.com/Masterminds/squirrel/issues/269)** — "[BUG] Missing brackets when using several sq.Eq inside sq.Or" (2 comments, opened 2020-12-11).

When a multi-key `Eq` (which produces `a = ? AND b = ?`) is placed inside an `Or`, the generated SQL lacks parentheses around each `Eq` group:
```sql
-- Actual:   WHERE (col1 = ? AND col2 = ? OR col1 = ? AND col2 = ?)
-- Expected: WHERE ((col1 = ? AND col2 = ?) OR (col1 = ? AND col2 = ?))
```
Due to SQL operator precedence (`AND` binds tighter than `OR`), the actual output happens to evaluate the same way, but the missing parentheses are **incorrect** per the documented intent, confusing, and fragile if the expression structure changes. The `Eq.ToSql()` method should wrap multi-key output in parentheses.

### 3.2 🔴 HIGH — Multiple `Distinct()` Calls Produce Invalid SQL

> **GitHub [#281](https://github.com/Masterminds/squirrel/issues/281)** — "[BUG] Multiple calls to Distinct() result in invalid SQL" (opened 2021-04-13).

`Distinct()` appends `"DISTINCT"` to the Options slice every time it is called. Calling `.Distinct().Distinct()` produces `SELECT DISTINCT DISTINCT ...`, which is invalid SQL. The method should be idempotent — either deduplicate options or use a boolean flag.

### 3.3 🔴 HIGH — `nil` Or/And Clause Silently Produces Wrong WHERE

> **GitHub [#382](https://github.com/Masterminds/squirrel/issues/382)** — "Incorrect SQL query when nil Or clause" (opened 2024-10-07).

Passing a `nil` `sq.Or` to `Where()` produces `WHERE (1=0)`, which filters out **all** rows. This is because `Or{}.ToSql()` returns `(1=0)` (the identity for OR). When a dynamically-constructed filter returns `nil`, the user expects "no filter" — not "match nothing". This is a silent data-loss bug.

### 3.4 🔴 HIGH — Dollar Placeholder Misnumbering with Subqueries in `UpdateBuilder.Set`

> **GitHub [#326](https://github.com/Masterminds/squirrel/issues/326)** — "UpdateBuilder.Set with subquery produces wrong dollar parameter placeholders" (opened 2022-07-25).

When using a `SelectBuilder` subquery as a value in `UpdateBuilder.Set()` with `Dollar` placeholders, the placeholder numbers restart from `$1` inside the subquery instead of continuing from the outer query's count, producing duplicate placeholder numbers and incorrect parameter binding.

### 3.5 🟡 HIGH — Misplaced Parameters with Window Functions / Complex Subqueries

> **GitHub [#351](https://github.com/Masterminds/squirrel/issues/351)** — "Misplaced params when using windows or subqueries" (opened 2022-12-31).
>
> **GitHub [#285](https://github.com/Masterminds/squirrel/issues/285)** — "Placeholder count is wrong with sub-queries" (opened 2021-05-19).

When composing multiple subqueries (via `Alias`, `Prefix`/`Suffix` wrapping, or in column expressions), parameter ordering becomes incorrect. The placeholder counter resets per-subquery rather than tracking a global index. Users must work around this with manual `Dollar.ReplacePlaceholders()` calls after `ToSql()`.

### 3.6 🟡 HIGH — `CaseBuilder` Rejects Non-String Values (`int`) in `When`/`Then`

> **GitHub [#388](https://github.com/Masterminds/squirrel/issues/388)** — "expected string or Sqlizer, not int" when using CASE WHEN (opened 2025-03-10).

`Case("order_no").When("ORD001", 500)` fails because `newPart()` only accepts `string` or `Sqlizer`. Integer (and other non-string) literal values should be supported — either by auto-wrapping them in `Expr("?", val)` or by accepting `interface{}` in the WHEN/THEN position.

### 3.7 🟡 HIGH — Conditional Insert Columns/Values Produce Invalid SQL

> **GitHub [#336](https://github.com/Masterminds/squirrel/issues/336)** — "Conditional insert column/value results in invalid SQL" (opened 2022-10-05).

Building an insert incrementally — adding a column+value pair after the initial `Columns(...).Values(...)` — produces separate value groups: `VALUES ($1,$2),($3)` instead of `VALUES ($1,$2,$3)`. The builder treats each `Values()` call as a new row, making conditional column addition impossible without pre-building the complete slices.

### 3.8 🟡 MEDIUM — `nil` Array in `Eq` Produces `(1=0)` Instead of `IS NULL`

> **GitHub [#277](https://github.com/Masterminds/squirrel/issues/277)** — "Null array in where clause argument causes an invalid where clause (1=0)" (opened 2021-02-10).

`sq.Eq{"id": ids}` where `ids` is a `nil` `[]uint64` produces `(1=0)` (empty-IN identity) rather than `id IS NULL` or simply omitting the clause. This silently breaks queries when a filter slice hasn't been populated.

### 3.9 🟡 MEDIUM — `Where()` with Raw String + Slice Arg Doesn't Expand

> **GitHub [#383](https://github.com/Masterminds/squirrel/issues/383)** — "Where with raw sql string and slice arg" (2 comments, opened 2024-10-22).

`Where("id NOT IN ?", []int{1,2,3})` produces `id NOT IN '[1 2 3]'` (Go's `%v` of the slice) instead of expanding to `id NOT IN (?,?,?)`. The `wherePart` for raw strings doesn't introspect slice args the way `Eq` does.

### 3.10 🟡 LOW — `Where()` Doesn't Auto-Parenthesize Raw OR Expressions

> **GitHub [#380](https://github.com/Masterminds/squirrel/issues/380)** — "Auto-parenthesis for Where()" (opened 2024-07-21).

`.Where("a = ? OR b = ?", 1, 2)` combined with another `.Where(...)` produces `WHERE a = ? OR b = ? AND c = ?` — the lack of auto-parenthesization around each `Where()` clause can cause unexpected operator precedence. Other query builders (e.g., GORM) wrap each clause.

---

## 4. Feature Requests (from GitHub Issues)

### High Priority

| Issue | Title | Rationale |
|-------|-------|-----------|
| **[#308](https://github.com/Masterminds/squirrel/issues/308)** | UNION support | Standard SQL, 11 comments, highest community demand. See §1.1. |
| **[#372](https://github.com/Masterminds/squirrel/issues/372)** | Upsert / ON CONFLICT | Essential write pattern, impossible via Suffix for multi-row. See §1.2. |
| **[#271](https://github.com/Masterminds/squirrel/issues/271)** | ✅ CTE / WITH clause | Standard SQL:1999, 8 comments. See §1.4. **Done.** |
| **[#355](https://github.com/Masterminds/squirrel/issues/355)** | Parameterized LIMIT/OFFSET | Defeats prepared-stmt caching. See §1.11. |
| **[#299](https://github.com/Masterminds/squirrel/issues/299)** / **[#258](https://github.com/Masterminds/squirrel/issues/258)** | Subquery in WHERE IN | 12 comments combined. See §1.5. |
| **[#257](https://github.com/Masterminds/squirrel/issues/257)** | JOIN support in DELETE/UPDATE | MySQL DELETE...JOIN and UPDATE...JOIN are common patterns. Currently no `Join()` method on `DeleteBuilder`. |
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
| 🔴 Critical | SQL injection via unquoted identifiers in `From`/`Table`/`Into`/`Columns`/`Set`/`Join`/`OrderBy`/`GroupBy` | [#328](https://github.com/Masterminds/squirrel/issues/328), [#294](https://github.com/Masterminds/squirrel/issues/294), [#387](https://github.com/Masterminds/squirrel/issues/387) | Security |
| 🔴 Critical | `DebugSqlizer` doesn't escape quotes — output looks like valid SQL but is injectable | — | Security |
| 🟡 High | `??` escape logic duplicated with dead code branches | [#322](https://github.com/Masterminds/squirrel/issues/322) | Security/Maintenance |
| 🟡 High | `StmtCache` unbounded growth → memory leak / DoS | — | Security |
| 🟡 Medium | Naive placeholder replacement doesn't respect SQL string literals | [#379](https://github.com/Masterminds/squirrel/issues/379) | Security |
| 🟡 Medium | `StatementKeyword` field not validated | — | Security |

### Outstanding Bugs

| Priority | Issue | GitHub | Type |
|----------|-------|--------|------|
| 🔴 High | Dollar placeholder misnumbering with subqueries in `Update.Set` | [#326](https://github.com/Masterminds/squirrel/issues/326) | Bug |
| 🔴 High | Misplaced params with window functions / multiple subqueries | [#351](https://github.com/Masterminds/squirrel/issues/351), [#285](https://github.com/Masterminds/squirrel/issues/285) | Bug |
| 🔴 High | `nil` Or/And clause silently produces `WHERE (1=0)` | [#382](https://github.com/Masterminds/squirrel/issues/382) | Bug |
| 🟡 High | Multiple `Distinct()` calls produce invalid SQL | [#281](https://github.com/Masterminds/squirrel/issues/281) | Bug |
| 🟡 High | `CaseBuilder` rejects non-string `int` values in When/Then | [#388](https://github.com/Masterminds/squirrel/issues/388) | Bug |
| 🟡 High | Conditional insert columns/values produce invalid SQL | [#336](https://github.com/Masterminds/squirrel/issues/336) | Bug |
| 🟡 High | Multi-key `Eq` inside `Or` missing parentheses | [#269](https://github.com/Masterminds/squirrel/issues/269) | Bug |
| 🟡 Medium | `nil` array in `Eq` produces `(1=0)` instead of `IS NULL` | [#277](https://github.com/Masterminds/squirrel/issues/277) | Bug |
| 🟡 Medium | `Where()` with raw string + slice arg doesn't expand | [#383](https://github.com/Masterminds/squirrel/issues/383) | Bug |
| 🟢 Low | `Where()` doesn't auto-parenthesize raw OR expressions | [#380](https://github.com/Masterminds/squirrel/issues/380) | Bug |

### Feature Requests

| Priority | Issue | GitHub |
|----------|-------|--------|
| ✅ Done | `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` | [#308](https://github.com/Masterminds/squirrel/issues/308) |
| ✅ Done | Upsert (`ON CONFLICT` / `ON DUPLICATE KEY UPDATE`) | [#372](https://github.com/Masterminds/squirrel/issues/372) |
| ✅ Done | CTE (`WITH` / `WITH RECURSIVE`) builder | [#271](https://github.com/Masterminds/squirrel/issues/271) |
| ⭐ High | Parameterized `LIMIT` / `OFFSET` | [#355](https://github.com/Masterminds/squirrel/issues/355) |
| ✅ Done | Subquery in WHERE IN / expression position | [#299](https://github.com/Masterminds/squirrel/issues/299), [#258](https://github.com/Masterminds/squirrel/issues/258) |
| ⭐ High | JOIN in DELETE / UPDATE builders | [#257](https://github.com/Masterminds/squirrel/issues/257) |
| ⭐ High | Common `Where` interface across builders | [#243](https://github.com/Masterminds/squirrel/issues/243) |
| ⭐ High | `RemoveOrderBy` / `GetOrderBy` | [#369](https://github.com/Masterminds/squirrel/issues/369) |
| ⭐ High | `JoinSelect` — join against a subquery | [#241](https://github.com/Masterminds/squirrel/issues/241) |
| ✅ Done | First-class `RETURNING` clause | [#348](https://github.com/Masterminds/squirrel/issues/348) |
| ⭐ High | Identifier quoting helper | [#328](https://github.com/Masterminds/squirrel/issues/328) |
| ⭐ High | `NOT`, `EXISTS` / `NOT EXISTS` expression helpers | — |
| ⭐ Medium | `BETWEEN` expression | [#340](https://github.com/Masterminds/squirrel/issues/340) |
| ⭐ Medium | Mid-query clause (MS SQL `OUTPUT INSERTED`) | [#348](https://github.com/Masterminds/squirrel/issues/348) |
| ⭐ Medium | Named/positional placeholder back-references | [#315](https://github.com/Masterminds/squirrel/issues/315) |
| ⭐ Medium | Mixed raw + parameterized values in `SetMap` | [#377](https://github.com/Masterminds/squirrel/issues/377) |
| ⭐ Medium | Tuple IN condition | [#354](https://github.com/Masterminds/squirrel/issues/354) |
| ⭐ Medium | `SetMap` raw-expression values | [#353](https://github.com/Masterminds/squirrel/issues/353) |
| ⭐ Medium | `Select FROM stored_proc(args...)` | [#306](https://github.com/Masterminds/squirrel/issues/306) |
| ⭐ Medium | `INSERT...SELECT` with non-string columns | [#365](https://github.com/Masterminds/squirrel/issues/365) |
| ⭐ Medium | `FULL OUTER JOIN` | — |
| ⭐ Medium | `JOIN ... USING` convenience | — |
| ⭐ Low | pgvector float-slice formatting | [#366](https://github.com/Masterminds/squirrel/issues/366) |
| ⭐ Low | `COPY FROM` / `COPY TO` | [#359](https://github.com/Masterminds/squirrel/issues/359) |
| ⭐ Low | `STRAIGHT_JOIN` | [#254](https://github.com/Masterminds/squirrel/issues/254) |
| ⭐ Low | `CreateBuilder` (DDL) | [#252](https://github.com/Masterminds/squirrel/issues/252) |
| ⭐ Low | SQL Server paging | [#356](https://github.com/Masterminds/squirrel/issues/356) |
| ⭐ Low | Oracle named params | [#309](https://github.com/Masterminds/squirrel/issues/309) |
| ⭐ Low | Schema validator | [#390](https://github.com/Masterminds/squirrel/issues/390) |

### Maintenance Note

> **GitHub [#227](https://github.com/Masterminds/squirrel/issues/227)** — "Maintainer?" (opened 2020-01-27). The library is in maintenance mode with minimal activity. PRs merge slowly if at all. Any investment in these issues should weigh the likelihood of upstream acceptance.
