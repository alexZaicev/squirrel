# Squirrel - fluent SQL generator for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/alexZaicev/squirrel.svg)](https://pkg.go.dev/github.com/alexZaicev/squirrel)

**Squirrel is not an ORM.**

Squirrel helps you build SQL queries from composable parts:

```go
import sq "github.com/alexZaicev/squirrel"

users := sq.Select("*").From("users").Join("emails USING (email_id)")

active := users.Where(sq.Eq{"deleted_at": nil})

sql, args, err := active.ToSql()

sql == "SELECT * FROM users JOIN emails USING (email_id) WHERE deleted_at IS NULL"
```

```go
sql, args, err := sq.
Insert("users").Columns("name", "age").
Values("moe", 13).Values("larry", sq.Expr("? + 5", 12)).
ToSql()

sql == "INSERT INTO users (name,age) VALUES (?,?),(?,? + 5)"
```

Squirrel can also execute queries directly:

```go
stooges := users.Where(sq.Eq{"username": []string{"moe", "larry", "curly", "shemp"}})
three_stooges := stooges.Limit(3)
rows, err := three_stooges.RunWith(db).Query()

// Behaves like:
rows, err := db.Query("SELECT * FROM users WHERE username IN (?,?,?,?) LIMIT ?",
"moe", "larry", "curly", "shemp", 3)
```

Squirrel makes conditional query building a breeze:

```go
if len(q) > 0 {
users = users.Where("name LIKE ?", fmt.Sprint("%", q, "%"))
}
```

Squirrel wants to make your life easier:

```go
// StmtCache caches Prepared Stmts for you
dbCache := sq.NewStmtCache(db)

// StatementBuilder keeps your syntax neat
mydb := sq.StatementBuilder.RunWith(dbCache)
select_users := mydb.Select("*").From("users")
```

Squirrel loves PostgreSQL:

```go
psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// You use question marks for placeholders...
sql, _, _ := psql.Select("*").From("elephants").Where("name IN (?,?)", "Dumbo", "Verna").ToSql()

/// ...squirrel replaces them using PlaceholderFormat.
sql == "SELECT * FROM elephants WHERE name IN ($1,$2)"


/// You can retrieve id ...
query := sq.Insert("nodes").
Columns("uuid", "type", "data").
Values(node.Uuid, node.Type, node.Data).
Suffix("RETURNING \"id\"").
RunWith(m.db).
PlaceholderFormat(sq.Dollar)

query.QueryRow().Scan(&node.id)
```

You can escape question marks by inserting two question marks:

```sql
SELECT * FROM nodes WHERE meta->'format' ??| array[?,?]
```

will generate with the Dollar Placeholder:

```sql
SELECT * FROM nodes WHERE meta->'format' ?| array[$1,$2]
```

## FAQ

* **How can I build an IN query on composite keys / tuples, e.g. `WHERE (col1, col2) IN ((1,2),(3,4))`? ([#104](https://github.com/Masterminds/squirrel/issues/104))**

  Squirrel does not explicitly support tuples, but you can get the same effect with e.g.:

    ```go
    sq.Or{
      sq.Eq{"col1": 1, "col2": 2},
      sq.Eq{"col1": 3, "col2": 4}}
    ```

    ```sql
    (col1 = ? AND col2 = ? OR col1 = ? AND col2 = ?)
    ```
  with args `[1 2 3 4]`

  (which should produce the same query plan as the tuple version, since AND has
  higher precedence than OR in SQL)

* **Why doesn't `Eq{"mynumber": []uint8{1,2,3}}` turn into an `IN` query? ([#114](https://github.com/Masterminds/squirrel/issues/114))**

  Values of type `[]byte` are handled specially by `database/sql`. In Go, [`byte` is just an alias of `uint8`](https://golang.org/pkg/builtin/#byte), so there is no way to distinguish `[]uint8` from `[]byte`.

* **Some features are poorly documented!**

  Hopefully not anymore! See the [Feature Reference](#feature-reference) below.
  The tests can also be considered a part of the documentation; take a look at
  those for ideas on how to express more complex queries.

## Feature Reference

### Statement Builders

Squirrel provides builders for the four main SQL statement types plus `CASE`
expressions and MySQL's `REPLACE`:

```go
// UPDATE
sql, args, err := sq.Update("users").Set("name", "moe").Set("age", 13).
Where(sq.Eq{"id": 1}).ToSql()
// UPDATE users SET name = ?, age = ? WHERE id = ?

// DELETE
sql, args, err := sq.Delete("users").Where(sq.Eq{"id": 1}).ToSql()
// DELETE FROM users WHERE id = ?

// CASE expression (usable inside SELECT columns, etc.)
sql, args, err := sq.Case("status").
When("1", "'active'").
When("2", "'inactive'").
Else("'unknown'").ToSql()
// CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END

// REPLACE (MySQL-specific; same interface as Insert)
sql, args, err := sq.Replace("users").Columns("name", "age").
Values("moe", 13).ToSql()
// REPLACE INTO users (name,age) VALUES (?,?)
```

### WHERE Expressions

Beyond `Eq`, Squirrel provides a rich set of expression helpers:

```go
sq.NotEq{"id": 1}          // id <> ?
sq.Lt{"age": 18}            // age < ?
sq.LtOrEq{"age": 18}        // age <= ?
sq.Gt{"age": 18}            // age > ?
sq.GtOrEq{"age": 18}        // age >= ?
sq.Like{"name": "%moe%"}    // name LIKE ?
sq.NotLike{"name": "%moe%"} // name NOT LIKE ?
sq.ILike{"name": "sq%"}     // name ILIKE ?  (PostgreSQL)
sq.NotILike{"name": "sq%"}  // name NOT ILIKE ?

sq.Between{"age": [2]interface{}{18, 65}}       // age BETWEEN ? AND ?
sq.NotBetween{"age": [2]interface{}{18, 65}}    // age NOT BETWEEN ? AND ?
```

Combine expressions with `And` / `Or` / `Not`:

```go
sq.And{sq.Gt{"age": 18}, sq.Eq{"active": true}}
// (age > ? AND active = ?)

sq.Or{sq.Eq{"col": 1}, sq.Eq{"col": 2}}
// (col = ? OR col = ?)

sq.Not{Cond: sq.Eq{"deleted": true}}
// NOT (deleted = ?)

// Compose Not with And/Or:
sq.And{sq.Eq{"active": true}, sq.Not{Cond: sq.Eq{"banned": true}}}
// (active = ? AND NOT (banned = ?))
```

Use `Exists` / `NotExists` for subquery existence checks:

```go
sub := sq.Select("1").From("orders").Where("orders.user_id = users.id")

sq.Select("*").From("users").Where(sq.Exists(sub))
// SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)

sq.Select("*").From("users").Where(sq.NotExists(sub))
// SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
```

Use `Expr` for arbitrary SQL fragments:

```go
sq.Expr("FROM_UNIXTIME(?)", ts)
```

Use `ConcatExpr` to build expressions by concatenating strings and other `Sqlizer` values:

```go
name_expr := sq.Expr("CONCAT(?, ' ', ?)", firstName, lastName)
sq.ConcatExpr("COALESCE(full_name,", name_expr, ")")
```

### Placeholder Formats

Four placeholder formats are built in:

```go
sq.Question // ?           (default — MySQL, SQLite)
sq.Dollar   // $1, $2, ... (PostgreSQL)
sq.Colon    // :1, :2, ... (Oracle)
sq.AtP      // @p1, @p2, ... (SQL Server)
```

Set a format on any builder or on `StatementBuilder`:

```go
psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
```

The `Placeholders` helper generates a comma-separated list of `?` markers:

```go
sq.Placeholders(3) // "?,?,?"
```

### SELECT Clauses

`SelectBuilder` supports the full range of SELECT clauses:

```go
sq.Select("department", "COUNT(*) as cnt").
Distinct().                          // SELECT DISTINCT ...
From("users").
Join("emails USING (email_id)").     // also LeftJoin, RightJoin, InnerJoin, CrossJoin, FullJoin
Where(sq.Gt{"age": 18}).
GroupBy("department").
Having("COUNT(*) > ?", 5).
OrderBy("cnt DESC").
Limit(10).
Offset(20).
ToSql()
// SELECT DISTINCT department, COUNT(*) as cnt FROM users
//   JOIN emails USING (email_id) WHERE age > ?
//   GROUP BY department HAVING COUNT(*) > ?
//   ORDER BY cnt DESC LIMIT ? OFFSET ?
// args: [18, 5, 10, 20]
```

`Limit` and `Offset` use parameterized placeholders (`LIMIT ?` / `OFFSET ?`) rather
than formatting values directly into the SQL string. This means the query string is
identical regardless of the limit/offset values, enabling prepared-statement caching
and reuse:

```go
// Both produce the same SQL: "SELECT * FROM users LIMIT ? OFFSET ?"
// Only the bound args differ.
page1 := sq.Select("*").From("users").Limit(10).Offset(0)
page2 := sq.Select("*").From("users").Limit(10).Offset(10)
```

`FullJoin` adds a `FULL OUTER JOIN` clause:

```go
sq.Select("*").From("a").FullJoin("b ON a.id = b.a_id")
// SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id
```

Use the `*JoinUsing` convenience methods for the common case where the join
condition is a simple column equality (`USING` clause):

```go
sq.Select("*").From("orders").JoinUsing("customers", "customer_id")
// SELECT * FROM orders JOIN customers USING (customer_id)

sq.Select("*").From("orders").LeftJoinUsing("customers", "customer_id", "region")
// SELECT * FROM orders LEFT JOIN customers USING (customer_id, region)

// All join types have a *JoinUsing variant:
// JoinUsing, LeftJoinUsing, RightJoinUsing, InnerJoinUsing, CrossJoinUsing, FullJoinUsing
```

### Structured Joins with `JoinExpr`

For more complex joins, `JoinExpr` provides a structured builder that avoids
raw SQL strings. Pass the result to `JoinClause`:

```go
// Basic ON condition — no raw string concatenation needed
sq.Select("items.name", "users.username").
From("items").
JoinClause(
sq.JoinExpr("users").On("items.fk_user_key = users.key"),
)
// SELECT items.name, users.username FROM items JOIN users ON items.fk_user_key = users.key
```

Chain multiple `.On()` calls — they are ANDed together:

```go
sq.Select("items.name", "users.username").
From("items").
JoinClause(
sq.JoinExpr("users").
On("items.fk_user_key = users.key").
On("users.username = ?", "alice"),
)
// ... JOIN users ON items.fk_user_key = users.key AND users.username = ?
```

Use `.OnExpr()` to compose with expression helpers like `Eq`, `Gt`, `Between`:

```go
sq.Select("*").From("items").JoinClause(
sq.JoinExpr("prices").
On("items.id = prices.item_id").
OnExpr(sq.Gt{"prices.amount": 100}),
)
// ... JOIN prices ON items.id = prices.item_id AND prices.amount > ?
```

Set the join type with `.Type()`, add an alias with `.As()`:

```go
sq.Select("i.name", "u.username").
From("items i").
JoinClause(
sq.JoinExpr("users").Type(sq.JoinLeft).As("u").
On("i.fk_user_key = u.key"),
)
// SELECT i.name, u.username FROM items i LEFT JOIN users u ON i.fk_user_key = u.key
```

Available join types: `sq.JoinInner` (default), `sq.JoinLeft`, `sq.JoinRight`,
`sq.JoinFull`, `sq.JoinCross`.

Use `.SubQuery()` to join against a subquery:

```go
sub := sq.Select("id", "name").From("users").Where(sq.Eq{"active": true})
sq.Select("items.name", "u.name").
From("items").
JoinClause(
sq.JoinExpr("").SubQuery(sub).As("u").
On("items.fk_user_key = u.id"),
)
// ... JOIN (SELECT id, name FROM users WHERE active = ?) u ON items.fk_user_key = u.id
```

Use `.Using()` for USING clauses:

```go
sq.Select("*").From("orders").JoinClause(
sq.JoinExpr("customers").Using("customer_id"),
)
// SELECT * FROM orders JOIN customers USING (customer_id)
```

`JoinExpr` is fully compatible with the existing string-based join methods —
you can mix both styles in the same query.

Remove clauses that were previously set:

```go
base := sq.Select("*").From("users").Limit(10).Offset(20)

// Remove limit and offset for a count query.
// RemoveLimit/RemoveOffset remove the parameterized LIMIT/OFFSET clauses entirely.
countQuery := base.RemoveColumns().RemoveLimit().RemoveOffset().
Column("COUNT(*)")
```

### Subqueries

Use `FromSelect` to nest a SELECT in the FROM clause:

```go
sub := sq.Select("id").From("other_table").Where(sq.Gt{"age": 18})
sql, args, err := sq.Select("*").FromSelect(sub, "subquery").ToSql()
// SELECT * FROM (SELECT id FROM other_table WHERE age > ?) AS subquery
```

Use a `SelectBuilder` as a value in `Eq` / `NotEq` for `WHERE ... IN (SELECT ...)`:

```go
sub := sq.Select("id").From("departments").Where(sq.Eq{"name": "Engineering"})
sql, args, err := sq.Select("*").From("employees").
Where(sq.Eq{"department_id": sub}).ToSql()
// SELECT * FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name = ?)
```

`NotEq` produces `NOT IN`:

```go
blocked := sq.Select("user_id").From("bans")
sq.Select("*").From("users").Where(sq.NotEq{"id": blocked})
// SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM bans)
```

Comparison operators (`Lt`, `Gt`, `LtOrEq`, `GtOrEq`) also accept subqueries for scalar comparisons:

```go
avgPrice := sq.Select("AVG(price)").From("products")
sq.Select("name").From("products").Where(sq.Gt{"price": avgPrice})
// SELECT name FROM products WHERE price > (SELECT AVG(price) FROM products)
```

Subqueries work correctly with all placeholder formats, including `Dollar` for PostgreSQL — placeholders are numbered sequentially across outer and inner queries.

### UNION / INTERSECT / EXCEPT

Combine multiple SELECT queries with set operations:

```go
q1 := sq.Select("name").From("employees")
q2 := sq.Select("name").From("contractors")

sql, args, err := sq.Union(q1, q2).ToSql()
// SELECT name FROM employees UNION SELECT name FROM contractors

sql, args, err = sq.UnionAll(q1, q2).ToSql()
// SELECT name FROM employees UNION ALL SELECT name FROM contractors

sql, args, err = sq.Intersect(q1, q2).ToSql()
// SELECT name FROM employees INTERSECT SELECT name FROM contractors

sql, args, err = sq.Except(q1, q2).ToSql()
// SELECT name FROM employees EXCEPT SELECT name FROM contractors
```

Chain additional set operations and add `ORDER BY`, `LIMIT`, `OFFSET` (all parameterized):

```go
q3 := sq.Select("name").From("interns")
sql, args, err := sq.Union(q1, q2).Union(q3).OrderBy("name").Limit(10).ToSql()
// ... ORDER BY name LIMIT ?
// args: [..., 10]
```

### Common Table Expressions (CTEs)

Build `WITH` / `WITH RECURSIVE` clauses using `CteBuilder`:

```go
activeSub := sq.Select("id", "name").From("users").Where(sq.Eq{"active": true})
sql, args, err := sq.With("active_users", activeSub).
Statement(sq.Select("*").From("active_users")).ToSql()
// WITH active_users AS (SELECT id, name FROM users WHERE active = ?)
//   SELECT * FROM active_users
```

Recursive CTEs:

```go
base := sq.Select("id", "parent_id").From("categories").Where(sq.Eq{"parent_id": nil})
recursive := sq.Select("c.id", "c.parent_id").From("categories c").
Join("tree t ON c.parent_id = t.id")

sql, args, err := sq.WithRecursive("tree", sq.Union(base, recursive)).
Statement(sq.Select("*").From("tree")).ToSql()
// WITH RECURSIVE tree AS (
//   SELECT id, parent_id FROM categories WHERE parent_id IS NULL
//   UNION SELECT c.id, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id
// ) SELECT * FROM tree
```

CTEs with explicit column lists:

```go
sq.WithColumns("cte", []string{"x", "y"}, sq.Select("a", "b").From("t1")).
Statement(sq.Select("x", "y").From("cte"))
// WITH cte (x, y) AS (SELECT a, b FROM t1) SELECT x, y FROM cte
```

The main `.Statement()` accepts any `Sqlizer` — SELECT, INSERT, UPDATE, DELETE, UNION, or even another CTE.

### Upsert — ON CONFLICT (PostgreSQL) / ON DUPLICATE KEY UPDATE (MySQL)

**PostgreSQL** — use `OnConflictColumns` (or `OnConflictOnConstraint`) with `OnConflictDoNothing` or `OnConflictDoUpdate`:

```go
// DO NOTHING
sq.Insert("users").Columns("id", "name").Values(1, "John").
OnConflictColumns("id").OnConflictDoNothing().ToSql()
// INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO NOTHING

// DO UPDATE SET
sq.Insert("users").Columns("id", "name").Values(1, "John").
OnConflictColumns("id").
OnConflictDoUpdate("name", sq.Expr("EXCLUDED.name")).ToSql()
// INSERT INTO users (id,name) VALUES (?,?)
//   ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name

// DO UPDATE with WHERE clause
sq.Insert("users").Columns("id", "name").Values(1, "John").
OnConflictColumns("id").
OnConflictDoUpdate("name", sq.Expr("EXCLUDED.name")).
OnConflictWhere(sq.Eq{"users.active": true}).ToSql()

// Named constraint
sq.Insert("users").Columns("id", "name").Values(1, "John").
OnConflictOnConstraint("users_pkey").OnConflictDoNothing().ToSql()

// Map convenience
sq.Insert("users").Columns("id", "name", "email").Values(1, "John", "j@x.com").
OnConflictColumns("id").
OnConflictDoUpdateMap(map[string]interface{}{
"name":  sq.Expr("EXCLUDED.name"),
"email": sq.Expr("EXCLUDED.email"),
}).ToSql()
```

**MySQL** — use `OnDuplicateKeyUpdate`:

```go
sq.Insert("users").Columns("id", "name").Values(1, "John").
OnDuplicateKeyUpdate("name", sq.Expr("VALUES(name)")).ToSql()
// INSERT INTO users (id,name) VALUES (?,?) ON DUPLICATE KEY UPDATE name = VALUES(name)
```

### RETURNING Clause

PostgreSQL, SQLite (3.35+), and MariaDB support `RETURNING`. Use the first-class
`Returning` method on `InsertBuilder`, `UpdateBuilder`, and `DeleteBuilder`:

```go
sq.Insert("users").Columns("name").Values("moe").Returning("id").ToSql()
// INSERT INTO users (name) VALUES (?) RETURNING id

sq.Update("users").Set("name", "moe").Where(sq.Eq{"id": 1}).
Returning("id", "name").ToSql()
// UPDATE users SET name = ? WHERE id = ? RETURNING id, name

sq.Delete("users").Where(sq.Eq{"id": 1}).Returning("*").ToSql()
// DELETE FROM users WHERE id = ? RETURNING *
```

`Returning` works correctly with `ON CONFLICT` — the `RETURNING` clause is emitted
after the conflict action:

```go
sq.Insert("users").Columns("id", "name").Values(1, "John").
OnConflictColumns("id").
OnConflictDoUpdate("name", sq.Expr("EXCLUDED.name")).
Returning("id", "name").ToSql()
// INSERT INTO users (id,name) VALUES (?,?)
//   ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
//   RETURNING id, name
```

### INSERT ... SELECT

Insert rows from a SELECT query instead of literal values:

```go
sub := sq.Select("name", "age").From("other_users").Where(sq.Gt{"age": 18})
sql, args, err := sq.Insert("users").Columns("name", "age").Select(sub).ToSql()
// INSERT INTO users (name,age) SELECT name, age FROM other_users WHERE age > ?
```

### SetMap

Set columns and values from a map (available on both `InsertBuilder` and `UpdateBuilder`):

```go
// Insert
sq.Insert("users").SetMap(map[string]interface{}{
"name": "moe",
"age":  13,
}).ToSql()
// INSERT INTO users (age,name) VALUES (?,?)   -- columns sorted alphabetically

// Update
sq.Update("users").SetMap(map[string]interface{}{
"name": "moe",
"age":  13,
}).Where(sq.Eq{"id": 1}).ToSql()
// UPDATE users SET age = ?, name = ? WHERE id = ?
```

### INSERT Options

Add keywords before the INTO clause (e.g. MySQL's `INSERT IGNORE`):

```go
sq.Insert("users").Options("IGNORE").Columns("name").Values("moe").ToSql()
// INSERT IGNORE INTO users (name) VALUES (?)
```

### UPDATE ... FROM (PostgreSQL)

Use `From` or `FromSelect` on an `UpdateBuilder` for PostgreSQL-style joins:

```go
sq.Update("users").Set("name", "moe").
From("accounts").
Where("users.account_id = accounts.id").
ToSql()
// UPDATE users SET name = ? FROM accounts WHERE users.account_id = accounts.id
```

### UPDATE ... JOIN (MySQL)

Use `Join`, `LeftJoin`, `InnerJoin`, etc. on an `UpdateBuilder` for MySQL-style joins.
The join clause is emitted between the table name and `SET`:

```go
sq.Update("orders").
    Join("customers ON orders.customer_id = customers.id").
    Set("orders.status", "verified").
    Where("customers.verified = ?", true).
    ToSql()
// UPDATE orders JOIN customers ON orders.customer_id = customers.id
//   SET orders.status = ? WHERE customers.verified = ?

// Multiple joins
sq.Update("t1").
    Join("t2 ON t1.id = t2.t1_id").
    LeftJoin("t3 ON t2.id = t3.t2_id AND t3.active = ?", true).
    Set("t1.name", "updated").
    Where("t1.id = ?", 1).
    ToSql()

// Structured JoinExpr works too
sq.Update("orders").
    JoinClause(
        sq.JoinExpr("customers").
            On("orders.customer_id = customers.id").
            On("customers.active = ?", true),
    ).
    Set("orders.status", "verified").
    ToSql()

// JoinUsing convenience
sq.Update("t1").JoinUsing("t2", "id").Set("t1.name", "updated").ToSql()
// UPDATE t1 JOIN t2 USING (id) SET t1.name = ?
```

All join types are available: `Join`, `LeftJoin`, `RightJoin`, `InnerJoin`,
`CrossJoin`, `FullJoin`, and their `*JoinUsing` variants.

### DELETE ... JOIN (MySQL) / DELETE ... USING (PostgreSQL)

**MySQL** — use `Join`, `LeftJoin`, etc. on a `DeleteBuilder`:

```go
sq.Delete("orders").
    Join("customers ON orders.customer_id = customers.id").
    Where("customers.active = ?", false).
    ToSql()
// DELETE orders FROM orders JOIN customers ON orders.customer_id = customers.id
//   WHERE customers.active = ?

// Structured JoinExpr works too
sq.Delete("orders").
    JoinClause(
        sq.JoinExpr("customers").
            On("orders.customer_id = customers.id").
            On("customers.active = ?", false),
    ).
    ToSql()
```

**PostgreSQL** — use `Using` for `DELETE ... USING` syntax:

```go
sq.Delete("orders").
    Using("customers").
    Where("orders.customer_id = customers.id AND customers.active = ?", false).
    PlaceholderFormat(sq.Dollar).
    ToSql()
// DELETE FROM orders USING customers
//   WHERE orders.customer_id = customers.id AND customers.active = $1
```

Multiple USING tables:

```go
sq.Delete("t1").
    Using("t2", "t3").
    Where("t1.id = t2.t1_id AND t2.t3_id = t3.id AND t3.active = ?", true).
    ToSql()
// DELETE FROM t1 USING t2, t3 WHERE ...
```

All join types are available on `DeleteBuilder`: `Join`, `LeftJoin`, `RightJoin`,
`InnerJoin`, `CrossJoin`, `FullJoin`, their `*JoinUsing` variants, and
`JoinClause` for structured `JoinExpr` builders.

### Prefix and Suffix

Add arbitrary SQL before or after the main statement:

```go
sq.Select("*").From("users").
Prefix("/* admin query */").
Suffix("FOR UPDATE").
Where(sq.Eq{"id": 1}).ToSql()
// /* admin query */ SELECT * FROM users WHERE id = ? FOR UPDATE
```

### Column Aliasing

Use `Alias` to wrap complex expressions with an `AS` alias:

```go
caseExpr := sq.Case().When(sq.Eq{"active": true}, "1").Else("0")
sq.Select("name").Column(sq.Alias(caseExpr, "is_active")).From("users")
```

### MustSql

All builders provide `MustSql()` which panics on error instead of returning it — useful in tests:

```go
sql, args := sq.Select("*").From("users").MustSql()
```

### Context-Aware Execution

All builders support context-aware variants for query execution:

```go
rows, err := sq.Select("*").From("users").
RunWith(db).
QueryContext(ctx)

result, err := sq.Update("users").Set("name", "moe").
Where(sq.Eq{"id": 1}).
RunWith(db).
ExecContext(ctx)
```

### Debugging

`DebugSqlizer` inlines arguments into the SQL string for display purposes.
**Never execute the output** — it is not safe against SQL injection:

```go
fmt.Println(sq.DebugSqlizer(
sq.Select("*").From("users").Where(sq.Eq{"name": "moe"}),
))
// SELECT * FROM users WHERE name = 'moe'
```

### Safe Identifiers — Preventing SQL Injection in Table & Column Names

Methods like `From()`, `Into()`, `Table()`, `Columns()`, `Set()`, `Join()`,
`OrderBy()`, and `GroupBy()` interpolate strings directly into SQL **without
sanitization**. If any of these strings come from user input (e.g., a dynamic
sort column from an API query parameter), your application is vulnerable to SQL
injection.

Squirrel provides the `Ident` type and two helper functions to safely handle
dynamic identifiers:

**`QuoteIdent`** — ANSI SQL double-quote escaping (maximum flexibility):

```go
// Safely quote any user-supplied identifier — even malicious input.
table, err := sq.QuoteIdent(userInput) // e.g. "users" → `"users"`
if err != nil { /* handle error */ }

sql, args, err := sq.Select("*").SafeFrom(table).ToSQL()
// SELECT * FROM "users"

// Injection attempt is safely neutralized:
table, _ := sq.QuoteIdent("users; DROP TABLE users; --")
sq.Select("*").SafeFrom(table).ToSQL()
// SELECT * FROM "users; DROP TABLE users; --"   ← treated as a single identifier
```

**`ValidateIdent`** — strict pattern validation (maximum strictness):

```go
// Only allows letters, digits, underscores, and dots.
// Rejects anything that doesn't look like a simple identifier.
col, err := sq.ValidateIdent(userSortColumn)
if err != nil {
// Reject the request — input contains invalid characters.
return err
}

sql, args, err := sq.Select("*").From("users").SafeOrderByDir(col, sq.Desc).ToSQL()
// SELECT * FROM users ORDER BY user_name DESC
```

**Safe builder methods** accept `Ident` values instead of raw strings:

```go
// SELECT
table, _ := sq.QuoteIdent("users")
cols, _ := sq.QuoteIdents("id", "name", "email")
orderCol, _ := sq.QuoteIdent("name")
groupCol, _ := sq.QuoteIdent("department")

sq.Select().SafeColumns(cols...).SafeFrom(table).
SafeGroupBy(groupCol).
SafeOrderByDir(orderCol, sq.Desc).
ToSQL()
// SELECT "id", "name", "email" FROM "users" GROUP BY "department" ORDER BY "name" DESC

// INSERT
table, _ := sq.QuoteIdent("users")
cols, _ := sq.QuoteIdents("id", "name")
sq.Insert("").SafeInto(table).SafeColumns(cols...).Values(1, "moe").ToSQL()
// INSERT INTO "users" ("id","name") VALUES (?,?)

// UPDATE
table, _ := sq.QuoteIdent("users")
col, _ := sq.QuoteIdent("name")
sq.Update("").SafeTable(table).SafeSet(col, "moe").Where("id = ?", 1).ToSQL()
// UPDATE "users" SET "name" = ? WHERE id = ?

// DELETE
table, _ := sq.QuoteIdent("users")
sq.Delete("").SafeFrom(table).Where("id = ?", 1).ToSQL()
// DELETE FROM "users" WHERE id = ?
```

**Batch helpers** quote or validate multiple identifiers at once:

```go
ids, err := sq.QuoteIdents("id", "name", "email")   // quote all
ids, err := sq.ValidateIdents("id", "name", "email") // validate all
```

**Panic variants** for use with known-safe literals in application code:

```go
table := sq.MustQuoteIdent("users")       // panics on error
col := sq.MustValidateIdent("created_at") // panics on error
```

The `Ident` type also implements `Sqlizer`, so it can be used anywhere a
`Sqlizer` is accepted.

**Summary of Safe methods:**

| Builder | Safe Method | Replaces |
|---------|-------------|----------|
| `SelectBuilder` | `SafeFrom(Ident)` | `From(string)` |
| `SelectBuilder` | `SafeColumns(...Ident)` | `Columns(...string)` |
| `SelectBuilder` | `SafeGroupBy(...Ident)` | `GroupBy(...string)` |
| `SelectBuilder` | `SafeOrderBy(...Ident)` | `OrderBy(...string)` |
| `SelectBuilder` | `SafeOrderByDir(Ident, OrderDir)` | `OrderBy("col DESC")` |
| `InsertBuilder` | `SafeInto(Ident)` | `Into(string)` |
| `InsertBuilder` | `SafeColumns(...Ident)` | `Columns(...string)` |
| `UpdateBuilder` | `SafeTable(Ident)` | `Table(string)` |
| `UpdateBuilder` | `SafeSet(Ident, any)` | `Set(string, any)` |
| `DeleteBuilder` | `SafeFrom(Ident)` | `From(string)` |

## License

Squirrel is released under the
[MIT License](http://www.opensource.org/licenses/MIT).
