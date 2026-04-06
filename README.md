# Squirrel - fluent SQL generator for Go

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
rows, err := db.Query("SELECT * FROM users WHERE username IN (?,?,?,?) LIMIT 3",
                      "moe", "larry", "curly", "shemp")
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
```

Combine expressions with `And` / `Or`:

```go
sq.And{sq.Gt{"age": 18}, sq.Eq{"active": true}}
// (age > ? AND active = ?)

sq.Or{sq.Eq{"col": 1}, sq.Eq{"col": 2}}
// (col = ? OR col = ?)
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
    Join("emails USING (email_id)").     // also LeftJoin, RightJoin, InnerJoin, CrossJoin
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
//   ORDER BY cnt DESC LIMIT 10 OFFSET 20
```

Remove clauses that were previously set:

```go
base := sq.Select("*").From("users").Limit(10).Offset(20)

// Remove limit and offset for a count query
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

## License

Squirrel is released under the
[MIT License](http://www.opensource.org/licenses/MIT).
