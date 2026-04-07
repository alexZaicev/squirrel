package squirrel_test

import (
	"fmt"

	sq "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

func ExampleSelect() {
	sql, args, err := sq.Select("id", "created", "first_name").From("users").ToSQL()
	if err != nil {
		panic(err)
	}
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, created, first_name FROM users
	// []
}

func ExampleSelectBuilder_From() {
	sql, _, _ := sq.Select("id", "created", "first_name").From("users").ToSQL()
	fmt.Println(sql)
	// Output: SELECT id, created, first_name FROM users
}

func ExampleSelectBuilder_Where() {
	sql, args, _ := sq.Select("id", "created", "first_name").
		From("users").
		Where("company = ?", 20).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, created, first_name FROM users WHERE company = ?
	// [20]
}

func ExampleSelectBuilder_Where_helpers() {
	sql, args, _ := sq.Select("id", "created", "first_name").
		From("users").
		Where(sq.Eq{"company": 20}).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, created, first_name FROM users WHERE company = ?
	// [20]
}

func ExampleSelectBuilder_Where_multiple() {
	sql, args, _ := sq.Select("id", "created", "first_name").
		From("users").
		Where("company = ?", 20).
		Where(sq.Gt{"created": 0}).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, created, first_name FROM users WHERE company = ? AND created > ?
	// [20 0]
}

func ExampleSelectBuilder_Columns() {
	query := sq.Select("id").Columns("created", "first_name").From("users")
	sql, _, _ := query.ToSQL()
	fmt.Println(sql)
	// Output: SELECT id, created, first_name FROM users
}

func ExampleSelectBuilder_Columns_order() {
	// out of order is ok too
	query := sq.Select("id").Columns("created").From("users").Columns("first_name")
	sql, _, _ := query.ToSQL()
	fmt.Println(sql)
	// Output: SELECT id, created, first_name FROM users
}

func ExampleSelectBuilder_Distinct() {
	sql, _, _ := sq.Select("country").Distinct().From("users").ToSQL()
	fmt.Println(sql)
	// Output: SELECT DISTINCT country FROM users
}

func ExampleSelectBuilder_Distinct_idempotent() {
	sql, _, _ := sq.Select("country").Distinct().Distinct().From("users").ToSQL()
	fmt.Println(sql)
	// Output: SELECT DISTINCT country FROM users
}

func ExampleSelectBuilder_FromSelect() {
	usersByCompany := sq.Select("company", "count(*) as n_users").From("users").GroupBy("company")
	query := sq.Select("company.id", "company.name", "users_by_company.n_users").
		FromSelect(usersByCompany, "users_by_company").
		Join("company on company.id = users_by_company.company")

	sql, _, _ := query.ToSQL()
	fmt.Println(sql)
	//nolint:lll // Output line must match actual single-line output
	// Output: SELECT company.id, company.name, users_by_company.n_users FROM (SELECT company, count(*) as n_users FROM users GROUP BY company) AS users_by_company JOIN company on company.id = users_by_company.company
}

func ExampleSelectBuilder_Join() {
	sql, _, _ := sq.Select("u.id", "u.name", "o.total").
		From("users u").
		Join("orders o ON o.user_id = u.id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT u.id, u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id
}

func ExampleSelectBuilder_LeftJoin() {
	sql, _, _ := sq.Select("u.id", "u.name", "o.total").
		From("users u").
		LeftJoin("orders o ON o.user_id = u.id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT u.id, u.name, o.total FROM users u LEFT JOIN orders o ON o.user_id = u.id
}

func ExampleSelectBuilder_FullJoin() {
	sql, _, _ := sq.Select("u.id", "d.name").
		From("users u").
		FullJoin("departments d ON u.dept_id = d.id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT u.id, d.name FROM users u FULL OUTER JOIN departments d ON u.dept_id = d.id
}

func ExampleSelectBuilder_JoinUsing() {
	sql, _, _ := sq.Select("orders.id", "customers.name").
		From("orders").
		JoinUsing("customers", "customer_id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT orders.id, customers.name FROM orders JOIN customers USING (customer_id)
}

func ExampleSelectBuilder_JoinUsing_multipleColumns() {
	sql, _, _ := sq.Select("*").
		From("orders").
		JoinUsing("shipments", "region_id", "order_id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM orders JOIN shipments USING (region_id, order_id)
}

func ExampleSelectBuilder_LeftJoinUsing() {
	sql, _, _ := sq.Select("orders.id", "returns.reason").
		From("orders").
		LeftJoinUsing("returns", "order_id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT orders.id, returns.reason FROM orders LEFT JOIN returns USING (order_id)
}

func ExampleSelectBuilder_RightJoinUsing() {
	sql, _, _ := sq.Select("orders.id", "products.name").
		From("orders").
		RightJoinUsing("products", "product_id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT orders.id, products.name FROM orders RIGHT JOIN products USING (product_id)
}

func ExampleSelectBuilder_InnerJoinUsing() {
	sql, _, _ := sq.Select("e.name", "d.name").
		From("employees e").
		InnerJoinUsing("departments d", "dept_id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT e.name, d.name FROM employees e INNER JOIN departments d USING (dept_id)
}

func ExampleSelectBuilder_CrossJoinUsing() {
	sql, _, _ := sq.Select("a.x", "b.y").
		From("a").
		CrossJoinUsing("b", "id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT a.x, b.y FROM a CROSS JOIN b USING (id)
}

func ExampleSelectBuilder_FullJoinUsing() {
	sql, _, _ := sq.Select("o.id", "r.region_name").
		From("orders o").
		FullJoinUsing("regions r", "region_id").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT o.id, r.region_name FROM orders o FULL OUTER JOIN regions r USING (region_id)
}

func ExampleJoinExpr() {
	sql, _, _ := sq.Select("items.name", "users.username").
		From("items").
		JoinClause(
			sq.JoinExpr("users").On("items.fk_user_key = users.key"),
		).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT items.name, users.username FROM items JOIN users ON items.fk_user_key = users.key
}

func ExampleJoinExpr_multipleConditions() {
	sql, args, _ := sq.Select("items.name", "users.username").
		From("items").
		JoinClause(
			sq.JoinExpr("users").
				On("items.fk_user_key = users.key").
				On("users.username = ?", "alice"),
		).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT items.name, users.username FROM items JOIN users ON items.fk_user_key = users.key AND users.username = ?
	// [alice]
}

func ExampleJoinExpr_leftJoinAlias() {
	sql, _, _ := sq.Select("i.name", "u.username").
		From("items i").
		JoinClause(
			sq.JoinExpr("users").Type(sq.JoinLeft).As("u").
				On("i.fk_user_key = u.key"),
		).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT i.name, u.username FROM items i LEFT JOIN users u ON i.fk_user_key = u.key
}

func ExampleJoinExpr_using() {
	sql, _, _ := sq.Select("*").
		From("orders").
		JoinClause(sq.JoinExpr("customers").Using("customer_id")).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM orders JOIN customers USING (customer_id)
}

func ExampleJoinExpr_subQuery() {
	sub := sq.Select("id", "name").From("users").Where(sq.Eq{"active": true})
	sql, args, _ := sq.Select("items.name", "u.name").
		From("items").
		JoinClause(
			sq.JoinExpr("").SubQuery(sub).As("u").
				On("items.fk_user_key = u.id"),
		).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT items.name, u.name FROM items JOIN (SELECT id, name FROM users WHERE active = ?) u ON items.fk_user_key = u.id
	// [true]
}

func ExampleSelectBuilder_GroupBy() {
	sql, _, _ := sq.Select("department", "count(*) as cnt").
		From("employees").
		GroupBy("department").
		Having("count(*) > 5").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT department, count(*) as cnt FROM employees GROUP BY department HAVING count(*) > 5
}

func ExampleSelectBuilder_OrderBy() {
	sql, _, _ := sq.Select("id", "name").
		From("users").
		OrderBy("name ASC", "id DESC").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT id, name FROM users ORDER BY name ASC, id DESC
}

func ExampleSelectBuilder_Limit() {
	sql, args, _ := sq.Select("id", "name").
		From("users").
		Limit(10).
		Offset(20).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, name FROM users LIMIT ? OFFSET ?
	// [10 20]
}

func ExampleSelectBuilder_Limit_dollar() {
	// Parameterized LIMIT/OFFSET works with all placeholder formats,
	// enabling prepared-statement reuse across different page sizes.
	sql, args, _ := sq.Select("id", "name").
		From("users").
		Where("active = ?", true).
		Limit(10).
		Offset(20).
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, name FROM users WHERE active = $1 LIMIT $2 OFFSET $3
	// [true 10 20]
}

func ExampleSelectBuilder_RemoveColumns() {
	query := sq.Select("id").
		From("users").
		RemoveColumns().
		Columns("name")
	sql, _, _ := query.ToSQL()
	fmt.Println(sql)
	// Output: SELECT name FROM users
}

func ExampleSelectBuilder_PlaceholderFormat() {
	sql, _, _ := sq.Select("id", "name").
		From("users").
		Where("id = ?", 1).
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT id, name FROM users WHERE id = $1
}

func ExampleSelectBuilder_Prefix() {
	sql, args, _ := sq.Select("*").
		Prefix("WITH cte AS (?)", 0).
		From("cte").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// WITH cte AS (?) SELECT * FROM cte
	// [0]
}

func ExampleSelectBuilder_Suffix() {
	sql, _, _ := sq.Select("id").
		From("users").
		Suffix("FOR UPDATE").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT id FROM users FOR UPDATE
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

func ExampleInsert() {
	sql, args, _ := sq.Insert("users").
		Columns("name", "age").
		Values("Alice", 30).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (name,age) VALUES (?,?)
	// [Alice 30]
}

func ExampleInsertBuilder_Values_multiple() {
	sql, args, _ := sq.Insert("users").
		Columns("name", "age").
		Values("Alice", 30).
		Values("Bob", 25).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (name,age) VALUES (?,?),(?,?)
	// [Alice 30 Bob 25]
}

func ExampleInsertBuilder_SetMap() {
	sql, args, _ := sq.Insert("users").
		SetMap(map[string]interface{}{
			"age":  30,
			"name": "Alice",
		}).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (age,name) VALUES (?,?)
	// [30 Alice]
}

func ExampleInsertBuilder_Select() {
	sql, args, _ := sq.Insert("user_archive").
		Columns("id", "name").
		Select(
			sq.Select("id", "name").From("users").Where(sq.Eq{"active": false}),
		).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO user_archive (id,name) SELECT id, name FROM users WHERE active = ?
	// [false]
}

func ExampleInsertBuilder_Options() {
	sql, args, _ := sq.Insert("users").
		Options("IGNORE").
		Columns("name").
		Values("Alice").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT IGNORE INTO users (name) VALUES (?)
	// [Alice]
}

func ExampleInsertBuilder_Returning() {
	sql, args, _ := sq.Insert("users").
		Columns("name").
		Values("Alice").
		Returning("id", "created_at").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (name) VALUES (?) RETURNING id, created_at
	// [Alice]
}

func ExampleInsertBuilder_OnConflictDoNothing() {
	sql, args, _ := sq.Insert("users").
		Columns("id", "name").
		Values(1, "Alice").
		OnConflictColumns("id").
		OnConflictDoNothing().
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING
	// [1 Alice]
}

func ExampleInsertBuilder_OnConflictDoUpdate() {
	sql, args, _ := sq.Insert("users").
		Columns("id", "name").
		Values(1, "Alice").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sq.Expr("EXCLUDED.name")).
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
	// [1 Alice]
}

func ExampleInsertBuilder_OnConflictOnConstraint() {
	sql, args, _ := sq.Insert("users").
		Columns("id", "name").
		Values(1, "Alice").
		OnConflictOnConstraint("users_pkey").
		OnConflictDoNothing().
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (id,name) VALUES ($1,$2) ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING
	// [1 Alice]
}

func ExampleInsertBuilder_OnConflictWhere() {
	sql, args, _ := sq.Insert("users").
		Columns("id", "name").
		Values(1, "Alice").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sq.Expr("EXCLUDED.name")).
		OnConflictWhere(sq.Eq{"users.active": true}).
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.active = $3
	// [1 Alice true]
}

func ExampleInsertBuilder_OnDuplicateKeyUpdate() {
	sql, args, _ := sq.Insert("users").
		Columns("id", "name").
		Values(1, "Alice").
		OnDuplicateKeyUpdate("name", "Alice").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (id,name) VALUES (?,?) ON DUPLICATE KEY UPDATE name = ?
	// [1 Alice Alice]
}

func ExampleReplace() {
	sql, args, _ := sq.Replace("users").
		Columns("name").
		Values("Alice").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// REPLACE INTO users (name) VALUES (?)
	// [Alice]
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

func ExampleUpdate() {
	sql, args, _ := sq.Update("users").
		Set("name", "Alice").
		Set("age", 30).
		Where("id = ?", 1).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE users SET name = ?, age = ? WHERE id = ?
	// [Alice 30 1]
}

func ExampleUpdateBuilder_SetMap() {
	sql, args, _ := sq.Update("users").
		SetMap(map[string]interface{}{
			"age":  30,
			"name": "Alice",
		}).
		Where("id = ?", 1).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE users SET age = ?, name = ? WHERE id = ?
	// [30 Alice 1]
}

func ExampleUpdateBuilder_From() {
	sql, args, _ := sq.Update("users").
		Set("status", "active").
		From("accounts").
		Where("users.account_id = accounts.id AND accounts.verified = ?", true).
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE users SET status = $1 FROM accounts WHERE users.account_id = accounts.id AND accounts.verified = $2
	// [active true]
}

func ExampleUpdateBuilder_OrderBy() {
	sql, args, _ := sq.Update("users").
		Set("name", "Alice").
		OrderBy("id").
		Limit(10).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE users SET name = ? ORDER BY id LIMIT ?
	// [Alice 10]
}

func ExampleUpdateBuilder_Returning() {
	sql, args, _ := sq.Update("users").
		Set("name", "Alice").
		Where("id = ?", 1).
		Returning("id", "name").
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE users SET name = $1 WHERE id = $2 RETURNING id, name
	// [Alice 1]
}

func ExampleUpdateBuilder_Join() {
	sql, args, _ := sq.Update("orders").
		Join("customers ON orders.customer_id = customers.id").
		Set("orders.status", "verified").
		Where("customers.verified = ?", true).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE orders JOIN customers ON orders.customer_id = customers.id SET orders.status = ? WHERE customers.verified = ?
	// [verified true]
}

func ExampleUpdateBuilder_LeftJoin() {
	sql, args, _ := sq.Update("items").
		LeftJoin("inventory ON items.id = inventory.item_id").
		Set("items.in_stock", false).
		Where("inventory.item_id IS NULL").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE items LEFT JOIN inventory ON items.id = inventory.item_id SET items.in_stock = ? WHERE inventory.item_id IS NULL
	// [false]
}

func ExampleUpdateBuilder_JoinClause() {
	sql, args, _ := sq.Update("orders").
		JoinClause(
			sq.JoinExpr("customers").
				Type(sq.JoinInner).
				On("orders.customer_id = customers.id").
				On("customers.active = ?", true),
		).
		Set("orders.status", "verified").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE orders JOIN customers ON orders.customer_id = customers.id AND customers.active = ? SET orders.status = ?
	// [true verified]
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

func ExampleDelete() {
	sql, args, _ := sq.Delete("users").
		Where("id = ?", 1).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// DELETE FROM users WHERE id = ?
	// [1]
}

func ExampleDeleteBuilder_OrderBy() {
	sql, args, _ := sq.Delete("logs").
		Where("created < ?", "2024-01-01").
		OrderBy("created").
		Limit(1000).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// DELETE FROM logs WHERE created < ? ORDER BY created LIMIT ?
	// [2024-01-01 1000]
}

func ExampleDeleteBuilder_Returning() {
	sql, args, _ := sq.Delete("users").
		Where("active = ?", false).
		Returning("id", "name").
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// DELETE FROM users WHERE active = $1 RETURNING id, name
	// [false]
}

func ExampleDeleteBuilder_Join() {
	sql, args, _ := sq.Delete("orders").
		Join("customers ON orders.customer_id = customers.id").
		Where("customers.active = ?", false).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// DELETE orders FROM orders JOIN customers ON orders.customer_id = customers.id WHERE customers.active = ?
	// [false]
}

func ExampleDeleteBuilder_Using() {
	sql, args, _ := sq.Delete("orders").
		Using("customers").
		Where("orders.customer_id = customers.id AND customers.active = ?", false).
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// DELETE FROM orders USING customers WHERE orders.customer_id = customers.id AND customers.active = $1
	// [false]
}

func ExampleDeleteBuilder_JoinClause() {
	sql, args, _ := sq.Delete("orders").
		JoinClause(
			sq.JoinExpr("customers").
				On("orders.customer_id = customers.id").
				On("customers.active = ?", false),
		).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// DELETE orders FROM orders JOIN customers ON orders.customer_id = customers.id AND customers.active = ?
	// [false]
}

// ---------------------------------------------------------------------------
// CASE
// ---------------------------------------------------------------------------

func ExampleCase() {
	caseStmt := sq.Case("status").
		When("1", "'active'").
		When("2", "'inactive'").
		Else(sq.Expr("?", "unknown"))

	sql, args, _ := sq.Select().
		Column(caseStmt).
		From("users").
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE ? END FROM users
	// [unknown]
}

func ExampleCase_searched() {
	// Searched CASE (no value after CASE)
	caseStmt := sq.Case().
		When("age < 18", "'minor'").
		When("age >= 18", "'adult'")

	sql, _, _ := sq.Select().
		Column(caseStmt).
		From("users").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT CASE WHEN age < 18 THEN 'minor' WHEN age >= 18 THEN 'adult' END FROM users
}

func ExampleCase_alias() {
	caseStmt := sq.Case("status").
		When("1", "'active'").
		When("2", "'inactive'")

	sql, _, _ := sq.Select().
		Column(sq.Alias(caseStmt, "status_text")).
		From("users").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT (CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' END) AS status_text FROM users
}

// ---------------------------------------------------------------------------
// UNION / INTERSECT / EXCEPT
// ---------------------------------------------------------------------------

func ExampleUnion() {
	q1 := sq.Select("id", "name").From("users").Where(sq.Eq{"active": true})
	q2 := sq.Select("id", "name").From("admins").Where(sq.Eq{"active": true})

	sql, args, _ := sq.Union(q1, q2).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, name FROM users WHERE active = ? UNION SELECT id, name FROM admins WHERE active = ?
	// [true true]
}

func ExampleUnionAll() {
	q1 := sq.Select("id").From("t1")
	q2 := sq.Select("id").From("t2")

	sql, _, _ := sq.UnionAll(q1, q2).ToSQL()
	fmt.Println(sql)
	// Output: SELECT id FROM t1 UNION ALL SELECT id FROM t2
}

func ExampleIntersect() {
	q1 := sq.Select("id").From("t1")
	q2 := sq.Select("id").From("t2")

	sql, _, _ := sq.Intersect(q1, q2).ToSQL()
	fmt.Println(sql)
	// Output: SELECT id FROM t1 INTERSECT SELECT id FROM t2
}

func ExampleExcept() {
	q1 := sq.Select("id").From("t1")
	q2 := sq.Select("id").From("t2")

	sql, _, _ := sq.Except(q1, q2).ToSQL()
	fmt.Println(sql)
	// Output: SELECT id FROM t1 EXCEPT SELECT id FROM t2
}

func ExampleUnionBuilder_OrderBy() {
	q1 := sq.Select("id", "name").From("t1")
	q2 := sq.Select("id", "name").From("t2")

	sql, args, _ := sq.Union(q1, q2).
		OrderBy("name").
		Limit(10).
		Offset(5).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY name LIMIT ? OFFSET ?
	// [10 5]
}

// ---------------------------------------------------------------------------
// CTE (WITH)
// ---------------------------------------------------------------------------

func ExampleWith() {
	sql, args, _ := sq.With("active_users",
		sq.Select("id", "name").From("users").Where(sq.Eq{"active": true}),
	).Statement(
		sq.Select("*").From("active_users"),
	).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// WITH active_users AS (SELECT id, name FROM users WHERE active = ?) SELECT * FROM active_users
	// [true]
}

func ExampleWith_multipleCTEs() {
	sql, args, _ := sq.With("cte1",
		sq.Select("id").From("t1").Where(sq.Eq{"a": 1}),
	).With("cte2",
		sq.Select("name").From("t2").Where(sq.Eq{"b": 2}),
	).Statement(
		sq.Select("*").From("cte1").Join("cte2 ON cte1.id = cte2.id"),
	).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// WITH cte1 AS (SELECT id FROM t1 WHERE a = ?), cte2 AS (SELECT name FROM t2 WHERE b = ?) SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id
	// [1 2]
}

func ExampleWithRecursive() {
	sql, args, _ := sq.WithRecursive("numbers",
		sq.Union(
			sq.Select("1 as n"),
			sq.Select("n + 1").From("numbers").Where("n < ?", 10),
		),
	).Statement(
		sq.Select("n").From("numbers"),
	).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// WITH RECURSIVE numbers AS (SELECT 1 as n UNION SELECT n + 1 FROM numbers WHERE n < ?) SELECT n FROM numbers
	// [10]
}

func ExampleWithColumns() {
	sql, _, _ := sq.WithColumns("cte", []string{"x", "y"},
		sq.Select("a", "b").From("t1"),
	).Statement(
		sq.Select("x", "y").From("cte"),
	).ToSQL()
	fmt.Println(sql)
	// Output: WITH cte (x, y) AS (SELECT a, b FROM t1) SELECT x, y FROM cte
}

func ExampleWithRecursiveColumns() {
	sql, args, _ := sq.WithRecursiveColumns("cnt", []string{"x"},
		sq.Union(
			sq.Select("1"),
			sq.Select("x + 1").From("cnt").Where("x < ?", 100),
		),
	).Statement(
		sq.Select("x").From("cnt"),
	).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// WITH RECURSIVE cnt (x) AS (SELECT 1 UNION SELECT x + 1 FROM cnt WHERE x < ?) SELECT x FROM cnt
	// [100]
}

// ---------------------------------------------------------------------------
// Expressions: Eq, NotEq, Lt, Gt, LtOrEq, GtOrEq
// ---------------------------------------------------------------------------

func ExampleEq() {
	sql, args, _ := sq.Eq{"company": 20}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// company = ?
	// [20]
}

func ExampleEq_in() {
	sql, args, _ := sq.Eq{"status": []string{"active", "pending"}}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// status IN (?,?)
	// [active pending]
}

func ExampleEq_null() {
	sql, _, _ := sq.Eq{"deleted_at": nil}.ToSQL()
	fmt.Println(sql)
	// Output: deleted_at IS NULL
}

func ExampleEq_subquery() {
	subQ := sq.Select("id").From("other_table").Where(sq.Eq{"active": true})
	sql, args, _ := sq.Eq{"id": subQ}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// id IN (SELECT id FROM other_table WHERE active = ?)
	// [true]
}

func ExampleNotEq() {
	sql, args, _ := sq.NotEq{"status": "deleted"}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// status <> ?
	// [deleted]
}

func ExampleLt() {
	sql, args, _ := sq.Lt{"age": 18}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// age < ?
	// [18]
}

func ExampleGt() {
	sql, args, _ := sq.Gt{"age": 65}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// age > ?
	// [65]
}

func ExampleLtOrEq() {
	sql, args, _ := sq.LtOrEq{"age": 65}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// age <= ?
	// [65]
}

func ExampleGtOrEq() {
	sql, args, _ := sq.GtOrEq{"age": 18}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// age >= ?
	// [18]
}

// ---------------------------------------------------------------------------
// Expressions: Between, NotBetween
// ---------------------------------------------------------------------------

func ExampleBetween() {
	sql, args, _ := sq.Between{"age": [2]interface{}{18, 65}}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// age BETWEEN ? AND ?
	// [18 65]
}

func ExampleNotBetween() {
	sql, args, _ := sq.NotBetween{"age": [2]interface{}{18, 65}}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// age NOT BETWEEN ? AND ?
	// [18 65]
}

// ---------------------------------------------------------------------------
// Expressions: Like, NotLike, ILike, NotILike
// ---------------------------------------------------------------------------

func ExampleLike() {
	sql, args, _ := sq.Like{"name": "%irrel"}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// name LIKE ?
	// [%irrel]
}

func ExampleNotLike() {
	sql, args, _ := sq.NotLike{"name": "%test%"}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// name NOT LIKE ?
	// [%test%]
}

func ExampleILike() {
	sql, args, _ := sq.ILike{"name": "sq%"}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// name ILIKE ?
	// [sq%]
}

func ExampleNotILike() {
	sql, args, _ := sq.NotILike{"name": "sq%"}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// name NOT ILIKE ?
	// [sq%]
}

// ---------------------------------------------------------------------------
// Expressions: And, Or, Not
// ---------------------------------------------------------------------------

func ExampleAnd() {
	sql, args, _ := sq.And{
		sq.Eq{"company": 20},
		sq.Gt{"age": 18},
	}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// (company = ? AND age > ?)
	// [20 18]
}

func ExampleOr() {
	sql, args, _ := sq.Or{
		sq.Eq{"status": "active"},
		sq.Eq{"status": "pending"},
	}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// (status = ? OR status = ?)
	// [active pending]
}

func ExampleNot() {
	sql, args, _ := sq.Not{Cond: sq.Eq{"active": true}}.ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// NOT (active = ?)
	// [true]
}

// ---------------------------------------------------------------------------
// Expressions: Exists, NotExists
// ---------------------------------------------------------------------------

func ExampleExists() {
	sub := sq.Select("1").From("orders").Where("orders.user_id = users.id")
	sql, _, _ := sq.Select("*").
		From("users").
		Where(sq.Exists(sub)).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
}

func ExampleNotExists() {
	sub := sq.Select("1").From("orders").Where("orders.user_id = users.id")
	sql, _, _ := sq.Select("*").
		From("users").
		Where(sq.NotExists(sub)).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
}

// ---------------------------------------------------------------------------
// Expressions: Expr, ConcatExpr, Alias
// ---------------------------------------------------------------------------

func ExampleExpr() {
	sql, args, _ := sq.Expr("COUNT(*) > ?", 5).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// COUNT(*) > ?
	// [5]
}

func ExampleConcatExpr() {
	nameExpr := sq.Expr("CONCAT(?, ' ', ?)", "first", "last")
	sql, args, _ := sq.ConcatExpr("COALESCE(full_name, ", nameExpr, ")").ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// COALESCE(full_name, CONCAT(?, ' ', ?))
	// [first last]
}

func ExampleAlias() {
	caseStmt := sq.Case("status").
		When("1", "'active'").
		When("2", "'inactive'")
	sql, _, _ := sq.Select().
		Column(sq.Alias(caseStmt, "status_text")).
		From("users").
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT (CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' END) AS status_text FROM users
}

// ---------------------------------------------------------------------------
// Placeholder formats
// ---------------------------------------------------------------------------

func ExampleDollar() {
	sql, _, _ := sq.Select("id").
		From("users").
		Where("id = ?", 1).
		PlaceholderFormat(sq.Dollar).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT id FROM users WHERE id = $1
}

func ExampleColon() {
	sql, _, _ := sq.Select("id").
		From("users").
		Where("id = ?", 1).
		PlaceholderFormat(sq.Colon).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT id FROM users WHERE id = :1
}

func ExampleAtP() {
	sql, _, _ := sq.Select("id").
		From("users").
		Where("id = ?", 1).
		PlaceholderFormat(sq.AtP).
		ToSQL()
	fmt.Println(sql)
	// Output: SELECT id FROM users WHERE id = @p1
}

func ExamplePlaceholders() {
	p := sq.Placeholders(3)
	fmt.Println(p)
	// Output: ?,?,?
}

// ---------------------------------------------------------------------------
// Safe Identifiers
// ---------------------------------------------------------------------------

func ExampleQuoteIdent() {
	id, _ := sq.QuoteIdent("users")
	fmt.Println(id.String())
	fmt.Println(id.Raw())
	// Output:
	// "users"
	// users
}

func ExampleQuoteIdent_schemaQualified() {
	id, _ := sq.QuoteIdent("public.users")
	fmt.Println(id.String())
	// Output: "public"."users"
}

func ExampleQuoteIdent_injectionSafe() {
	// Even malicious input is safely wrapped in quotes.
	id, _ := sq.QuoteIdent("users; DROP TABLE users; --")
	fmt.Println(id.String())
	// Output: "users; DROP TABLE users; --"
}

func ExampleValidateIdent() {
	id, err := sq.ValidateIdent("users")
	if err != nil {
		panic(err)
	}
	fmt.Println(id.String())
	// Output: users
}

func ExampleValidateIdent_rejected() {
	_, err := sq.ValidateIdent("users; DROP TABLE users; --")
	fmt.Println(err)
	// Output: invalid SQL identifier: "users; DROP TABLE users; --" contains invalid characters
}

func ExampleSelectBuilder_SafeFrom() {
	table, _ := sq.QuoteIdent("users")
	sql, _, _ := sq.Select("*").SafeFrom(table).ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM "users"
}

func ExampleSelectBuilder_SafeColumns() {
	cols, _ := sq.QuoteIdents("id", "name", "email")
	sql, _, _ := sq.Select().SafeColumns(cols...).From("users").ToSQL()
	fmt.Println(sql)
	// Output: SELECT "id", "name", "email" FROM users
}

func ExampleSelectBuilder_SafeOrderByDir() {
	col, _ := sq.QuoteIdent("name")
	sql, _, _ := sq.Select("*").From("users").SafeOrderByDir(col, sq.Desc).ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM users ORDER BY "name" DESC
}

func ExampleSelectBuilder_SafeGroupBy() {
	col, _ := sq.QuoteIdent("category")
	sql, _, _ := sq.Select("category", "count(*)").From("items").SafeGroupBy(col).ToSQL()
	fmt.Println(sql)
	// Output: SELECT category, count(*) FROM items GROUP BY "category"
}

func ExampleInsertBuilder_SafeInto() {
	table, _ := sq.QuoteIdent("users")
	sql, args, _ := sq.Insert("").SafeInto(table).Columns("name").Values("moe").ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO "users" (name) VALUES (?)
	// [moe]
}

func ExampleInsertBuilder_SafeColumns() {
	cols, _ := sq.QuoteIdents("id", "name")
	sql, args, _ := sq.Insert("users").SafeColumns(cols...).Values(1, "moe").ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users ("id","name") VALUES (?,?)
	// [1 moe]
}

func ExampleUpdateBuilder_SafeTable() {
	table, _ := sq.QuoteIdent("users")
	sql, args, _ := sq.Update("").SafeTable(table).Set("name", "moe").Where("id = ?", 1).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE "users" SET name = ? WHERE id = ?
	// [moe 1]
}

func ExampleUpdateBuilder_SafeSet() {
	col, _ := sq.QuoteIdent("name")
	sql, args, _ := sq.Update("users").SafeSet(col, "moe").Where("id = ?", 1).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE users SET "name" = ? WHERE id = ?
	// [moe 1]
}

func ExampleDeleteBuilder_SafeFrom() {
	table, _ := sq.QuoteIdent("users")
	sql, args, _ := sq.Delete("").SafeFrom(table).Where("id = ?", 1).ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// DELETE FROM "users" WHERE id = ?
	// [1]
}

// ---------------------------------------------------------------------------
// StatementBuilder
// ---------------------------------------------------------------------------

func ExampleStatementBuilder() {
	// StatementBuilder can set shared options for all child builders.
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	sql, args, _ := psql.Select("id", "name").
		From("users").
		Where("id = ?", 1).
		ToSQL()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, name FROM users WHERE id = $1
	// [1]
}

// ---------------------------------------------------------------------------
// DebugSqlizer
// ---------------------------------------------------------------------------

func ExampleDebugSqlizer() {
	query := sq.Select("id", "name").
		From("users").
		Where("id = ?", 42)
	fmt.Println(sq.DebugSqlizer(query))
	// Output: SELECT id, name FROM users WHERE id = '42'
}
