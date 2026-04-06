package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// Basic ON clause
// ---------------------------------------------------------------------------

func TestJoinExprBasicOn(t *testing.T) {
	j := JoinExpr("users").On("items.fk_user_key = users.key")

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN users ON items.fk_user_key = users.key", sql)
	assert.Nil(t, args)
}

func TestJoinExprOnWithArgs(t *testing.T) {
	j := JoinExpr("users").On("items.fk_user_key = users.key AND users.username = ?", "alice")

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN users ON items.fk_user_key = users.key AND users.username = ?", sql)
	assert.Equal(t, []interface{}{"alice"}, args)
}

func TestJoinExprMultipleOn(t *testing.T) {
	j := JoinExpr("users").
		On("items.fk_user_key = users.key").
		On("users.active = ?", true)

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN users ON items.fk_user_key = users.key AND users.active = ?", sql)
	assert.Equal(t, []interface{}{true}, args)
}

// ---------------------------------------------------------------------------
// OnExpr with expression helpers
// ---------------------------------------------------------------------------

func TestJoinExprOnExprEq(t *testing.T) {
	j := JoinExpr("users").
		On("items.fk_user_key = users.key").
		OnExpr(Eq{"users.active": true})

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN users ON items.fk_user_key = users.key AND users.active = ?", sql)
	assert.Equal(t, []interface{}{true}, args)
}

func TestJoinExprOnExprGt(t *testing.T) {
	j := JoinExpr("scores").
		On("items.id = scores.item_id").
		OnExpr(Gt{"scores.value": 100})

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN scores ON items.id = scores.item_id AND scores.value > ?", sql)
	assert.Equal(t, []interface{}{100}, args)
}

// ---------------------------------------------------------------------------
// USING clause
// ---------------------------------------------------------------------------

func TestJoinExprUsingSingle(t *testing.T) {
	j := JoinExpr("emails").Using("email_id")

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN emails USING (email_id)", sql)
	assert.Nil(t, args)
}

func TestJoinExprUsingMultiple(t *testing.T) {
	j := JoinExpr("addresses").Using("user_id", "region_id")

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN addresses USING (user_id, region_id)", sql)
	assert.Nil(t, args)
}

// ---------------------------------------------------------------------------
// Join types
// ---------------------------------------------------------------------------

func TestJoinExprTypeLeft(t *testing.T) {
	j := JoinExpr("users").Type(JoinLeft).On("items.user_id = users.id")

	sql, _, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "LEFT JOIN users ON items.user_id = users.id", sql)
}

func TestJoinExprTypeRight(t *testing.T) {
	j := JoinExpr("users").Type(JoinRight).Using("user_id")

	sql, _, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "RIGHT JOIN users USING (user_id)", sql)
}

func TestJoinExprTypeFull(t *testing.T) {
	j := JoinExpr("users").Type(JoinFull).On("items.user_id = users.id")

	sql, _, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "FULL OUTER JOIN users ON items.user_id = users.id", sql)
}

func TestJoinExprTypeCross(t *testing.T) {
	j := JoinExpr("sizes").Type(JoinCross)

	sql, _, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "CROSS JOIN sizes", sql)
}

// ---------------------------------------------------------------------------
// Alias
// ---------------------------------------------------------------------------

func TestJoinExprAlias(t *testing.T) {
	j := JoinExpr("users").As("u").On("items.user_id = u.id")

	sql, _, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN users u ON items.user_id = u.id", sql)
}

// ---------------------------------------------------------------------------
// SubQuery
// ---------------------------------------------------------------------------

func TestJoinExprSubQuery(t *testing.T) {
	sub := Select("id", "name").From("users").Where(Eq{"active": true})

	j := JoinExpr("").SubQuery(sub).As("u").On("items.user_id = u.id")

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN (SELECT id, name FROM users WHERE active = ?) u ON items.user_id = u.id", sql)
	assert.Equal(t, []interface{}{true}, args)
}

func TestJoinExprSubQueryWithOnArgs(t *testing.T) {
	sub := Select("id").From("users").Where(Eq{"active": true})

	j := JoinExpr("").SubQuery(sub).As("u").
		On("items.user_id = u.id").
		On("u.role = ?", "admin")

	sql, args, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "JOIN (SELECT id FROM users WHERE active = ?) u ON items.user_id = u.id AND u.role = ?", sql)
	assert.Equal(t, []interface{}{true, "admin"}, args)
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func TestJoinExprNoTable(t *testing.T) {
	j := JoinExpr("").On("a = b")

	_, _, err := j.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table name or subquery")
}

// ---------------------------------------------------------------------------
// Integration with SelectBuilder.JoinClause
// ---------------------------------------------------------------------------

func TestJoinExprWithSelectBuilder(t *testing.T) {
	b := Select("items.name", "users.username").
		From("items").
		JoinClause(
			JoinExpr("users").On("items.fk_user_key = users.key"),
		).
		Where(Eq{"items.id": 1})

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT items.name, users.username FROM items "+
			"JOIN users ON items.fk_user_key = users.key "+
			"WHERE items.id = ?",
		sql)
	assert.Equal(t, []interface{}{1}, args)
}

func TestJoinExprWithSelectBuilderMultipleConditions(t *testing.T) {
	b := Select("items.name", "users.username").
		From("items").
		JoinClause(
			JoinExpr("users").
				On("items.fk_user_key = users.key").
				On("users.username = ?", "alice"),
		)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT items.name, users.username FROM items "+
			"JOIN users ON items.fk_user_key = users.key AND users.username = ?",
		sql)
	assert.Equal(t, []interface{}{"alice"}, args)
}

func TestJoinExprWithSelectBuilderUsing(t *testing.T) {
	b := Select("*").
		From("orders").
		JoinClause(JoinExpr("customers").Using("customer_id"))

	sql, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM orders JOIN customers USING (customer_id)", sql)
}

func TestJoinExprWithSelectBuilderLeftJoinAlias(t *testing.T) {
	b := Select("i.name", "u.username").
		From("items i").
		JoinClause(
			JoinExpr("users").Type(JoinLeft).As("u").
				On("i.fk_user_key = u.key"),
		)

	sql, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT i.name, u.username FROM items i "+
			"LEFT JOIN users u ON i.fk_user_key = u.key",
		sql)
}

func TestJoinExprWithSelectBuilderSubquery(t *testing.T) {
	sub := Select("id", "name").From("users").Where(Eq{"active": true})

	b := Select("items.name", "u.name").
		From("items").
		JoinClause(
			JoinExpr("").SubQuery(sub).As("u").
				On("items.fk_user_key = u.id"),
		)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT items.name, u.name FROM items "+
			"JOIN (SELECT id, name FROM users WHERE active = ?) u ON items.fk_user_key = u.id",
		sql)
	assert.Equal(t, []interface{}{true}, args)
}

func TestJoinExprWithSelectBuilderMultipleJoins(t *testing.T) {
	b := Select("i.name", "u.username", "c.name").
		From("items i").
		JoinClause(
			JoinExpr("users").As("u").On("i.fk_user_key = u.key"),
		).
		JoinClause(
			JoinExpr("categories").Type(JoinLeft).As("c").
				On("i.category_id = c.id"),
		)

	sql, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT i.name, u.username, c.name FROM items i "+
			"JOIN users u ON i.fk_user_key = u.key "+
			"LEFT JOIN categories c ON i.category_id = c.id",
		sql)
}

func TestJoinExprWithSelectBuilderDollarPlaceholders(t *testing.T) {
	b := Select("items.name", "users.username").
		From("items").
		JoinClause(
			JoinExpr("users").
				On("items.fk_user_key = users.key").
				On("users.active = ?", true),
		).
		Where(Eq{"items.price": 100}).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT items.name, users.username FROM items "+
			"JOIN users ON items.fk_user_key = users.key AND users.active = $1 "+
			"WHERE items.price = $2",
		sql)
	assert.Equal(t, []interface{}{true, 100}, args)
}

func TestJoinExprMixedWithStringJoin(t *testing.T) {
	// Structured and string-based joins coexist.
	b := Select("*").
		From("items").
		JoinClause(
			JoinExpr("users").As("u").On("items.fk_user_key = u.key"),
		).
		LeftJoin("categories c ON items.category_id = c.id")

	sql, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT * FROM items "+
			"JOIN users u ON items.fk_user_key = u.key "+
			"LEFT JOIN categories c ON items.category_id = c.id",
		sql)
}

func TestJoinExprOnExprCombined(t *testing.T) {
	// OnExpr with Between and additional On string condition.
	b := Select("*").
		From("items").
		JoinClause(
			JoinExpr("prices").
				On("items.id = prices.item_id").
				OnExpr(Between{"prices.valid_from": [2]interface{}{"2025-01-01", "2025-12-31"}}),
		)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT * FROM items "+
			"JOIN prices ON items.id = prices.item_id AND prices.valid_from BETWEEN ? AND ?",
		sql)
	assert.Equal(t, []interface{}{"2025-01-01", "2025-12-31"}, args)
}

func TestJoinExprFullJoinUsing(t *testing.T) {
	j := JoinExpr("regions").Type(JoinFull).Using("region_id")

	sql, _, err := j.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "FULL OUTER JOIN regions USING (region_id)", sql)
}
