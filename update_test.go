package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilderToSql(t *testing.T) {
	b := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Set("c1", Case("status").When("1", "2").When("2", "1")).
		Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
		Set("c3", Select("a").From("b")).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "WITH prefix AS ? " +
		"UPDATE a SET b = ? + 1, c = ?, " +
		"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
		"c2 = CASE WHEN a = 2 THEN ? WHEN a = 3 THEN ? END, " +
		"c3 = (SELECT a FROM b) " +
		"WHERE d = ? " +
		"ORDER BY e LIMIT 4 OFFSET 5 " +
		"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2, "foo", "bar", 3, 6}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderToSqlErr(t *testing.T) {
	_, _, err := Update("").Set("x", 1).ToSQL()
	assert.Error(t, err)

	_, _, err = Update("x").ToSQL()
	assert.Error(t, err)
}

func TestUpdateBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestUpdateBuilderMustSql should have panicked!")
		}
	}()
	Update("").MustSQL()
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	b := Update("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}

func TestUpdateBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Update("test").Set("x", 1).RunWith(db)

	expectedSQL := "UPDATE test SET x = ?"

	_, err := b.Exec()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastExecSQL)
}

func TestUpdateBuilderNoRunner(t *testing.T) {
	b := Update("test").Set("x", 1)

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestUpdateBuilderFrom(t *testing.T) {
	sql, _, err := Update("employees").Set("sales_count", 100).From("accounts").Where("accounts.name = ?", "ACME").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE employees SET sales_count = ? FROM accounts WHERE accounts.name = ?", sql)
}

func TestUpdateBuilderFromSelect(t *testing.T) {
	sql, _, err := Update("employees").
		Set("sales_count", 100).
		FromSelect(Select("id").
			From("accounts").
			Where("accounts.name = ?", "ACME"), "subquery").
		Where("employees.account_id = subquery.id").ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE employees " +
		"SET sales_count = ? " +
		"FROM (SELECT id FROM accounts WHERE accounts.name = ?) AS subquery " +
		"WHERE employees.account_id = subquery.id"
	assert.Equal(t, expectedSQL, sql)
}

func TestUpdateBuilderReturning(t *testing.T) {
	b := Update("users").
		Set("name", "John").
		Where("id = ?", 1).
		Returning("id", "name")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE users SET name = ? WHERE id = ? RETURNING id, name"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John", 1}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderReturningWithPlaceholders(t *testing.T) {
	b := Update("users").
		Set("name", "John").
		Where("id = ?", 1).
		Returning("id").
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE users SET name = $1 WHERE id = $2 RETURNING id"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John", 1}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderReturningStar(t *testing.T) {
	b := Update("users").
		Set("name", "John").
		Where("id = ?", 1).
		Returning("*")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE users SET name = ? WHERE id = ? RETURNING *"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John", 1}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderReturningWithSuffix(t *testing.T) {
	b := Update("users").
		Set("name", "John").
		Where("id = ?", 1).
		Returning("id").
		Suffix("-- comment")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE users SET name = ? WHERE id = ? RETURNING id -- comment"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John", 1}
	assert.Equal(t, expectedArgs, args)
}
