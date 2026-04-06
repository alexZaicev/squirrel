package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderToSql(t *testing.T) {
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "WITH prefix AS ? " +
		"DELETE FROM a WHERE b = ? ORDER BY c LIMIT ? OFFSET ? " +
		"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, uint64(2), uint64(3), 4}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderToSqlErr(t *testing.T) {
	_, _, err := Delete("").ToSQL()
	assert.Error(t, err)
}

func TestDeleteBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestDeleteBuilderMustSql should have panicked!")
		}
	}()
	Delete("").MustSQL()
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}

func TestDeleteBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("x = ?", 1).RunWith(db)

	expectedSQL := "DELETE FROM test WHERE x = ?"

	_, err := b.Exec()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastExecSQL)
}

func TestDeleteBuilderNoRunner(t *testing.T) {
	b := Delete("test")

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestDeleteWithQuery(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("id=55").Suffix("RETURNING path").RunWith(db)

	expectedSQL := "DELETE FROM test WHERE id=55 RETURNING path"
	_, err := b.Query()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, db.LastQuerySQL)
}

func TestDeleteBuilderReturning(t *testing.T) {
	b := Delete("users").
		Where("id = ?", 1).
		Returning("id", "name")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM users WHERE id = ? RETURNING id, name"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderReturningWithPlaceholders(t *testing.T) {
	b := Delete("users").
		Where("id = ?", 1).
		Returning("id").
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM users WHERE id = $1 RETURNING id"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderReturningStar(t *testing.T) {
	b := Delete("users").
		Where("id = ?", 1).
		Returning("*")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM users WHERE id = ? RETURNING *"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderReturningWithSuffix(t *testing.T) {
	b := Delete("users").
		Where("id = ?", 1).
		Returning("id").
		Suffix("-- comment")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM users WHERE id = ? RETURNING id -- comment"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderReturningWithQuery(t *testing.T) {
	db := &DBStub{}
	b := Delete("users").
		Where("id = ?", 1).
		Returning("id").
		RunWith(db)

	expectedSQL := "DELETE FROM users WHERE id = ? RETURNING id"
	_, err := b.Query()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastQuerySQL)
}

func TestDeleteBuilderParameterizedLimit(t *testing.T) {
	sql, args, err := Delete("logs").Where("created < ?", "2024-01-01").Limit(100).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM logs WHERE created < ? LIMIT ?", sql)
	assert.Equal(t, []any{"2024-01-01", uint64(100)}, args)
}

func TestDeleteBuilderParameterizedOffset(t *testing.T) {
	sql, args, err := Delete("logs").Offset(10).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM logs OFFSET ?", sql)
	assert.Equal(t, []any{uint64(10)}, args)
}

func TestDeleteBuilderParameterizedLimitOffset(t *testing.T) {
	sql, args, err := Delete("logs").Limit(50).Offset(10).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM logs LIMIT ? OFFSET ?", sql)
	assert.Equal(t, []any{uint64(50), uint64(10)}, args)
}

func TestDeleteBuilderParameterizedLimitZero(t *testing.T) {
	sql, args, err := Delete("logs").Limit(0).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM logs LIMIT ?", sql)
	assert.Equal(t, []any{uint64(0)}, args)
}

func TestDeleteBuilderParameterizedLimitDollar(t *testing.T) {
	sql, args, err := Delete("logs").
		Where("active = ?", false).
		Limit(10).
		Offset(5).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM logs WHERE active = $1 LIMIT $2 OFFSET $3", sql)
	assert.Equal(t, []any{false, uint64(10), uint64(5)}, args)
}

func TestDeleteBuilderParameterizedLimitPreparedStatementReuse(t *testing.T) {
	b1 := Delete("logs").Limit(100)
	b2 := Delete("logs").Limit(200)

	sql1, _, err := b1.ToSQL()
	assert.NoError(t, err)
	sql2, _, err := b2.ToSQL()
	assert.NoError(t, err)

	// Same SQL string for different limit values
	assert.Equal(t, sql1, sql2)
	assert.Equal(t, "DELETE FROM logs LIMIT ?", sql1)
}
