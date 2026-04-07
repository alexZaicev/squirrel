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

// ---------------------------------------------------------------------------
// JOIN clauses
// ---------------------------------------------------------------------------

func TestDeleteBuilderJoin(t *testing.T) {
	sql, args, err := Delete("t1").
		Join("t2 ON t1.id = t2.t1_id").
		Where("t2.active = ?", false).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t2.active = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{false}, args)
}

func TestDeleteBuilderLeftJoin(t *testing.T) {
	sql, _, err := Delete("t1").
		LeftJoin("t2 ON t1.id = t2.t1_id").
		Where("t2.id IS NULL").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE t1 FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id WHERE t2.id IS NULL", sql)
}

func TestDeleteBuilderRightJoin(t *testing.T) {
	sql, _, err := Delete("t1").
		RightJoin("t2 ON t1.id = t2.t1_id").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE t1 FROM t1 RIGHT JOIN t2 ON t1.id = t2.t1_id", sql)
}

func TestDeleteBuilderInnerJoin(t *testing.T) {
	sql, _, err := Delete("t1").
		InnerJoin("t2 ON t1.id = t2.t1_id").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE t1 FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id", sql)
}

func TestDeleteBuilderCrossJoin(t *testing.T) {
	sql, _, err := Delete("t1").
		CrossJoin("t2").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE t1 FROM t1 CROSS JOIN t2", sql)
}

func TestDeleteBuilderFullJoin(t *testing.T) {
	sql, _, err := Delete("t1").
		FullJoin("t2 ON t1.id = t2.t1_id").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE t1 FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.t1_id", sql)
}

func TestDeleteBuilderJoinUsing(t *testing.T) {
	sql, _, err := Delete("t1").
		JoinUsing("t2", "id").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE t1 FROM t1 JOIN t2 USING (id)", sql)
}

func TestDeleteBuilderLeftJoinUsing(t *testing.T) {
	sql, _, err := Delete("t1").
		LeftJoinUsing("t2", "id", "region").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE t1 FROM t1 LEFT JOIN t2 USING (id, region)", sql)
}

func TestDeleteBuilderJoinWithPlaceholders(t *testing.T) {
	sql, args, err := Delete("t1").
		Join("t2 ON t1.id = t2.t1_id AND t2.status = ?", "inactive").
		Where("t1.id = ?", 1).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id AND t2.status = ? WHERE t1.id = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"inactive", 1}, args)
}

func TestDeleteBuilderJoinDollarPlaceholders(t *testing.T) {
	sql, args, err := Delete("t1").
		Join("t2 ON t1.id = t2.t1_id AND t2.status = ?", "inactive").
		Where("t1.id = ?", 1).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id AND t2.status = $1 WHERE t1.id = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"inactive", 1}, args)
}

func TestDeleteBuilderMultipleJoins(t *testing.T) {
	sql, args, err := Delete("t1").
		Join("t2 ON t1.id = t2.t1_id").
		LeftJoin("t3 ON t2.id = t3.t2_id AND t3.active = ?", true).
		Where("t1.id = ?", 1).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = t3.t2_id AND t3.active = ? WHERE t1.id = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{true, 1}, args)
}

func TestDeleteBuilderJoinClauseWithJoinExpr(t *testing.T) {
	sql, args, err := Delete("t1").
		JoinClause(
			JoinExpr("t2").On("t1.id = t2.t1_id").On("t2.status = ?", "inactive"),
		).
		Where("t1.id = ?", 1).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id AND t2.status = ? WHERE t1.id = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"inactive", 1}, args)
}

// ---------------------------------------------------------------------------
// USING clause (PostgreSQL)
// ---------------------------------------------------------------------------

func TestDeleteBuilderUsing(t *testing.T) {
	sql, args, err := Delete("t1").
		Using("t2").
		Where("t1.id = t2.t1_id AND t2.active = ?", false).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id AND t2.active = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{false}, args)
}

func TestDeleteBuilderUsingMultipleTables(t *testing.T) {
	sql, args, err := Delete("t1").
		Using("t2", "t3").
		Where("t1.id = t2.t1_id AND t2.t3_id = t3.id AND t3.active = ?", true).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM t1 USING t2, t3 WHERE t1.id = t2.t1_id AND t2.t3_id = t3.id AND t3.active = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{true}, args)
}

func TestDeleteBuilderUsingDollarPlaceholders(t *testing.T) {
	sql, args, err := Delete("t1").
		Using("t2").
		Where("t1.id = t2.t1_id AND t2.active = ?", false).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id AND t2.active = $1"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{false}, args)
}

func TestDeleteBuilderJoinWithReturning(t *testing.T) {
	sql, args, err := Delete("t1").
		Join("t2 ON t1.id = t2.t1_id").
		Where("t2.active = ?", false).
		Returning("t1.id").
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t2.active = ? RETURNING t1.id"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{false}, args)
}

func TestDeleteBuilderUsingWithReturning(t *testing.T) {
	sql, args, err := Delete("t1").
		Using("t2").
		Where("t1.id = t2.t1_id AND t2.active = ?", false).
		Returning("t1.id").
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id AND t2.active = $1 RETURNING t1.id"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{false}, args)
}
