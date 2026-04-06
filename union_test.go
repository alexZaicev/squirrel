package squirrel

import (
	"testing"

	"github.com/lann/builder"
	"github.com/stretchr/testify/assert"
)

func TestUnionBuilderToSQL(t *testing.T) {
	q1 := Select("id", "name").From("users").Where(Eq{"active": true})
	q2 := Select("id", "name").From("admins").Where(Eq{"active": true})

	sql, args, err := Union(q1, q2).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id, name FROM users WHERE active = ? " +
		"UNION " +
		"SELECT id, name FROM admins WHERE active = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{true, true}, args)
}

func TestUnionAllBuilderToSQL(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := UnionAll(q1, q2).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 UNION ALL SELECT id FROM t2"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestIntersectBuilderToSQL(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Intersect(q1, q2).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 INTERSECT SELECT id FROM t2"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestExceptBuilderToSQL(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Except(q1, q2).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 EXCEPT SELECT id FROM t2"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestUnionBuilderChaining(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")
	q3 := Select("id").From("t3")

	sql, args, err := Union(q1, q2).UnionAll(q3).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 UNION SELECT id FROM t2 UNION ALL SELECT id FROM t3"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestUnionBuilderMixedSetOperations(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")
	q3 := Select("id").From("t3")
	q4 := Select("id").From("t4")

	sql, args, err := Union(q1, q2).Intersect(q3).Except(q4).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 UNION SELECT id FROM t2 " +
		"INTERSECT SELECT id FROM t3 " +
		"EXCEPT SELECT id FROM t4"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestUnionBuilderWithOrderByAndLimit(t *testing.T) {
	q1 := Select("id", "name").From("users")
	q2 := Select("id", "name").From("admins")

	sql, args, err := Union(q1, q2).
		OrderBy("name ASC").
		Limit(10).
		Offset(5).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id, name FROM users UNION SELECT id, name FROM admins " +
		"ORDER BY name ASC LIMIT ? OFFSET ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{uint64(10), uint64(5)}, args)
}

func TestUnionBuilderWithPrefixAndSuffix(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Union(q1, q2).
		Prefix("WITH cte AS (SELECT 1)").
		Suffix("FOR UPDATE").
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "WITH cte AS (SELECT 1) " +
		"SELECT id FROM t1 UNION SELECT id FROM t2 " +
		"FOR UPDATE"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestUnionBuilderDollarPlaceholders(t *testing.T) {
	q1 := Select("id").From("t1").Where(Eq{"a": 1})
	q2 := Select("id").From("t2").Where(Eq{"b": 2})

	sql, args, err := Union(q1, q2).PlaceholderFormat(Dollar).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 WHERE a = $1 UNION SELECT id FROM t2 WHERE b = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestUnionBuilderNoParts(t *testing.T) {
	b := UnionBuilder(builder.EmptyBuilder)
	_, _, err := b.ToSQL()
	assert.Error(t, err)
}

func TestUnionBuilderSingleSelect(t *testing.T) {
	q1 := Select("id").From("t1")

	sql, args, err := Union(q1).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestUnionBuilderMustSQL(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args := Union(q1, q2).MustSQL()
	expectedSQL := "SELECT id FROM t1 UNION SELECT id FROM t2"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestUnionBuilderMustSQLPanic(t *testing.T) {
	b := UnionBuilder(builder.EmptyBuilder)
	assert.Panics(t, func() { b.MustSQL() })
}

func TestUnionBuilderRunners(t *testing.T) {
	db := &DBStub{}
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")
	b := Union(q1, q2).RunWith(db)

	expectedSQL := "SELECT id FROM t1 UNION SELECT id FROM t2"

	_, err := b.Exec()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastExecSQL)

	_, err = b.Query()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastQuerySQL)

	b.QueryRow()
	assert.Equal(t, expectedSQL, db.LastQueryRowSQL)
}

func TestUnionBuilderNoRunner(t *testing.T) {
	q1 := Select("id").From("t1")
	b := Union(q1)

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.Query()
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestUnionBuilderNoRunnerQueryRow(t *testing.T) {
	q1 := Select("id").From("t1")
	b := Union(q1)

	row := b.QueryRow()
	err := row.Scan()
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestUnionBuilderQueryRowNotQueryRunner(t *testing.T) {
	q1 := Select("id").From("t1")
	b := Union(q1).RunWith(fakeBaseRunner{})

	row := b.QueryRow()
	err := row.Scan()
	assert.Equal(t, ErrRunnerNotQueryRunner, err)
}

func TestUnionBuilderOrderByClause(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Union(q1, q2).
		OrderByClause("? ASC", 1).
		OrderBy("id DESC").
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY ? ASC, id DESC"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1}, args)
}

func TestUnionBuilderRemoveLimitOffset(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	b := Union(q1, q2).Limit(10).Offset(5)

	sql, _, err := b.RemoveLimit().RemoveOffset().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 UNION SELECT id FROM t2", sql)
}

func TestUnionAllMultipleSelects(t *testing.T) {
	q1 := Select("id").From("t1").Where("a = ?", 1)
	q2 := Select("id").From("t2").Where("b = ?", 2)
	q3 := Select("id").From("t3").Where("c = ?", 3)

	sql, args, err := UnionAll(q1, q2, q3).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 WHERE a = ? " +
		"UNION ALL SELECT id FROM t2 WHERE b = ? " +
		"UNION ALL SELECT id FROM t3 WHERE c = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 2, 3}, args)
}

func TestUnionBuilderPrefixExpr(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Union(q1, q2).
		PrefixExpr(Expr("WITH cte AS (SELECT ?)", 42)).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "WITH cte AS (SELECT ?) SELECT id FROM t1 UNION SELECT id FROM t2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{42}, args)
}

func TestUnionBuilderSuffixExpr(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Union(q1, q2).
		SuffixExpr(Expr("LIMIT ?", 10)).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 UNION SELECT id FROM t2 LIMIT ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{10}, args)
}

func TestUnionBuilderWithNestedDollarPlaceholders(t *testing.T) {
	q1 := Select("id").From("t1").Where(Eq{"a": 1}).PlaceholderFormat(Dollar)
	q2 := Select("id").From("t2").Where(Eq{"b": 2}).PlaceholderFormat(Dollar)
	q3 := Select("id").From("t3").Where(Eq{"c": 3}).PlaceholderFormat(Dollar)

	sql, args, err := UnionAll(q1, q2, q3).PlaceholderFormat(Dollar).ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT id FROM t1 WHERE a = $1 " +
		"UNION ALL SELECT id FROM t2 WHERE b = $2 " +
		"UNION ALL SELECT id FROM t3 WHERE c = $3"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 2, 3}, args)
}

func TestStatementBuilderUnion(t *testing.T) {
	db := &DBStub{}
	sb := StatementBuilder.RunWith(db)

	q1 := sb.Select("id").From("t1")
	q2 := sb.Select("id").From("t2")

	sql, _, err := Union(q1, q2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 UNION SELECT id FROM t2", sql)
}

func TestUnionBuilderParameterizedLimit(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Union(q1, q2).Limit(10).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 UNION SELECT id FROM t2 LIMIT ?", sql)
	assert.Equal(t, []any{uint64(10)}, args)
}

func TestUnionBuilderParameterizedOffset(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Union(q1, q2).Offset(5).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 UNION SELECT id FROM t2 OFFSET ?", sql)
	assert.Equal(t, []any{uint64(5)}, args)
}

func TestUnionBuilderParameterizedLimitOffset(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	sql, args, err := Union(q1, q2).Limit(10).Offset(5).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 UNION SELECT id FROM t2 LIMIT ? OFFSET ?", sql)
	assert.Equal(t, []any{uint64(10), uint64(5)}, args)
}

func TestUnionBuilderParameterizedLimitZero(t *testing.T) {
	q1 := Select("id").From("t1")

	sql, args, err := Union(q1).Limit(0).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 LIMIT ?", sql)
	assert.Equal(t, []any{uint64(0)}, args)
}

func TestUnionBuilderParameterizedLimitDollar(t *testing.T) {
	q1 := Select("id").From("t1").Where(Eq{"a": 1})
	q2 := Select("id").From("t2").Where(Eq{"b": 2})

	sql, args, err := Union(q1, q2).
		Limit(10).
		Offset(5).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 WHERE a = $1 UNION SELECT id FROM t2 WHERE b = $2 LIMIT $3 OFFSET $4", sql)
	assert.Equal(t, []any{1, 2, uint64(10), uint64(5)}, args)
}

func TestUnionBuilderParameterizedLimitPreparedStatementReuse(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	b1 := Union(q1, q2).Limit(10).Offset(0)
	b2 := Union(q1, q2).Limit(20).Offset(10)

	sql1, args1, err := b1.ToSQL()
	assert.NoError(t, err)
	sql2, args2, err := b2.ToSQL()
	assert.NoError(t, err)

	// Same SQL string for different limit/offset values
	assert.Equal(t, sql1, sql2)
	assert.Equal(t, "SELECT id FROM t1 UNION SELECT id FROM t2 LIMIT ? OFFSET ?", sql1)

	assert.Equal(t, []any{uint64(10), uint64(0)}, args1)
	assert.Equal(t, []any{uint64(20), uint64(10)}, args2)
}

func TestUnionBuilderRemoveLimitOffset_Parameterized(t *testing.T) {
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")

	b := Union(q1, q2).Limit(10).Offset(5)

	sql, args, err := b.RemoveLimit().RemoveOffset().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT id FROM t1 UNION SELECT id FROM t2", sql)
	assert.Nil(t, args)
}
