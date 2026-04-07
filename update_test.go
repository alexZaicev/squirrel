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
		"ORDER BY e LIMIT ? OFFSET ? " +
		"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2, "foo", "bar", 3, uint64(4), uint64(5), 6}
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

func TestUpdateBuilderParameterizedLimit(t *testing.T) {
	sql, args, err := Update("users").Set("name", "Alice").Limit(10).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE users SET name = ? LIMIT ?", sql)
	assert.Equal(t, []any{"Alice", uint64(10)}, args)
}

func TestUpdateBuilderParameterizedOffset(t *testing.T) {
	sql, args, err := Update("users").Set("name", "Alice").Offset(5).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE users SET name = ? OFFSET ?", sql)
	assert.Equal(t, []any{"Alice", uint64(5)}, args)
}

func TestUpdateBuilderParameterizedLimitOffset(t *testing.T) {
	sql, args, err := Update("users").Set("name", "Alice").Limit(10).Offset(5).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE users SET name = ? LIMIT ? OFFSET ?", sql)
	assert.Equal(t, []any{"Alice", uint64(10), uint64(5)}, args)
}

func TestUpdateBuilderParameterizedLimitZero(t *testing.T) {
	sql, args, err := Update("users").Set("name", "Alice").Limit(0).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE users SET name = ? LIMIT ?", sql)
	assert.Equal(t, []any{"Alice", uint64(0)}, args)
}

func TestUpdateBuilderParameterizedLimitDollar(t *testing.T) {
	sql, args, err := Update("users").
		Set("name", "Alice").
		Where("id = ?", 1).
		Limit(10).
		Offset(5).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE users SET name = $1 WHERE id = $2 LIMIT $3 OFFSET $4", sql)
	assert.Equal(t, []any{"Alice", 1, uint64(10), uint64(5)}, args)
}

func TestUpdateBuilderParameterizedLimitPreparedStatementReuse(t *testing.T) {
	b1 := Update("users").Set("name", "Alice").Limit(10)
	b2 := Update("users").Set("name", "Alice").Limit(20)

	sql1, _, err := b1.ToSQL()
	assert.NoError(t, err)
	sql2, _, err := b2.ToSQL()
	assert.NoError(t, err)

	// Same SQL string for different limit values
	assert.Equal(t, sql1, sql2)
	assert.Equal(t, "UPDATE users SET name = ? LIMIT ?", sql1)
}

func TestUpdateBuilderSetSubqueryDollarPlaceholders(t *testing.T) {
	// Regression test for GitHub #326: Dollar placeholder misnumbering
	// with subqueries in UpdateBuilder.Set.
	b := Update("t").
		Set("a", 1).
		Set("b", Select("x").From("y").Where("z = ?", 2)).
		Where("id = ?", 3).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t SET a = $1, b = (SELECT x FROM y WHERE z = $2) WHERE id = $3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetMultipleSubqueriesDollarPlaceholders(t *testing.T) {
	// Multiple subqueries with Dollar placeholders should number sequentially.
	b := Update("t").
		Set("a", Select("x").From("y").Where("y.id = ?", 1)).
		Set("b", Select("p").From("q").Where("q.id = ?", 2)).
		Where("id = ?", 3).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t " +
		"SET a = (SELECT x FROM y WHERE y.id = $1), " +
		"b = (SELECT p FROM q WHERE q.id = $2) " +
		"WHERE id = $3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetSubqueryColonPlaceholders(t *testing.T) {
	// Colon-style positional placeholders should also number correctly.
	b := Update("t").
		Set("a", 1).
		Set("b", Select("x").From("y").Where("z = ?", 2)).
		Where("id = ?", 3).
		PlaceholderFormat(Colon)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t SET a = :1, b = (SELECT x FROM y WHERE z = :2) WHERE id = :3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetExprSubqueryDollarPlaceholders(t *testing.T) {
	// Non-SelectBuilder Sqlizer (e.g. Expr) should also work correctly.
	b := Update("t").
		Set("a", 1).
		Set("b", Expr("(SELECT x FROM y WHERE z = ?)", 2)).
		Where("id = ?", 3).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t SET a = $1, b = (SELECT x FROM y WHERE z = $2) WHERE id = $3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetCaseSubqueryDollarPlaceholders(t *testing.T) {
	// CaseBuilder within Set should also number correctly with Dollar.
	b := Update("t").
		Set("a", 1).
		Set("b", Case().When(Expr("x = ?", 2), Expr("?", 3)).Else(Expr("?", 4))).
		Where("id = ?", 5).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t SET a = $1, b = CASE WHEN x = $2 THEN $3 ELSE $4 END WHERE id = $5"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetSubqueryAtPPlaceholders(t *testing.T) {
	// AtP-style positional placeholders should also number correctly.
	b := Update("t").
		Set("a", 1).
		Set("b", Select("x").From("y").Where("z = ?", 2)).
		Where("id = ?", 3).
		PlaceholderFormat(AtP)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t SET a = @p1, b = (SELECT x FROM y WHERE z = @p2) WHERE id = @p3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetMapSubqueryDollarPlaceholders(t *testing.T) {
	// SetMap with a Sqlizer value should also number correctly with Dollar.
	b := Update("t").
		SetMap(map[string]any{
			"a": 1,
			"b": Select("x").From("y").Where("z = ?", 2),
		}).
		Where("id = ?", 3).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t SET a = $1, b = (SELECT x FROM y WHERE z = $2) WHERE id = $3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetSubqueryFromSelectDollarPlaceholders(t *testing.T) {
	// Mixed: Set with subquery + FromSelect + Where, all with Dollar placeholders.
	b := Update("t").
		Set("a", Select("x").From("y").Where("y.id = ?", 1)).
		FromSelect(Select("id").From("s").Where("s.active = ?", true), "sub").
		Where("t.id = sub.id").
		Where("t.status = ?", 2).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t " +
		"SET a = (SELECT x FROM y WHERE y.id = $1) " +
		"FROM (SELECT id FROM s WHERE s.active = $2) AS sub " +
		"WHERE t.id = sub.id AND t.status = $3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, true, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSetSubqueryInWhereDollarPlaceholders(t *testing.T) {
	// Set with subquery + Where with Eq subquery, all with Dollar.
	b := Update("t").
		Set("a", 1).
		Set("b", Select("x").From("y").Where("z = ?", 2)).
		Where(Eq{"t.id": Select("id").From("s").Where("s.active = ?", true)}).
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t SET a = $1, b = (SELECT x FROM y WHERE z = $2) WHERE t.id IN (SELECT id FROM s WHERE s.active = $3)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, true}
	assert.Equal(t, expectedArgs, args)
}

// ---------------------------------------------------------------------------
// JOIN clauses
// ---------------------------------------------------------------------------

func TestUpdateBuilderJoin(t *testing.T) {
	sql, args, err := Update("t1").
		Join("t2 ON t1.id = t2.t1_id").
		Set("t1.name", "updated").
		Where("t2.active = ?", true).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t1 JOIN t2 ON t1.id = t2.t1_id SET t1.name = ? WHERE t2.active = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"updated", true}, args)
}

func TestUpdateBuilderLeftJoin(t *testing.T) {
	sql, _, err := Update("t1").
		LeftJoin("t2 ON t1.id = t2.t1_id").
		Set("t1.name", "updated").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE t1 LEFT JOIN t2 ON t1.id = t2.t1_id SET t1.name = ?", sql)
}

func TestUpdateBuilderRightJoin(t *testing.T) {
	sql, _, err := Update("t1").
		RightJoin("t2 ON t1.id = t2.t1_id").
		Set("t1.name", "updated").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE t1 RIGHT JOIN t2 ON t1.id = t2.t1_id SET t1.name = ?", sql)
}

func TestUpdateBuilderInnerJoin(t *testing.T) {
	sql, _, err := Update("t1").
		InnerJoin("t2 ON t1.id = t2.t1_id").
		Set("t1.name", "updated").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE t1 INNER JOIN t2 ON t1.id = t2.t1_id SET t1.name = ?", sql)
}

func TestUpdateBuilderCrossJoin(t *testing.T) {
	sql, _, err := Update("t1").
		CrossJoin("t2").
		Set("t1.name", "updated").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE t1 CROSS JOIN t2 SET t1.name = ?", sql)
}

func TestUpdateBuilderFullJoin(t *testing.T) {
	sql, _, err := Update("t1").
		FullJoin("t2 ON t1.id = t2.t1_id").
		Set("t1.name", "updated").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE t1 FULL OUTER JOIN t2 ON t1.id = t2.t1_id SET t1.name = ?", sql)
}

func TestUpdateBuilderJoinUsing(t *testing.T) {
	sql, _, err := Update("t1").
		JoinUsing("t2", "id").
		Set("t1.name", "updated").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE t1 JOIN t2 USING (id) SET t1.name = ?", sql)
}

func TestUpdateBuilderLeftJoinUsing(t *testing.T) {
	sql, _, err := Update("t1").
		LeftJoinUsing("t2", "id", "region").
		Set("t1.name", "updated").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE t1 LEFT JOIN t2 USING (id, region) SET t1.name = ?", sql)
}

func TestUpdateBuilderJoinWithPlaceholders(t *testing.T) {
	sql, args, err := Update("t1").
		Join("t2 ON t1.id = t2.t1_id AND t2.status = ?", "active").
		Set("t1.name", "updated").
		Where("t1.id = ?", 1).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t1 JOIN t2 ON t1.id = t2.t1_id AND t2.status = ? SET t1.name = ? WHERE t1.id = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"active", "updated", 1}, args)
}

func TestUpdateBuilderJoinDollarPlaceholders(t *testing.T) {
	sql, args, err := Update("t1").
		Join("t2 ON t1.id = t2.t1_id AND t2.status = ?", "active").
		Set("t1.name", "updated").
		Where("t1.id = ?", 1).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t1 JOIN t2 ON t1.id = t2.t1_id AND t2.status = $1 SET t1.name = $2 WHERE t1.id = $3"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"active", "updated", 1}, args)
}

func TestUpdateBuilderMultipleJoins(t *testing.T) {
	sql, args, err := Update("t1").
		Join("t2 ON t1.id = t2.t1_id").
		LeftJoin("t3 ON t2.id = t3.t2_id AND t3.active = ?", true).
		Set("t1.name", "updated").
		Where("t1.id = ?", 1).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t1 JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = t3.t2_id AND t3.active = ? SET t1.name = ? WHERE t1.id = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{true, "updated", 1}, args)
}

func TestUpdateBuilderJoinClauseWithJoinExpr(t *testing.T) {
	sql, args, err := Update("t1").
		JoinClause(
			JoinExpr("t2").On("t1.id = t2.t1_id").On("t2.status = ?", "active"),
		).
		Set("t1.name", "updated").
		Where("t1.id = ?", 1).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t1 JOIN t2 ON t1.id = t2.t1_id AND t2.status = ? SET t1.name = ? WHERE t1.id = ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"active", "updated", 1}, args)
}

func TestUpdateBuilderJoinWithFrom(t *testing.T) {
	// JOIN and FROM can coexist — JOIN comes before SET, FROM comes after SET.
	sql, _, err := Update("t1").
		Join("t2 ON t1.id = t2.t1_id").
		Set("t1.name", "updated").
		From("t3").
		Where("t3.id = t2.t3_id").
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE t1 JOIN t2 ON t1.id = t2.t1_id SET t1.name = ? FROM t3 WHERE t3.id = t2.t3_id"
	assert.Equal(t, expectedSQL, sql)
}
