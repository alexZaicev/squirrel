package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectBuilderToSql(t *testing.T) {
	subQ := Select("aa", "bb").From("dd")
	b := Select("a", "b").
		Prefix("WITH prefix AS ?", 0).
		Distinct().
		Columns("c").
		Column("IF(d IN ("+Placeholders(3)+"), 1, 0) as stat_column", 1, 2, 3).
		Column(Expr("a > ?", 100)).
		Column(Alias(Eq{"b": []int{101, 102, 103}}, "b_alias")).
		Column(Alias(subQ, "subq")).
		From("e").
		JoinClause("CROSS JOIN j1").
		Join("j2").
		LeftJoin("j3").
		RightJoin("j4").
		InnerJoin("j5").
		CrossJoin("j6").
		Where("f = ?", 4).
		Where(Eq{"g": 5}).
		Where(map[string]any{"h": 6}).
		Where(Eq{"i": []int{7, 8, 9}}).
		Where(Or{Expr("j = ?", 10), And{Eq{"k": 11}, Expr("true")}}).
		GroupBy("l").
		Having("m = n").
		OrderByClause("? DESC", 1).
		OrderBy("o ASC", "p DESC").
		Limit(12).
		Offset(13).
		Suffix("FETCH FIRST ? ROWS ONLY", 14)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "WITH prefix AS ? " +
		"SELECT DISTINCT a, b, c, IF(d IN (?,?,?), 1, 0) as stat_column, a > ?, " +
		"(b IN (?,?,?)) AS b_alias, " +
		"(SELECT aa, bb FROM dd) AS subq " +
		"FROM e " +
		"CROSS JOIN j1 JOIN j2 LEFT JOIN j3 RIGHT JOIN j4 INNER JOIN j5 CROSS JOIN j6 " +
		"WHERE f = ? AND g = ? AND h = ? AND i IN (?,?,?) AND (j = ? OR (k = ? AND true)) " +
		"GROUP BY l HAVING m = n ORDER BY ? DESC, o ASC, p DESC LIMIT 12 OFFSET 13 " +
		"FETCH FIRST ? ROWS ONLY"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2, 3, 100, 101, 102, 103, 4, 5, 6, 7, 8, 9, 10, 11, 1, 14}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderFromSelect(t *testing.T) {
	subQ := Select("c").From("d").Where(Eq{"i": 0})
	b := Select("a", "b").FromSelect(subQ, "subq")
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT a, b FROM (SELECT c FROM d WHERE i = ?) AS subq"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderFromSelectNestedDollarPlaceholders(t *testing.T) {
	subQ := Select("c").
		From("t").
		Where(Gt{"c": 1}).
		PlaceholderFormat(Dollar)
	b := Select("c").
		FromSelect(subQ, "subq").
		Where(Lt{"c": 2}).
		PlaceholderFormat(Dollar)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT c FROM (SELECT c FROM t WHERE c > $1) AS subq WHERE c < $2"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderToSqlErr(t *testing.T) {
	_, _, err := Select().From("x").ToSQL()
	assert.Error(t, err)
}

func TestSelectBuilderPlaceholders(t *testing.T) {
	b := Select("test").Where("x = ? AND y = ?")

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = $1 AND y = $2", sql)

	sql, _, _ = b.PlaceholderFormat(Colon).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = :1 AND y = :2", sql)

	sql, _, _ = b.PlaceholderFormat(AtP).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = @p1 AND y = @p2", sql)
}

func TestSelectBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Select("test").RunWith(db)

	expectedSQL := "SELECT test"

	_, err := b.Exec()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastExecSQL)

	_, err = b.Query()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastQuerySQL)

	b.QueryRow()
	assert.Equal(t, expectedSQL, db.LastQueryRowSQL)

	err = b.Scan()
	assert.NoError(t, err)
}

func TestSelectBuilderNoRunner(t *testing.T) {
	b := Select("test")

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.Query()
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.Scan()
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestSelectBuilderSimpleJoin(t *testing.T) {
	expectedSQL := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo"
	expectedArgs := []any(nil)

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderParamJoin(t *testing.T) {
	expectedSQL := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo AND baz.foo = ?"
	expectedArgs := []any{42}

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo AND baz.foo = ?", 42)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderNestedSelectJoin(t *testing.T) {
	expectedSQL := "SELECT * FROM bar JOIN ( SELECT * FROM baz WHERE foo = ? ) r ON bar.foo = r.foo"
	expectedArgs := []any{42}

	nestedSelect := Select("*").From("baz").Where("foo = ?", 42)

	b := Select("*").From("bar").JoinClause(nestedSelect.Prefix("JOIN (").Suffix(") r ON bar.foo = r.foo"))

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestSelectWithOptions(t *testing.T) {
	sql, _, err := Select("*").From("foo").Distinct().Options("SQL_NO_CACHE").ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT DISTINCT SQL_NO_CACHE * FROM foo", sql)
}

func TestSelectWithRemoveLimit(t *testing.T) {
	sql, _, err := Select("*").From("foo").Limit(10).RemoveLimit().ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM foo", sql)
}

func TestSelectWithRemoveOffset(t *testing.T) {
	sql, _, err := Select("*").From("foo").Offset(10).RemoveOffset().ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM foo", sql)
}

func TestSelectBuilderNestedSelectDollar(t *testing.T) {
	nestedBuilder := StatementBuilder.PlaceholderFormat(Dollar).Select("*").Prefix("NOT EXISTS (").
		From("bar").Where("y = ?", 42).Suffix(")")
	outerSQL, _, err := StatementBuilder.PlaceholderFormat(Dollar).Select("*").
		From("foo").Where("x = ?").Where(nestedBuilder).ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM foo WHERE x = $1 AND NOT EXISTS ( SELECT * FROM bar WHERE y = $2 )", outerSQL)
}

func TestSelectBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestSelectBuilderMustSql should have panicked!")
		}
	}()
	// This function should cause a panic
	Select().From("foo").MustSQL()
}

func TestSelectWithoutWhereClause(t *testing.T) {
	sql, _, err := Select("*").From("users").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
}

func TestSelectWithNilWhereClause(t *testing.T) {
	sql, _, err := Select("*").From("users").Where(nil).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
}

func TestSelectWithEmptyStringWhereClause(t *testing.T) {
	sql, _, err := Select("*").From("users").Where("").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
}

func TestSelectSubqueryPlaceholderNumbering(t *testing.T) {
	subquery := Select("a").Where("b = ?", 1).PlaceholderFormat(Dollar)
	with := subquery.Prefix("WITH a AS (").Suffix(")")

	sql, args, err := Select("*").
		PrefixExpr(with).
		FromSelect(subquery, "q").
		Where("c = ?", 2).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "WITH a AS ( SELECT a WHERE b = $1 ) SELECT * FROM (SELECT a WHERE b = $2) AS q WHERE c = $3"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 1, 2}, args)
}

func TestSelectSubqueryInConjunctionPlaceholderNumbering(t *testing.T) {
	subquery := Select("a").Where(Eq{"b": 1}).Prefix("EXISTS(").Suffix(")").PlaceholderFormat(Dollar)

	sql, args, err := Select("*").
		Where(Or{subquery}).
		Where("c = ?", 2).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * WHERE (EXISTS( SELECT a WHERE b = $1 )) AND c = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestSelectJoinClausePlaceholderNumbering(t *testing.T) {
	subquery := Select("a").Where(Eq{"b": 2}).PlaceholderFormat(Dollar)

	sql, args, err := Select("t1.a").
		From("t1").
		Where(Eq{"a": 1}).
		JoinClause(subquery.Prefix("JOIN (").Suffix(") t2 ON (t1.a = t2.a)")).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT t1.a FROM t1 JOIN ( SELECT a WHERE b = $1 ) t2 ON (t1.a = t2.a) WHERE a = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{2, 1}, args)
}

func TestSelectBuilderFullJoin(t *testing.T) {
	expectedSQL := "SELECT * FROM bar FULL OUTER JOIN baz ON bar.foo = baz.foo"

	b := Select("*").From("bar").FullJoin("baz ON bar.foo = baz.foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderFullJoinWithArgs(t *testing.T) {
	expectedSQL := "SELECT * FROM bar FULL OUTER JOIN baz ON bar.foo = baz.foo AND baz.id = ?"
	expectedArgs := []any{42}

	b := Select("*").From("bar").FullJoin("baz ON bar.foo = baz.foo AND baz.id = ?", 42)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderJoinUsing(t *testing.T) {
	expectedSQL := "SELECT * FROM bar JOIN baz USING (foo)"

	b := Select("*").From("bar").JoinUsing("baz", "foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderJoinUsingMultipleColumns(t *testing.T) {
	expectedSQL := "SELECT * FROM bar JOIN baz USING (foo, bar_id)"

	b := Select("*").From("bar").JoinUsing("baz", "foo", "bar_id")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderLeftJoinUsing(t *testing.T) {
	expectedSQL := "SELECT * FROM bar LEFT JOIN baz USING (foo)"

	b := Select("*").From("bar").LeftJoinUsing("baz", "foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderRightJoinUsing(t *testing.T) {
	expectedSQL := "SELECT * FROM bar RIGHT JOIN baz USING (foo)"

	b := Select("*").From("bar").RightJoinUsing("baz", "foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderInnerJoinUsing(t *testing.T) {
	expectedSQL := "SELECT * FROM bar INNER JOIN baz USING (foo)"

	b := Select("*").From("bar").InnerJoinUsing("baz", "foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderCrossJoinUsing(t *testing.T) {
	expectedSQL := "SELECT * FROM bar CROSS JOIN baz USING (foo)"

	b := Select("*").From("bar").CrossJoinUsing("baz", "foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderFullJoinUsing(t *testing.T) {
	expectedSQL := "SELECT * FROM bar FULL OUTER JOIN baz USING (foo)"

	b := Select("*").From("bar").FullJoinUsing("baz", "foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderFullJoinUsingMultipleColumns(t *testing.T) {
	expectedSQL := "SELECT * FROM bar FULL OUTER JOIN baz USING (foo, bar_id)"

	b := Select("*").From("bar").FullJoinUsing("baz", "foo", "bar_id")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderMixedJoins(t *testing.T) {
	expectedSQL := "SELECT * FROM bar " +
		"JOIN baz USING (id) " +
		"LEFT JOIN qux ON bar.qux_id = qux.id " +
		"FULL OUTER JOIN quux USING (foo, bar_id)"

	b := Select("*").From("bar").
		JoinUsing("baz", "id").
		LeftJoin("qux ON bar.qux_id = qux.id").
		FullJoinUsing("quux", "foo", "bar_id")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestRemoveColumns(t *testing.T) {
	query := Select("id").
		From("users").
		RemoveColumns()
	query = query.Columns("name")
	sql, _, err := query.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT name FROM users", sql)
}
