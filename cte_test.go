package squirrel

import (
	"testing"

	"github.com/lann/builder"
	"github.com/stretchr/testify/assert"
)

func TestCteBuilderToSQL(t *testing.T) {
	sql, args, err := With("active_users",
		Select("id", "name").From("users").Where(Eq{"active": true}),
	).Statement(
		Select("*").From("active_users"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH active_users AS (SELECT id, name FROM users WHERE active = ?) SELECT * FROM active_users"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{true}, args)
}

func TestCteBuilderMultipleCTEs(t *testing.T) {
	sql, args, err := With("cte1",
		Select("id").From("t1").Where(Eq{"a": 1}),
	).With("cte2",
		Select("name").From("t2").Where(Eq{"b": 2}),
	).Statement(
		Select("*").From("cte1").Join("cte2 ON cte1.id = cte2.id"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH cte1 AS (SELECT id FROM t1 WHERE a = ?), " +
		"cte2 AS (SELECT name FROM t2 WHERE b = ?) " +
		"SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestCteBuilderRecursive(t *testing.T) {
	sql, args, err := WithRecursive("numbers",
		Union(
			Select("1 as n"),
			Select("n + 1").From("numbers").Where("n < ?", 10),
		),
	).Statement(
		Select("n").From("numbers"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH RECURSIVE numbers AS " +
		"(SELECT 1 as n UNION SELECT n + 1 FROM numbers WHERE n < ?) " +
		"SELECT n FROM numbers"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{10}, args)
}

func TestCteBuilderWithColumns(t *testing.T) {
	sql, args, err := WithColumns("cte", []string{"x", "y"},
		Select("a", "b").From("t1"),
	).Statement(
		Select("x", "y").From("cte"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH cte (x, y) AS (SELECT a, b FROM t1) SELECT x, y FROM cte"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestCteBuilderWithRecursiveColumns(t *testing.T) {
	sql, args, err := WithRecursiveColumns("cnt", []string{"x"},
		Union(
			Select("1"),
			Select("x + 1").From("cnt").Where("x < ?", 100),
		),
	).Statement(
		Select("x").From("cnt"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH RECURSIVE cnt (x) AS " +
		"(SELECT 1 UNION SELECT x + 1 FROM cnt WHERE x < ?) " +
		"SELECT x FROM cnt"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{100}, args)
}

func TestCteBuilderDollarPlaceholders(t *testing.T) {
	sql, args, err := With("active_users",
		Select("id").From("users").Where(Eq{"active": true}),
	).Statement(
		Select("*").From("active_users").Where(Eq{"role": "admin"}),
	).PlaceholderFormat(Dollar).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH active_users AS (SELECT id FROM users WHERE active = $1) " +
		"SELECT * FROM active_users WHERE role = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{true, "admin"}, args)
}

func TestCteBuilderWithInsert(t *testing.T) {
	sql, args, err := With("new_data",
		Select("id", "name").From("staging").Where(Eq{"status": "approved"}),
	).Statement(
		Insert("target").Columns("id", "name").Select(
			Select("id", "name").From("new_data"),
		),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH new_data AS (SELECT id, name FROM staging WHERE status = ?) " +
		"INSERT INTO target (id,name) SELECT id, name FROM new_data"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"approved"}, args)
}

func TestCteBuilderWithUpdate(t *testing.T) {
	sql, args, err := With("source",
		Select("id", "new_name").From("staging"),
	).Statement(
		Update("target").Set("name", Expr("source.new_name")).
			From("source").Where("target.id = source.id"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH source AS (SELECT id, new_name FROM staging) " +
		"UPDATE target SET name = source.new_name FROM source WHERE target.id = source.id"
	assert.Equal(t, expectedSQL, sql)
	assert.Nil(t, args)
}

func TestCteBuilderWithDelete(t *testing.T) {
	sql, args, err := With("old_data",
		Select("id").From("users").Where("created_at < ?", "2020-01-01"),
	).Statement(
		Delete("users").Where("id IN (SELECT id FROM old_data)"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH old_data AS (SELECT id FROM users WHERE created_at < ?) " +
		"DELETE FROM users WHERE id IN (SELECT id FROM old_data)"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"2020-01-01"}, args)
}

func TestCteBuilderMixedRecursiveAndNonRecursive(t *testing.T) {
	sql, args, err := With("simple",
		Select("1 as id"),
	).WithRecursive("tree",
		Union(
			Select("1 as n"),
			Select("n + 1").From("tree").Where("n < ?", 5),
		),
	).Statement(
		Select("*").From("simple").Join("tree ON simple.id = tree.n"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH RECURSIVE simple AS (SELECT 1 as id), " +
		"tree AS (SELECT 1 as n UNION SELECT n + 1 FROM tree WHERE n < ?) " +
		"SELECT * FROM simple JOIN tree ON simple.id = tree.n"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{5}, args)
}

func TestCteBuilderNoCtes(t *testing.T) {
	_, _, err := CteBuilder(builder.EmptyBuilder).
		Statement(Select("1")).
		ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one")
}

func TestCteBuilderNoStatement(t *testing.T) {
	_, _, err := With("cte", Select("1")).ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "main statement")
}

func TestCteBuilderSuffix(t *testing.T) {
	sql, _, err := With("cte", Select("1 as n")).
		Statement(Select("n").From("cte")).
		Suffix("FOR UPDATE").
		ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH cte AS (SELECT 1 as n) SELECT n FROM cte FOR UPDATE"
	assert.Equal(t, expectedSQL, sql)
}

func TestCteBuilderSuffixExpr(t *testing.T) {
	sql, args, err := With("cte", Select("1 as n")).
		Statement(Select("n").From("cte")).
		SuffixExpr(Expr("LIMIT ?", 10)).
		ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH cte AS (SELECT 1 as n) SELECT n FROM cte LIMIT ?"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{10}, args)
}

func TestCteBuilderRunWith(t *testing.T) {
	db := &DBStub{}
	b := With("cte", Select("1 as n")).
		Statement(Select("n").From("cte")).
		RunWith(db)

	expectedSQL := "WITH cte AS (SELECT 1 as n) SELECT n FROM cte"

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

func TestCteBuilderNoRunner(t *testing.T) {
	b := With("cte", Select("1")).Statement(Select("*").From("cte"))

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.Query()
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.Scan()
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestCteBuilderNoQueryRower(t *testing.T) {
	b := With("cte", Select("1")).
		Statement(Select("*").From("cte")).
		RunWith(&fakeBaseRunner{})

	err := b.Scan()
	assert.Equal(t, ErrRunnerNotQueryRunner, err)
}

func TestCteBuilderMustSQL(t *testing.T) {
	sql, args := With("cte", Select("1 as n")).
		Statement(Select("n").From("cte")).
		MustSQL()

	assert.Equal(t, "WITH cte AS (SELECT 1 as n) SELECT n FROM cte", sql)
	assert.Nil(t, args)
}

func TestCteBuilderMustSQLPanic(t *testing.T) {
	assert.Panics(t, func() {
		With("cte", Select("1")).MustSQL()
	})
}

func TestCteBuilderPlaceholderFormat(t *testing.T) {
	b := With("cte", Select("id").From("t").Where(Eq{"x": 1})).
		Statement(Select("id").From("cte").Where(Eq{"y": 2}))

	// Default: Question
	sql, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Contains(t, sql, "x = ?")
	assert.Contains(t, sql, "y = ?")

	// Dollar
	sql, _, err = b.PlaceholderFormat(Dollar).ToSQL()
	assert.NoError(t, err)
	assert.Contains(t, sql, "x = $1")
	assert.Contains(t, sql, "y = $2")
}

func TestCteBuilderChainedWith(t *testing.T) {
	// Test builder method chaining With on existing CteBuilder
	b := With("a", Select("1 as x"))
	b = b.With("b", Select("2 as y"))
	b = b.WithColumns("c", []string{"z"}, Select("3"))
	sql, _, err := b.Statement(Select("*").From("a").Join("b").Join("c")).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH a AS (SELECT 1 as x), b AS (SELECT 2 as y), c (z) AS (SELECT 3) " +
		"SELECT * FROM a JOIN b JOIN c"
	assert.Equal(t, expectedSQL, sql)
}

func TestCteBuilderWithRecursiveColumnsChained(t *testing.T) {
	// Chained WithRecursiveColumns on existing builder
	b := With("base", Select("1 as id"))
	b = b.WithRecursiveColumns("cnt", []string{"n"},
		Union(
			Select("1"),
			Select("n + 1").From("cnt").Where("n < ?", 5),
		),
	)
	sql, args, err := b.Statement(Select("*").From("base").Join("cnt ON base.id = cnt.n")).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH RECURSIVE base AS (SELECT 1 as id), " +
		"cnt (n) AS (SELECT 1 UNION SELECT n + 1 FROM cnt WHERE n < ?) " +
		"SELECT * FROM base JOIN cnt ON base.id = cnt.n"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{5}, args)
}

func TestCteBuilderWithUnionStatement(t *testing.T) {
	// CTE with a UNION as the main statement
	sql, args, err := With("cte",
		Select("id").From("t1").Where(Eq{"x": 1}),
	).Statement(
		Union(
			Select("id").From("cte"),
			Select("id").From("t2"),
		),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH cte AS (SELECT id FROM t1 WHERE x = ?) " +
		"SELECT id FROM cte UNION SELECT id FROM t2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1}, args)
}

func TestCteBuilderWithDeleteReturning(t *testing.T) {
	sql, args, err := With("old_rows",
		Select("id").From("logs").Where("created_at < ?", "2020-01-01"),
	).Statement(
		Delete("logs").Where("id IN (SELECT id FROM old_rows)").Returning("id"),
	).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH old_rows AS (SELECT id FROM logs WHERE created_at < ?) " +
		"DELETE FROM logs WHERE id IN (SELECT id FROM old_rows) RETURNING id"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{"2020-01-01"}, args)
}

func TestCteBuilderDollarWithInsert(t *testing.T) {
	sql, args, err := With("src",
		Select("id", "name").From("staging").Where(Eq{"approved": true}),
	).Statement(
		Insert("target").Columns("id", "name").Select(
			Select("id", "name").From("src"),
		),
	).PlaceholderFormat(Dollar).ToSQL()

	assert.NoError(t, err)
	expectedSQL := "WITH src AS (SELECT id, name FROM staging WHERE approved = $1) " +
		"INSERT INTO target (id,name) SELECT id, name FROM src"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{true}, args)
}
