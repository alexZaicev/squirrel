package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCaseWithVal(t *testing.T) {
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	qb := Select().
		Column(caseStmt).
		From("table")
	sql, args, err := qb.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "SELECT CASE number " +
		"WHEN 1 THEN one " +
		"WHEN 2 THEN two " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"big number"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithComplexVal(t *testing.T) {
	caseStmt := Case("? > ?", 10, 5).
		When("true", "'T'")

	qb := Select().
		Column(Alias(caseStmt, "complexCase")).
		From("table")
	sql, args, err := qb.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "SELECT (CASE ? > ? " +
		"WHEN true THEN 'T' " +
		"END) AS complexCase " +
		"FROM table"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{10, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoVal(t *testing.T) {
	caseStmt := Case().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "SELECT CASE " +
		"WHEN x = ? THEN x is zero " +
		"WHEN x > ? THEN CONCAT('x is greater than ', ?) " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithExpr(t *testing.T) {
	caseStmt := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "SELECT CASE x = ? " +
		"WHEN true THEN ? " +
		"ELSE 42 " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true, "it's true!"}
	assert.Equal(t, expectedArgs, args)
}

func TestMultipleCase(t *testing.T) {
	caseStmtNoval := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")
	caseStmtExpr := Case().
		When(Eq{"x": 0}, "'x is zero'").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().
		Column(Alias(caseStmtNoval, "case_noval")).
		Column(Alias(caseStmtExpr, "case_expr")).
		From("table")

	sql, args, err := qb.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "SELECT " +
		"(CASE x = ? WHEN true THEN ? ELSE 42 END) AS case_noval, " +
		"(CASE WHEN x = ? THEN 'x is zero' WHEN x > ? THEN CONCAT('x is greater than ', ?) END) AS case_expr " +
		"FROM table"

	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{
		true, "it's true!",
		0, 1, 2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoWhenClause(t *testing.T) {
	caseStmt := Case("something").
		Else("42")

	qb := Select().Column(caseStmt).From("table")

	_, _, err := qb.ToSQL()

	assert.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}

func TestCaseWithIntValues(t *testing.T) {
	// GitHub #388: non-string values in When/Then should be auto-wrapped
	// as parameterized placeholders.
	caseStmt := Case("order_no").
		When("ORD001", 500).
		When("ORD002", 300).
		Else(0)

	sql, args, err := caseStmt.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "CASE order_no " +
		"WHEN ORD001 THEN ? " +
		"WHEN ORD002 THEN ? " +
		"ELSE ? " +
		"END"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{500, 300, 0}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithIntWhenAndThen(t *testing.T) {
	// Both WHEN and THEN can be non-string values.
	caseStmt := Case("status").
		When(1, "active").
		When(2, "inactive")

	sql, args, err := caseStmt.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "CASE status " +
		"WHEN ? THEN active " +
		"WHEN ? THEN inactive " +
		"END"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithFloat64Values(t *testing.T) {
	caseStmt := Case("score").
		When(1.5, "low").
		When(3.5, "high").
		Else("unknown")

	sql, args, err := caseStmt.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "CASE score " +
		"WHEN ? THEN low " +
		"WHEN ? THEN high " +
		"ELSE unknown " +
		"END"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1.5, 3.5}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithBoolValues(t *testing.T) {
	caseStmt := Case().
		When(Eq{"active": true}, 1).
		Else(0)

	sql, args, err := caseStmt.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "CASE " +
		"WHEN active = ? THEN ? " +
		"ELSE ? " +
		"END"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true, 1, 0}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithMixedNonStringValues(t *testing.T) {
	// Mix of string, int, Sqlizer, and float in When/Then/Else.
	caseStmt := Case().
		When(Eq{"x": 0}, 100).
		When("x > 10", 200.5).
		Else(Expr("?", "fallback"))

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSQL()

	assert.NoError(t, err)

	expectedSQL := "SELECT CASE " +
		"WHEN x = ? THEN ? " +
		"WHEN x > 10 THEN ? " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 100, 200.5, "fallback"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCaseBuilderMustSql should have panicked!")
		}
	}()
	Case("").MustSQL()
}
