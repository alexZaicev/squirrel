package squirrel

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConcatExpr(t *testing.T) {
	b := ConcatExpr("COALESCE(name,", Expr("CONCAT(?,' ',?)", "f", "l"), ")")
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "COALESCE(name,CONCAT(?,' ',?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"f", "l"}
	assert.Equal(t, expectedArgs, args)
}

func TestConcatExprBadType(t *testing.T) {
	b := ConcatExpr("prefix", 123, "suffix")
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "123 is not")
}

func TestEqToSql(t *testing.T) {
	b := Eq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqEmptyToSql(t *testing.T) {
	sql, args, err := Eq{}.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(1=1)"
	assert.Equal(t, expectedSQL, sql)
	assert.Empty(t, args)
}

func TestEqInToSql(t *testing.T) {
	b := Eq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id IN (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqToSql(t *testing.T) {
	b := NotEq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id <> ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqNotInToSql(t *testing.T) {
	b := NotEq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id NOT IN (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInEmptyToSql(t *testing.T) {
	b := Eq{"id": []int{}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(1=0)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqInEmptyToSql(t *testing.T) {
	b := NotEq{"id": []int{}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(1=1)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestEqBytesToSql(t *testing.T) {
	b := Eq{"id": []byte("test")}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{[]byte("test")}
	assert.Equal(t, expectedArgs, args)
}

func TestLtToSql(t *testing.T) {
	b := Lt{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id < ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestLtOrEqToSql(t *testing.T) {
	b := LtOrEq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id <= ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtToSql(t *testing.T) {
	b := Gt{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id > ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtOrEqToSql(t *testing.T) {
	b := GtOrEq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id >= ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestExprNilToSql(t *testing.T) {
	var b Sqlizer
	b = NotEq{"name": nil}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSQL := "name IS NOT NULL"
	assert.Equal(t, expectedSQL, sql)

	b = Eq{"name": nil}
	sql, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSQL = "name IS NULL"
	assert.Equal(t, expectedSQL, sql)
}

func TestNullTypeString(t *testing.T) {
	var b Sqlizer
	var name sql.NullString

	b = Eq{"name": name}
	sql, args, err := b.ToSQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	err = name.Scan("Name")
	assert.NoError(t, err)
	b = Eq{"name": name}
	sql, args, err = b.ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{"Name"}, args)
	assert.Equal(t, "name = ?", sql)
}

func TestNullTypeInt64(t *testing.T) {
	var userID sql.NullInt64
	err := userID.Scan(nil)
	assert.NoError(t, err)
	b := Eq{"user_id": userID}
	sql, args, err := b.ToSQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "user_id IS NULL", sql)

	err = userID.Scan(int64(10))
	assert.NoError(t, err)
	b = Eq{"user_id": userID}
	sql, args, err = b.ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{int64(10)}, args)
	assert.Equal(t, "user_id = ?", sql)
}

func TestNilPointer(t *testing.T) {
	var name *string
	eq := Eq{"name": name}
	sql, args, err := eq.ToSQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	neq := NotEq{"name": name}
	sql, args, err = neq.ToSQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NOT NULL", sql)

	var ids *[]int
	eq = Eq{"id": ids}
	sql, args, err = eq.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NULL", sql)

	neq = NotEq{"id": ids}
	sql, args, err = neq.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NOT NULL", sql)

	var ida *[3]int
	eq = Eq{"id": ida}
	sql, args, err = eq.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NULL", sql)

	neq = NotEq{"id": ida}
	sql, args, err = neq.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NOT NULL", sql)
}

func TestNotNilPointer(t *testing.T) {
	c := "Name"
	name := &c
	eq := Eq{"name": name}
	sql, args, err := eq.ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{"Name"}, args)
	assert.Equal(t, "name = ?", sql)

	neq := NotEq{"name": name}
	sql, args, err = neq.ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{"Name"}, args)
	assert.Equal(t, "name <> ?", sql)

	s := []int{1, 2, 3}
	ids := &s
	eq = Eq{"id": ids}
	sql, args, err = eq.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id IN (?,?,?)", sql)

	neq = NotEq{"id": ids}
	sql, args, err = neq.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id NOT IN (?,?,?)", sql)

	a := [3]int{1, 2, 3}
	ida := &a
	eq = Eq{"id": ida}
	sql, args, err = eq.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id IN (?,?,?)", sql)

	neq = NotEq{"id": ida}
	sql, args, err = neq.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id NOT IN (?,?,?)", sql)
}

func TestEmptyAndToSql(t *testing.T) {
	sql, args, err := And{}.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(1=1)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestEmptyOrToSql(t *testing.T) {
	sql, args, err := Or{}.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(1=0)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestLikeToSql(t *testing.T) {
	b := Like{"name": "%irrel"}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "name LIKE ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"%irrel"}
	assert.Equal(t, expectedArgs, args)
}

func TestNotLikeToSql(t *testing.T) {
	b := NotLike{"name": "%irrel"}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "name NOT LIKE ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"%irrel"}
	assert.Equal(t, expectedArgs, args)
}

func TestILikeToSql(t *testing.T) {
	b := ILike{"name": "sq%"}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "name ILIKE ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"sq%"}
	assert.Equal(t, expectedArgs, args)
}

func TestNotILikeToSql(t *testing.T) {
	b := NotILike{"name": "sq%"}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "name NOT ILIKE ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"sq%"}
	assert.Equal(t, expectedArgs, args)
}

func TestSqlEqOrder(t *testing.T) {
	b := Eq{"a": 1, "b": 2, "c": 3}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "a = ? AND b = ? AND c = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestSqlLtOrder(t *testing.T) {
	b := Lt{"a": 1, "b": 2, "c": 3}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "a < ? AND b < ? AND c < ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestExprEscaped(t *testing.T) {
	b := Expr("count(??)", Expr("x"))
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "count(??)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{Expr("x")}
	assert.Equal(t, expectedArgs, args)
}

func TestExprRecursion(t *testing.T) {
	{
		b := Expr("count(?)", Expr("nullif(a,?)", "b"))
		sql, args, err := b.ToSQL()
		assert.NoError(t, err)

		expectedSQL := "count(nullif(a,?))"
		assert.Equal(t, expectedSQL, sql)

		expectedArgs := []any{"b"}
		assert.Equal(t, expectedArgs, args)
	}
	{
		b := Expr("extract(? from ?)", Expr("epoch"), "2001-02-03")
		sql, args, err := b.ToSQL()
		assert.NoError(t, err)

		expectedSQL := "extract(epoch from ?)"
		assert.Equal(t, expectedSQL, sql)

		expectedArgs := []any{"2001-02-03"}
		assert.Equal(t, expectedArgs, args)
	}
	{
		b := Expr("JOIN t1 ON ?", And{Eq{"id": 1}, Expr("NOT c1"), Expr("? @@ ?", "x", "y")})
		sql, args, err := b.ToSQL()
		assert.NoError(t, err)

		expectedSQL := "JOIN t1 ON (id = ? AND NOT c1 AND ? @@ ?)"
		assert.Equal(t, expectedSQL, sql)

		expectedArgs := []any{1, "x", "y"}
		assert.Equal(t, expectedArgs, args)
	}
}

func ExampleEq() {
	Select("id", "created", "first_name").From("users").Where(Eq{
		"company": 20,
	})
}

func TestEqSubqueryToSql(t *testing.T) {
	subQ := Select("id").From("other_table").Where(Eq{"active": true})
	b := Eq{"id": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id IN (SELECT id FROM other_table WHERE active = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqSubqueryToSql(t *testing.T) {
	subQ := Select("id").From("blocked_users")
	b := NotEq{"user_id": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "user_id NOT IN (SELECT id FROM blocked_users)"
	assert.Equal(t, expectedSQL, sql)

	assert.Empty(t, args)
}

func TestEqSubqueryWithArgsToSql(t *testing.T) {
	subQ := Select("id").From("posts").Where(And{
		Eq{"status": "published"},
		Gt{"views": 100},
	})
	b := Eq{"post_id": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "post_id IN (SELECT id FROM posts WHERE (status = ? AND views > ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"published", 100}
	assert.Equal(t, expectedArgs, args)
}

func TestEqSubqueryWithMultipleKeysToSql(t *testing.T) {
	subQ := Select("id").From("active_users")
	b := Eq{"active": true, "user_id": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "active = ? AND user_id IN (SELECT id FROM active_users)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true}
	assert.Equal(t, expectedArgs, args)
}

func TestEqSubqueryInSelectWhere(t *testing.T) {
	subQ := Select("id").From("departments").Where(Eq{"name": "Engineering"})
	b := Select("*").From("employees").Where(Eq{"department_id": subQ})
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"Engineering"}
	assert.Equal(t, expectedArgs, args)
}

func TestEqSubqueryWithDollarPlaceholders(t *testing.T) {
	subQ := Select("id").From("other_table").Where(Eq{"active": true})
	b := Select("*").From("main_table").
		Where(Eq{"status": "open"}).
		Where(Eq{"id": subQ}).
		PlaceholderFormat(Dollar)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM main_table WHERE status = $1 AND id IN (SELECT id FROM other_table WHERE active = $2)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"open", true}
	assert.Equal(t, expectedArgs, args)
}

func TestLtSubqueryToSql(t *testing.T) {
	subQ := Select("AVG(price)").From("products")
	b := Lt{"price": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "price < (SELECT AVG(price) FROM products)"
	assert.Equal(t, expectedSQL, sql)

	assert.Empty(t, args)
}

func TestGtSubqueryToSql(t *testing.T) {
	subQ := Select("AVG(score)").From("results").Where(Eq{"subject": "math"})
	b := Gt{"score": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "score > (SELECT AVG(score) FROM results WHERE subject = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"math"}
	assert.Equal(t, expectedArgs, args)
}

func TestLtOrEqSubqueryToSql(t *testing.T) {
	subQ := Select("MAX(age)").From("users")
	b := LtOrEq{"age": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "age <= (SELECT MAX(age) FROM users)"
	assert.Equal(t, expectedSQL, sql)

	assert.Empty(t, args)
}

func TestGtOrEqSubqueryToSql(t *testing.T) {
	subQ := Select("MIN(salary)").From("employees").Where(Eq{"department": "sales"})
	b := GtOrEq{"salary": subQ}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "salary >= (SELECT MIN(salary) FROM employees WHERE department = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"sales"}
	assert.Equal(t, expectedArgs, args)
}
