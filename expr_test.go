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

func TestBetweenToSql(t *testing.T) {
	b := Between{"age": [2]interface{}{18, 65}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "age BETWEEN ? AND ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{18, 65}
	assert.Equal(t, expectedArgs, args)
}

func TestNotBetweenToSql(t *testing.T) {
	b := NotBetween{"age": [2]interface{}{18, 65}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "age NOT BETWEEN ? AND ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{18, 65}
	assert.Equal(t, expectedArgs, args)
}

func TestBetweenEmptyToSql(t *testing.T) {
	sql, args, err := Between{}.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(1=1)"
	assert.Equal(t, expectedSQL, sql)
	assert.Empty(t, args)
}

func TestBetweenMultipleKeysToSql(t *testing.T) {
	b := Between{"age": [2]interface{}{18, 65}, "price": [2]interface{}{10, 100}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "age BETWEEN ? AND ? AND price BETWEEN ? AND ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{18, 65, 10, 100}
	assert.Equal(t, expectedArgs, args)
}

func TestBetweenWithStringValues(t *testing.T) {
	b := Between{"created_at": [2]interface{}{"2024-01-01", "2024-12-31"}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "created_at BETWEEN ? AND ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"2024-01-01", "2024-12-31"}
	assert.Equal(t, expectedArgs, args)
}

func TestBetweenWithSliceValue(t *testing.T) {
	b := Between{"id": []interface{}{1, 10}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id BETWEEN ? AND ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 10}
	assert.Equal(t, expectedArgs, args)
}

func TestBetweenNullError(t *testing.T) {
	b := Between{"age": nil}
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "null")
}

func TestBetweenWrongSizeError(t *testing.T) {
	b := Between{"age": [3]interface{}{1, 2, 3}}
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 2 elements")
}

func TestBetweenNonArrayError(t *testing.T) {
	b := Between{"age": 42}
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "two-element array or slice")
}

func TestBetweenInSelectWhereToSql(t *testing.T) {
	b := Select("*").From("users").Where(Between{"age": [2]interface{}{18, 65}})
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE age BETWEEN ? AND ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{18, 65}
	assert.Equal(t, expectedArgs, args)
}

func TestBetweenWithDollarPlaceholders(t *testing.T) {
	b := Select("*").From("users").
		Where(Eq{"active": true}).
		Where(Between{"age": [2]interface{}{18, 65}}).
		PlaceholderFormat(Dollar)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE active = $1 AND age BETWEEN $2 AND $3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true, 18, 65}
	assert.Equal(t, expectedArgs, args)
}

func TestBetweenCombinedWithAndToSql(t *testing.T) {
	b := And{Eq{"active": true}, Between{"age": [2]interface{}{18, 65}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(active = ? AND age BETWEEN ? AND ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true, 18, 65}
	assert.Equal(t, expectedArgs, args)
}

func TestNotBetweenInSelectWhereToSql(t *testing.T) {
	b := Select("*").From("users").Where(NotBetween{"age": [2]interface{}{18, 65}})
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE age NOT BETWEEN ? AND ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{18, 65}
	assert.Equal(t, expectedArgs, args)
}

func TestBetweenWithTypedIntSlice(t *testing.T) {
	b := Between{"id": []int{1, 10}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "id BETWEEN ? AND ?", sql)
	assert.Equal(t, []any{1, 10}, args)
}

func TestBetweenOneElementSliceError(t *testing.T) {
	b := Between{"age": []interface{}{1}}
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 2 elements")
}

func TestBetweenEmptySliceError(t *testing.T) {
	b := Between{"age": []interface{}{}}
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 2 elements")
}

func TestNotBetweenEmptyToSql(t *testing.T) {
	sql, args, err := NotBetween{}.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "(1=1)", sql)
	assert.Empty(t, args)
}

func TestNotBetweenWithDollarPlaceholders(t *testing.T) {
	b := Select("*").From("users").
		Where(Eq{"active": true}).
		Where(NotBetween{"age": [2]interface{}{18, 65}}).
		PlaceholderFormat(Dollar)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "SELECT * FROM users WHERE active = $1 AND age NOT BETWEEN $2 AND $3", sql)
	assert.Equal(t, []any{true, 18, 65}, args)
}

func TestNotBetweenCombinedWithAndToSql(t *testing.T) {
	b := And{Eq{"active": true}, NotBetween{"age": [2]interface{}{18, 65}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "(active = ? AND age NOT BETWEEN ? AND ?)", sql)
	assert.Equal(t, []any{true, 18, 65}, args)
}

func TestBetweenCombinedWithOrToSql(t *testing.T) {
	b := Or{
		Between{"age": [2]interface{}{18, 30}},
		Between{"age": [2]interface{}{60, 80}},
	}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "(age BETWEEN ? AND ? OR age BETWEEN ? AND ?)", sql)
	assert.Equal(t, []any{18, 30, 60, 80}, args)
}

func TestBetweenCombinedWithNotToSql(t *testing.T) {
	b := Not{Cond: Between{"price": [2]interface{}{10, 50}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "NOT (price BETWEEN ? AND ?)", sql)
	assert.Equal(t, []any{10, 50}, args)
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

func TestNotToSql(t *testing.T) {
	b := Not{Cond: Eq{"active": true}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "NOT (active = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true}
	assert.Equal(t, expectedArgs, args)
}

func TestNotWithOrToSql(t *testing.T) {
	b := Not{Cond: Or{Eq{"a": 1}, Eq{"b": 2}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "NOT ((a = ? OR b = ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestNotWithAndToSql(t *testing.T) {
	b := Not{Cond: And{Eq{"a": 1}, Gt{"b": 2}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "NOT ((a = ? AND b > ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestNotWithExprToSql(t *testing.T) {
	b := Not{Cond: Expr("EXISTS (SELECT 1 FROM users WHERE id = ?)", 42)}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "NOT (EXISTS (SELECT 1 FROM users WHERE id = ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{42}
	assert.Equal(t, expectedArgs, args)
}

func TestNotNilCondToSql(t *testing.T) {
	b := Not{Cond: nil}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(1=1)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestNotWithLikeToSql(t *testing.T) {
	b := Not{Cond: Like{"name": "%irrel"}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "NOT (name LIKE ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"%irrel"}
	assert.Equal(t, expectedArgs, args)
}

func TestDoubleNotToSql(t *testing.T) {
	b := Not{Cond: Not{Cond: Eq{"active": true}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "NOT (NOT (active = ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true}
	assert.Equal(t, expectedArgs, args)
}

func TestNotInSelectWhereToSql(t *testing.T) {
	b := Select("*").From("users").Where(Not{Cond: Eq{"deleted": true}})
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE NOT (deleted = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true}
	assert.Equal(t, expectedArgs, args)
}

func TestNotCombinedWithAndToSql(t *testing.T) {
	b := And{Eq{"active": true}, Not{Cond: Eq{"banned": true}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(active = ? AND NOT (banned = ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true, true}
	assert.Equal(t, expectedArgs, args)
}

func TestNotZeroValueToSql(t *testing.T) {
	// Zero-value Not{} has nil Cond — same as Not{Cond: nil}.
	b := Not{}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(1=1)", sql)
	assert.Equal(t, []any{}, args)
}

func TestNotWithBetweenToSql(t *testing.T) {
	b := Not{Cond: Between{"age": [2]interface{}{18, 65}}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "NOT (age BETWEEN ? AND ?)", sql)
	assert.Equal(t, []any{18, 65}, args)
}

func TestNotWithGtToSql(t *testing.T) {
	b := Not{Cond: Gt{"price": 100}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "NOT (price > ?)", sql)
	assert.Equal(t, []any{100}, args)
}

func TestNotWithLtToSql(t *testing.T) {
	b := Not{Cond: Lt{"price": 50}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "NOT (price < ?)", sql)
	assert.Equal(t, []any{50}, args)
}

func TestOrContainingNotToSql(t *testing.T) {
	b := Or{Not{Cond: Eq{"a": 1}}, Eq{"b": 2}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "(NOT (a = ?) OR b = ?)", sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestNotExistsEquivalenceToSql(t *testing.T) {
	// Not{Cond: Exists(sub)} should produce the same args as NotExists(sub),
	// though the SQL wrapping differs slightly.
	sub := Select("1").From("orders").Where(Eq{"status": "active"})

	notWrapped := Not{Cond: Exists(sub)}
	sql1, args1, err1 := notWrapped.ToSQL()
	assert.NoError(t, err1)
	assert.Equal(t, "NOT (EXISTS (SELECT 1 FROM orders WHERE status = ?))", sql1)

	direct := NotExists(sub)
	sql2, args2, err2 := direct.ToSQL()
	assert.NoError(t, err2)
	assert.Equal(t, "NOT EXISTS (SELECT 1 FROM orders WHERE status = ?)", sql2)

	// Both produce the same args.
	assert.Equal(t, args1, args2)
}

func TestNotWithNotExistsToSql(t *testing.T) {
	// Not{Cond: NotExists(sub)} → double negation
	sub := Select("1").From("orders")
	b := Not{Cond: NotExists(sub)}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "NOT (NOT EXISTS (SELECT 1 FROM orders))", sql)
	assert.Empty(t, args)
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

func TestExistsToSql(t *testing.T) {
	sub := Select("1").From("orders").Where("orders.user_id = users.id")
	b := Exists(sub)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
	assert.Equal(t, expectedSQL, sql)
	assert.Empty(t, args)
}

func TestNotExistsToSql(t *testing.T) {
	sub := Select("1").From("orders").Where("orders.user_id = users.id")
	b := NotExists(sub)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
	assert.Equal(t, expectedSQL, sql)
	assert.Empty(t, args)
}

func TestExistsWithArgsToSql(t *testing.T) {
	sub := Select("1").From("orders").Where(Eq{"status": "active"})
	b := Exists(sub)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "EXISTS (SELECT 1 FROM orders WHERE status = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"active"}
	assert.Equal(t, expectedArgs, args)
}

func TestExistsNilSubqueryError(t *testing.T) {
	b := Exists(nil)
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-nil subquery")
}

func TestNotExistsNilSubqueryError(t *testing.T) {
	b := NotExists(nil)
	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-nil subquery")
}

func TestExistsInSelectWhereToSql(t *testing.T) {
	sub := Select("1").From("orders").Where("orders.user_id = users.id")
	b := Select("*").From("users").Where(Exists(sub))
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
	assert.Equal(t, expectedSQL, sql)
	assert.Empty(t, args)
}

func TestNotExistsInSelectWhereToSql(t *testing.T) {
	sub := Select("1").From("orders").Where("orders.user_id = users.id")
	b := Select("*").From("users").Where(NotExists(sub))
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
	assert.Equal(t, expectedSQL, sql)
	assert.Empty(t, args)
}

func TestExistsWithDollarPlaceholders(t *testing.T) {
	sub := Select("1").From("orders").Where(Eq{"status": "active"})
	b := Select("*").From("users").
		Where(Eq{"role": "admin"}).
		Where(Exists(sub)).
		PlaceholderFormat(Dollar)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE role = $1 AND EXISTS (SELECT 1 FROM orders WHERE status = $2)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"admin", "active"}
	assert.Equal(t, expectedArgs, args)
}

func TestNotExistsWithDollarPlaceholders(t *testing.T) {
	sub := Select("1").From("banned").Where(Eq{"banned.user_id": 42})
	b := Select("*").From("users").
		Where(NotExists(sub)).
		PlaceholderFormat(Dollar)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM banned WHERE banned.user_id = $1)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{42}
	assert.Equal(t, expectedArgs, args)
}

func TestExistsCombinedWithAndToSql(t *testing.T) {
	sub := Select("1").From("orders").Where("orders.user_id = users.id")
	b := And{Eq{"active": true}, Exists(sub)}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(active = ? AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{true}
	assert.Equal(t, expectedArgs, args)
}

func TestExistsCombinedWithOrToSql(t *testing.T) {
	sub := Select("1").From("admins").Where("admins.user_id = users.id")
	b := Or{Eq{"role": "admin"}, Exists(sub)}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(role = ? OR EXISTS (SELECT 1 FROM admins WHERE admins.user_id = users.id))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"admin"}
	assert.Equal(t, expectedArgs, args)
}

func TestNotExistsCombinedWithAndToSql(t *testing.T) {
	sub := Select("1").From("orders").Where("orders.user_id = users.id")
	b := And{Eq{"active": true}, NotExists(sub)}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "(active = ? AND NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id))", sql)
	assert.Equal(t, []any{true}, args)
}

func TestNotExistsCombinedWithOrToSql(t *testing.T) {
	sub := Select("1").From("banned").Where("banned.user_id = users.id")
	b := Or{Eq{"role": "admin"}, NotExists(sub)}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "(role = ? OR NOT EXISTS (SELECT 1 FROM banned WHERE banned.user_id = users.id))", sql)
	assert.Equal(t, []any{"admin"}, args)
}

func TestExistsWithMultipleWhereConditions(t *testing.T) {
	sub := Select("1").From("orders").
		Where("orders.user_id = users.id").
		Where(Eq{"status": "active"}).
		Where(Gt{"total": 100})
	b := Exists(sub)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND status = ? AND total > ?)", sql)
	assert.Equal(t, []any{"active", 100}, args)
}

func TestNotNotExistsDoubleNegationToSql(t *testing.T) {
	// Not{Cond: NotExists(sub)} is semantically equivalent to Exists(sub)
	sub := Select("1").From("orders").Where(Eq{"status": "active"})
	b := Not{Cond: NotExists(sub)}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "NOT (NOT EXISTS (SELECT 1 FROM orders WHERE status = ?))", sql)
	assert.Equal(t, []any{"active"}, args)
}
