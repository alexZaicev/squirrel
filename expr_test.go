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

func TestEqNilSliceToSql(t *testing.T) {
	// GitHub #277: nil slice should produce IS NULL, not (1=0).
	var ids []uint64
	b := Eq{"id": ids}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "id IS NULL", sql)
	assert.Empty(t, args)
}

func TestNotEqNilSliceToSql(t *testing.T) {
	// GitHub #277: nil slice with NotEq should produce IS NOT NULL.
	var ids []int
	b := NotEq{"id": ids}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "id IS NOT NULL", sql)
	assert.Empty(t, args)
}

func TestEqNilSliceMultiKey(t *testing.T) {
	// GitHub #277: nil slice combined with other keys.
	var ids []int
	b := Eq{"id": ids, "name": "test"}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(id IS NULL AND name = ?)", sql)
	assert.Equal(t, []any{"test"}, args)
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

	expectedSQL := "(age BETWEEN ? AND ? AND price BETWEEN ? AND ?)"
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
	assert.Empty(t, sql)
	assert.Empty(t, args)
}

func TestNilAndToSql(t *testing.T) {
	var a And
	sql, args, err := a.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, sql)
	assert.Empty(t, args)
}

func TestEmptyOrToSql(t *testing.T) {
	sql, args, err := Or{}.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, sql)
	assert.Empty(t, args)
}

func TestNilOrToSql(t *testing.T) {
	var o Or
	sql, args, err := o.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, sql)
	assert.Empty(t, args)
}

func TestNilOrInWhereProducesNoFilter(t *testing.T) {
	// GitHub #382 — nil Or in Where should produce no WHERE clause, not WHERE (1=0).
	var filters Or
	sql, args, err := Select("*").From("users").Where(filters).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
	assert.Empty(t, args)
}

func TestNilAndInWhereProducesNoFilter(t *testing.T) {
	// GitHub #382 — nil And in Where should produce no WHERE clause.
	var filters And
	sql, args, err := Select("*").From("users").Where(filters).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
	assert.Empty(t, args)
}

func TestEmptyOrInWhereProducesNoFilter(t *testing.T) {
	// GitHub #382 — empty Or{} in Where should produce no WHERE clause.
	sql, args, err := Select("*").From("users").Where(Or{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
	assert.Empty(t, args)
}

func TestEmptyAndInWhereProducesNoFilter(t *testing.T) {
	// GitHub #382 — empty And{} in Where should produce no WHERE clause.
	sql, args, err := Select("*").From("users").Where(And{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
	assert.Empty(t, args)
}

func TestNilOrFollowedByConditionInWhere(t *testing.T) {
	// GitHub #382 — nil Or followed by a real condition should produce only the real condition.
	var filters Or
	sql, args, err := Select("*").From("users").Where(filters).Where(Eq{"active": true}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users WHERE active = ?", sql)
	assert.Equal(t, []any{true}, args)
}

func TestConditionFollowedByNilOrInWhere(t *testing.T) {
	// GitHub #382 — real condition followed by nil Or should produce only the real condition.
	var filters Or
	sql, args, err := Select("*").From("users").Where(Eq{"active": true}).Where(filters).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users WHERE active = ?", sql)
	assert.Equal(t, []any{true}, args)
}

func TestNilOrWithDollarPlaceholders(t *testing.T) {
	var filters Or
	sql, args, err := Select("*").From("users").
		Where(filters).
		Where(Eq{"active": true}).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users WHERE active = $1", sql)
	assert.Equal(t, []any{true}, args)
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

func TestOrWithMultiKeyEqToSql(t *testing.T) {
	// GitHub #269 — multi-key Eq inside Or must be parenthesized.
	b := Or{Eq{"a": 1, "b": 2}, Eq{"c": 3, "d": 4}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "((a = ? AND b = ?) OR (c = ? AND d = ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestOrWithMultiKeyNotEqToSql(t *testing.T) {
	b := Or{NotEq{"a": 1, "b": 2}, NotEq{"c": 3, "d": 4}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "((a <> ? AND b <> ?) OR (c <> ? AND d <> ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestOrWithMultiKeyLtToSql(t *testing.T) {
	b := Or{Lt{"a": 1, "b": 2}, Gt{"c": 3, "d": 4}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "((a < ? AND b < ?) OR (c > ? AND d > ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestOrWithMultiKeyBetweenToSql(t *testing.T) {
	b := Or{
		Between{"a": [2]interface{}{1, 10}, "b": [2]interface{}{20, 30}},
		Between{"c": [2]interface{}{40, 50}},
	}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "((a BETWEEN ? AND ? AND b BETWEEN ? AND ?) OR c BETWEEN ? AND ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 10, 20, 30, 40, 50}
	assert.Equal(t, expectedArgs, args)
}

func TestOrWithMixedMultiKeySingleKeyEqToSql(t *testing.T) {
	// One multi-key Eq (parenthesized) and one single-key Eq (not parenthesized).
	b := Or{Eq{"a": 1, "b": 2}, Eq{"c": 3}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "((a = ? AND b = ?) OR c = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestAndWithMultiKeyEqToSql(t *testing.T) {
	// Multi-key Eq inside And — parenthesization is still correct.
	b := And{Eq{"a": 1, "b": 2}, Eq{"c": 3, "d": 4}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "((a = ? AND b = ?) AND (c = ? AND d = ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestOrWithMultiKeyEqInSelectWhereToSql(t *testing.T) {
	// Full integration with SelectBuilder.
	b := Select("*").From("t").Where(Or{
		Eq{"col1": 1, "col2": 2},
		Eq{"col1": 3, "col2": 4},
	})
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM t WHERE ((col1 = ? AND col2 = ?) OR (col1 = ? AND col2 = ?))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestOrWithMultiKeyEqDollarPlaceholders(t *testing.T) {
	b := Select("*").From("t").Where(Or{
		Eq{"a": 1, "b": 2},
		Eq{"c": 3, "d": 4},
	}).PlaceholderFormat(Dollar)
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * FROM t WHERE ((a = $1 AND b = $2) OR (c = $3 AND d = $4))"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestSingleKeyEqStillUnparenthesized(t *testing.T) {
	// Single-key Eq should NOT be wrapped in parentheses.
	b := Eq{"a": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "a = ?", sql)
	assert.Equal(t, []any{1}, args)
}

func TestSingleKeyLtStillUnparenthesized(t *testing.T) {
	// Single-key Lt should NOT be wrapped in parentheses.
	b := Lt{"a": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, "a < ?", sql)
	assert.Equal(t, []any{1}, args)
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

	expectedSQL := "(a = ? AND b = ? AND c = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestSqlLtOrder(t *testing.T) {
	b := Lt{"a": 1, "b": 2, "c": 3}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "(a < ? AND b < ? AND c < ?)"
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

	expectedSQL := "(active = ? AND user_id IN (SELECT id FROM active_users))"
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

// Tests for issue #351 / #285: Misplaced parameters with window functions /
// complex subqueries. Wrapper types (Alias, Expr, ConcatExpr) must implement
// rawSqlizer so that outer queries get raw ? placeholders from nested builders,
// preventing double placeholder formatting.

func TestAliasSubqueryDollarPlaceholders(t *testing.T) {
	// Alias wrapping a subquery with Dollar format must produce correct
	// sequential numbering when used inside an outer Dollar-formatted query.
	inner := Select("id").From("orders").Where(Eq{"status": "active"}).PlaceholderFormat(Dollar)
	outer := Select("name").
		Column(Alias(inner, "order_ids")).
		From("users").
		Where(Eq{"age": 25}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT name, (SELECT id FROM orders WHERE status = $1) AS order_ids FROM users WHERE age = $2", sql)
	assert.Equal(t, []any{"active", 25}, args)
}

func TestAliasSubqueryDollarMultipleColumns(t *testing.T) {
	// Multiple aliased subqueries in columns with Dollar format.
	sub1 := Select("COUNT(*)").From("orders").Where(Eq{"user_id": 1}).PlaceholderFormat(Dollar)
	sub2 := Select("SUM(amount)").From("payments").Where(Eq{"user_id": 2}).PlaceholderFormat(Dollar)
	outer := Select("name").
		Column(Alias(sub1, "order_count")).
		Column(Alias(sub2, "total_paid")).
		From("users").
		Where(Eq{"active": true}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = $1) AS order_count, "+
			"(SELECT SUM(amount) FROM payments WHERE user_id = $2) AS total_paid "+
			"FROM users WHERE active = $3",
		sql)
	assert.Equal(t, []any{1, 2, true}, args)
}

func TestExprSubqueryDollarPlaceholders(t *testing.T) {
	// Expr with a Sqlizer arg (subquery) in Dollar mode must produce correct
	// sequential numbering.
	inner := Select("MAX(score)").From("scores").Where(Eq{"game": "chess"}).PlaceholderFormat(Dollar)
	outer := Select("*").
		From("users").
		Where(Expr("score > (?)", inner)).
		Where(Eq{"active": true}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users WHERE score > (SELECT MAX(score) FROM scores WHERE game = $1) AND active = $2", sql)
	assert.Equal(t, []any{"chess", true}, args)
}

func TestExprMultipleSubqueryArgsDollar(t *testing.T) {
	// Expr with multiple Sqlizer args in Dollar mode.
	sub1 := Select("MIN(price)").From("products").Where(Eq{"cat": "A"}).PlaceholderFormat(Dollar)
	sub2 := Select("MAX(price)").From("products").Where(Eq{"cat": "B"}).PlaceholderFormat(Dollar)
	outer := Select("*").
		From("items").
		Where(Expr("price BETWEEN (?) AND (?)", sub1, sub2)).
		Where(Eq{"in_stock": true}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT * FROM items "+
			"WHERE price BETWEEN (SELECT MIN(price) FROM products WHERE cat = $1) "+
			"AND (SELECT MAX(price) FROM products WHERE cat = $2) "+
			"AND in_stock = $3",
		sql)
	assert.Equal(t, []any{"A", "B", true}, args)
}

func TestConcatExprSubqueryDollarPlaceholders(t *testing.T) {
	// ConcatExpr with a Sqlizer that has Dollar format must produce correct
	// sequential numbering when nested inside an outer Dollar-formatted query.
	inner := Select("name").From("categories").Where(Eq{"id": 5}).PlaceholderFormat(Dollar)
	ce := ConcatExpr("COALESCE(cat_name, (", inner, "))")
	outer := Select("id").
		Column(ce).
		From("products").
		Where(Eq{"active": true}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT id, COALESCE(cat_name, (SELECT name FROM categories WHERE id = $1)) "+
			"FROM products WHERE active = $2",
		sql)
	assert.Equal(t, []any{5, true}, args)
}

func TestAliasConcatExprNestedDollar(t *testing.T) {
	// Alias wrapping a ConcatExpr that contains a Dollar-formatted subquery.
	inner := Select("AVG(rating)").From("reviews").Where(Eq{"product_id": 10}).PlaceholderFormat(Dollar)
	ce := ConcatExpr("COALESCE(", inner, ", 0)")
	outer := Select("name").
		Column(Alias(ce, "avg_rating")).
		From("products").
		Where(Eq{"active": true}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT name, (COALESCE(SELECT AVG(rating) FROM reviews WHERE product_id = $1, 0)) AS avg_rating "+
			"FROM products WHERE active = $2",
		sql)
	assert.Equal(t, []any{10, true}, args)
}

func TestPrefixExprSubqueryDollarPlaceholders(t *testing.T) {
	// Prefix/Suffix with Expr wrapping a subquery in Dollar mode.
	sub := Select("id").From("active_users").Where(Eq{"status": "active"}).PlaceholderFormat(Dollar)
	outer := Select("*").
		Prefix("WITH cte AS (?)", sub).
		From("cte").
		Where(Eq{"role": "admin"}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"WITH cte AS (SELECT id FROM active_users WHERE status = $1) "+
			"SELECT * FROM cte WHERE role = $2",
		sql)
	assert.Equal(t, []any{"active", "admin"}, args)
}

func TestSuffixExprSubqueryDollarPlaceholders(t *testing.T) {
	// Suffix with Expr wrapping a subquery in Dollar mode.
	sub := Select("1").From("audit").Where(Eq{"user_id": 99}).PlaceholderFormat(Dollar)
	outer := Select("*").
		From("users").
		Where(Eq{"active": true}).
		Suffix("AND EXISTS (?)", sub).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT * FROM users WHERE active = $1 AND EXISTS (SELECT 1 FROM audit WHERE user_id = $2)",
		sql)
	assert.Equal(t, []any{true, 99}, args)
}

func TestColumnExprSubqueryDollarPlaceholders(t *testing.T) {
	// Column with Expr("(?) AS alias", subquery) pattern in Dollar mode.
	sub := Select("COUNT(*)").From("orders").Where(Eq{"user_id": 7}).PlaceholderFormat(Dollar)
	outer := Select("name").
		Column(Expr("(?) AS order_count", sub)).
		From("users").
		Where(Eq{"active": true}).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = $1) AS order_count "+
			"FROM users WHERE active = $2",
		sql)
	assert.Equal(t, []any{7, true}, args)
}

func TestComplexNestedSubqueriesDollarPlaceholders(t *testing.T) {
	// Complex scenario: multiple subqueries in different positions (columns,
	// WHERE, prefix, suffix) all with Dollar format — tests global placeholder
	// ordering across the entire query.
	colSub := Select("COUNT(*)").From("orders").Where(Eq{"uid": 1}).PlaceholderFormat(Dollar)
	whereSub := Select("id").From("blocked").Where(Eq{"reason": "spam"}).PlaceholderFormat(Dollar)
	prefixSub := Select("id").From("vip").Where(Eq{"level": 3}).PlaceholderFormat(Dollar)

	outer := Select("name").
		Column(Alias(colSub, "cnt")).
		Prefix("WITH vips AS (?)", prefixSub).
		From("users").
		Where(Eq{"active": true}).
		Where(Expr("id NOT IN (?)", whereSub)).
		PlaceholderFormat(Dollar)

	sql, args, err := outer.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		"WITH vips AS (SELECT id FROM vip WHERE level = $1) "+
			"SELECT name, (SELECT COUNT(*) FROM orders WHERE uid = $2) AS cnt "+
			"FROM users WHERE active = $3 AND id NOT IN (SELECT id FROM blocked WHERE reason = $4)",
		sql)
	assert.Equal(t, []any{3, 1, true, "spam"}, args)
}

func TestExprRawSqlizerInterface(t *testing.T) {
	// Verify that expr implements rawSqlizer.
	e := Expr("x = ?", 1)
	_, ok := e.(rawSqlizer)
	assert.True(t, ok, "expr should implement rawSqlizer")
}

func TestAliasRawSqlizerInterface(t *testing.T) {
	// Verify that aliasExpr implements rawSqlizer.
	a := Alias(Expr("1"), "one")
	_, ok := a.(rawSqlizer)
	assert.True(t, ok, "aliasExpr should implement rawSqlizer")
}

func TestConcatExprRawSqlizerInterface(t *testing.T) {
	// Verify that concatExpr implements rawSqlizer.
	ce := ConcatExpr("a", Expr("b"))
	_, ok := ce.(rawSqlizer)
	assert.True(t, ok, "concatExpr should implement rawSqlizer")
}

// ---------------------------------------------------------------------------
// valuesExpr
// ---------------------------------------------------------------------------

func TestValuesExprBasic(t *testing.T) {
	v := valuesExpr{
		rows:    [][]interface{}{{1, "Alice"}, {2, "Bob"}},
		alias:   "v",
		columns: []string{"id", "name"},
	}
	sql, args, err := v.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(VALUES (?::bigint, ?::text), (?, ?)) AS v(id, name)", sql)
	assert.Equal(t, []interface{}{1, "Alice", 2, "Bob"}, args)
}

func TestValuesExprNoColumns(t *testing.T) {
	v := valuesExpr{
		rows:  [][]interface{}{{1, "x"}},
		alias: "v",
	}
	sql, args, err := v.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(VALUES (?::bigint, ?::text)) AS v", sql)
	assert.Equal(t, []interface{}{1, "x"}, args)
}

func TestValuesExprEmptyRowsError(t *testing.T) {
	v := valuesExpr{
		rows:  [][]interface{}{},
		alias: "v",
	}
	_, _, err := v.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one row")
}

func TestValuesExprEmptyAliasError(t *testing.T) {
	v := valuesExpr{
		rows:  [][]interface{}{{1}},
		alias: "",
	}
	_, _, err := v.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "alias")
}

func TestValuesExprRawSqlizerInterface(t *testing.T) {
	v := valuesExpr{
		rows:  [][]interface{}{{1}},
		alias: "v",
	}
	_, ok := interface{}(v).(rawSqlizer)
	assert.True(t, ok, "valuesExpr should implement rawSqlizer")
}

func TestValuesExprToSQLRaw(t *testing.T) {
	v := valuesExpr{
		rows:    [][]interface{}{{1, "a"}},
		alias:   "v",
		columns: []string{"id", "name"},
	}
	sql, args, err := v.toSQLRaw()
	assert.NoError(t, err)
	assert.Equal(t, "(VALUES (?::bigint, ?::text)) AS v(id, name)", sql)
	assert.Equal(t, []interface{}{1, "a"}, args)
}

func TestValuesExprWithSqlizerValue(t *testing.T) {
	v := valuesExpr{
		rows:    [][]interface{}{{1, Expr("NOW()")}},
		alias:   "v",
		columns: []string{"id", "ts"},
	}
	sql, args, err := v.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(VALUES (?::bigint, NOW())) AS v(id, ts)", sql)
	assert.Equal(t, []interface{}{1}, args)
}
