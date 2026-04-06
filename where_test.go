package squirrel

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWherePartsAppendToSql(t *testing.T) {
	parts := []Sqlizer{
		newWherePart("x = ?", 1),
		newWherePart(nil),
		newWherePart(Eq{"y": 2}),
	}
	sql := &bytes.Buffer{}
	args, _ := appendToSQL(parts, sql, " AND ", []any{})
	assert.Equal(t, "x = ? AND y = ?", sql.String())
	assert.Equal(t, []any{1, 2}, args)
}

func TestWherePartsAppendToSqlErr(t *testing.T) {
	parts := []Sqlizer{newWherePart(1)}
	_, err := appendToSQL(parts, &bytes.Buffer{}, "", []any{})
	assert.Error(t, err)
}

func TestWherePartsAppendToSqlNilFirst(t *testing.T) {
	parts := []Sqlizer{
		newWherePart(nil),
		newWherePart("x = ?", 1),
	}
	sql := &bytes.Buffer{}
	args, _ := appendToSQL(parts, sql, " AND ", []any{})
	assert.Equal(t, "x = ?", sql.String())
	assert.Equal(t, []any{1}, args)
}

func TestWherePartsAppendToSqlAllNil(t *testing.T) {
	parts := []Sqlizer{
		newWherePart(nil),
		newWherePart(nil),
	}
	sql := &bytes.Buffer{}
	args, _ := appendToSQL(parts, sql, " AND ", []any{})
	assert.Empty(t, sql.String())
	assert.Empty(t, args)
}

func TestWherePartNil(t *testing.T) {
	sql, _, _ := newWherePart(nil).ToSQL()
	assert.Empty(t, sql)
}

func TestWherePartErr(t *testing.T) {
	_, _, err := newWherePart(1).ToSQL()
	assert.Error(t, err)
}

func TestWherePartString(t *testing.T) {
	sql, args, _ := newWherePart("x = ?", 1).ToSQL()
	assert.Equal(t, "x = ?", sql)
	assert.Equal(t, []any{1}, args)
}

func TestWherePartMap(t *testing.T) {
	test := func(pred any) {
		sql, _, _ := newWherePart(pred).ToSQL()
		expect := []string{"(x = ? AND y = ?)", "(y = ? AND x = ?)"}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]any{"x": 1, "y": 2}
	test(m)
	test(Eq(m))
}

func TestWherePartSliceArgExpansion(t *testing.T) {
	// GitHub #383: Where("id NOT IN ?", []int{1,2,3}) should expand the
	// slice into (?,?,?) placeholders.
	sql, args, err := newWherePart("id NOT IN ?", []int{1, 2, 3}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "id NOT IN (?,?,?)", sql)
	assert.Equal(t, []any{1, 2, 3}, args)
}

func TestWherePartSliceArgExpansionIN(t *testing.T) {
	sql, args, err := newWherePart("id IN ?", []string{"a", "b"}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "id IN (?,?)", sql)
	assert.Equal(t, []any{"a", "b"}, args)
}

func TestWherePartSliceArgExpansionMixed(t *testing.T) {
	// Mix of scalar and slice args.
	sql, args, err := newWherePart("x = ? AND id IN ?", 42, []int{1, 2}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "x = ? AND id IN (?,?)", sql)
	assert.Equal(t, []any{42, 1, 2}, args)
}

func TestWherePartSliceArgExpansionEmpty(t *testing.T) {
	// Empty slice should produce ()
	sql, args, err := newWherePart("id IN ?", []int{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "id IN ()", sql)
	assert.Empty(t, args)
}

func TestWherePartSliceArgExpansionSingle(t *testing.T) {
	sql, args, err := newWherePart("id IN ?", []int{42}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "id IN (?)", sql)
	assert.Equal(t, []any{42}, args)
}

func TestWherePartSliceArgNoExpansionBytes(t *testing.T) {
	// []byte should NOT be expanded — database/sql treats it as a single value.
	sql, args, err := newWherePart("data = ?", []byte("hello")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "data = ?", sql)
	assert.Equal(t, []any{[]byte("hello")}, args)
}

func TestWherePartAutoParenOR(t *testing.T) {
	// GitHub #380: raw string with OR should be auto-parenthesized.
	sql, args, err := newWherePart("a = ? OR b = ?", 1, 2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(a = ? OR b = ?)", sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestWherePartAutoParenORCombinedWithAnd(t *testing.T) {
	// Combining an OR clause with another Where clause via appendToSQL.
	parts := []Sqlizer{
		newWherePart("a = ? OR b = ?", 1, 2),
		newWherePart("c = ?", 3),
	}
	buf := &bytes.Buffer{}
	args, err := appendToSQL(parts, buf, " AND ", []any{})
	assert.NoError(t, err)
	assert.Equal(t, "(a = ? OR b = ?) AND c = ?", buf.String())
	assert.Equal(t, []any{1, 2, 3}, args)
}

func TestWherePartNoParenSimple(t *testing.T) {
	// Simple expression without OR should NOT be parenthesized.
	sql, args, err := newWherePart("x = ?", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "x = ?", sql)
	assert.Equal(t, []any{1}, args)
}

func TestWherePartNoParenAND(t *testing.T) {
	// Expression with only AND should NOT be parenthesized (AND is the
	// separator already used between Where parts).
	sql, args, err := newWherePart("x = ? AND y = ?", 1, 2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "x = ? AND y = ?", sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestWherePartAlreadyParenthesized(t *testing.T) {
	// Already fully wrapped in parens — should NOT double-wrap.
	sql, args, err := newWherePart("(a = ? OR b = ?)", 1, 2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(a = ? OR b = ?)", sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestWherePartSliceArgWithOR(t *testing.T) {
	// Both fixes together: slice expansion + auto-paren for OR.
	sql, args, err := newWherePart("id IN ? OR name = ?", []int{1, 2}, "test").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "(id IN (?,?) OR name = ?)", sql)
	assert.Equal(t, []any{1, 2, "test"}, args)
}

func TestWherePartEscapedPlaceholderWithSlice(t *testing.T) {
	// Escaped ?? should be preserved even with slice expansion.
	sql, args, err := newWherePart("x ?? y AND id IN ?", []int{1, 2}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "x ?? y AND id IN (?,?)", sql)
	assert.Equal(t, []any{1, 2}, args)
}
