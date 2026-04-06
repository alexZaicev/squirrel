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
