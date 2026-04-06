package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// QuoteIdent
// ---------------------------------------------------------------------------

func TestQuoteIdentSimple(t *testing.T) {
	id, err := QuoteIdent("users")
	assert.NoError(t, err)
	assert.Equal(t, `"users"`, id.String())
	assert.Equal(t, "users", id.Raw())
}

func TestQuoteIdentSchemaQualified(t *testing.T) {
	id, err := QuoteIdent("public.users")
	assert.NoError(t, err)
	assert.Equal(t, `"public"."users"`, id.String())
	assert.Equal(t, "public.users", id.Raw())
}

func TestQuoteIdentWithSpaces(t *testing.T) {
	id, err := QuoteIdent("my table")
	assert.NoError(t, err)
	assert.Equal(t, `"my table"`, id.String())
}

func TestQuoteIdentWithDoubleQuote(t *testing.T) {
	id, err := QuoteIdent(`Robert"`)
	assert.NoError(t, err)
	assert.Equal(t, `"Robert"""`, id.String())
}

func TestQuoteIdentSQLInjectionAttempt(t *testing.T) {
	// The key test: injection attempts are safely quoted.
	id, err := QuoteIdent(`users; DROP TABLE users; --`)
	assert.NoError(t, err)
	assert.Equal(t, `"users; DROP TABLE users; --"`, id.String())
}

func TestQuoteIdentEmpty(t *testing.T) {
	_, err := QuoteIdent("")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestQuoteIdentEmptyPart(t *testing.T) {
	_, err := QuoteIdent("schema.")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestQuoteIdentMultipleDots(t *testing.T) {
	id, err := QuoteIdent("catalog.schema.table")
	assert.NoError(t, err)
	assert.Equal(t, `"catalog"."schema"."table"`, id.String())
}

func TestMustQuoteIdentSuccess(t *testing.T) {
	id := MustQuoteIdent("users")
	assert.Equal(t, `"users"`, id.String())
}

func TestMustQuoteIdentPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustQuoteIdent should have panicked on empty string")
		}
	}()
	MustQuoteIdent("")
}

// ---------------------------------------------------------------------------
// ValidateIdent
// ---------------------------------------------------------------------------

func TestValidateIdentSimple(t *testing.T) {
	id, err := ValidateIdent("users")
	assert.NoError(t, err)
	assert.Equal(t, "users", id.String())
	assert.Equal(t, "users", id.Raw())
}

func TestValidateIdentSchemaQualified(t *testing.T) {
	id, err := ValidateIdent("public.users")
	assert.NoError(t, err)
	assert.Equal(t, "public.users", id.String())
}

func TestValidateIdentUnderscore(t *testing.T) {
	id, err := ValidateIdent("_my_table_1")
	assert.NoError(t, err)
	assert.Equal(t, "_my_table_1", id.String())
}

func TestValidateIdentEmpty(t *testing.T) {
	_, err := ValidateIdent("")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestValidateIdentWithSpace(t *testing.T) {
	_, err := ValidateIdent("my table")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestValidateIdentWithSemicolon(t *testing.T) {
	_, err := ValidateIdent("users; DROP TABLE users; --")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestValidateIdentWithDash(t *testing.T) {
	_, err := ValidateIdent("my-table")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestValidateIdentStartsWithDigit(t *testing.T) {
	_, err := ValidateIdent("1table")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestValidateIdentWithQuote(t *testing.T) {
	_, err := ValidateIdent(`users"`)
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestValidateIdentWithParens(t *testing.T) {
	_, err := ValidateIdent("users()")
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestMustValidateIdentSuccess(t *testing.T) {
	id := MustValidateIdent("users")
	assert.Equal(t, "users", id.String())
}

func TestMustValidateIdentPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustValidateIdent should have panicked on invalid input")
		}
	}()
	MustValidateIdent("users; DROP TABLE users; --")
}

// ---------------------------------------------------------------------------
// QuoteIdents / ValidateIdents
// ---------------------------------------------------------------------------

func TestQuoteIdents(t *testing.T) {
	ids, err := QuoteIdents("id", "name", "email")
	assert.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Equal(t, `"id"`, ids[0].String())
	assert.Equal(t, `"name"`, ids[1].String())
	assert.Equal(t, `"email"`, ids[2].String())
}

func TestQuoteIdentsError(t *testing.T) {
	_, err := QuoteIdents("id", "", "email")
	assert.Error(t, err)
}

func TestValidateIdents(t *testing.T) {
	ids, err := ValidateIdents("id", "name", "email")
	assert.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Equal(t, "id", ids[0].String())
}

func TestValidateIdentsError(t *testing.T) {
	_, err := ValidateIdents("id", "bad name", "email")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Ident.ToSQL (implements Sqlizer)
// ---------------------------------------------------------------------------

func TestIdentToSQL(t *testing.T) {
	id, err := QuoteIdent("users")
	assert.NoError(t, err)
	sql, args, err := id.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `"users"`, sql)
	assert.Nil(t, args)
}

// ---------------------------------------------------------------------------
// SelectBuilder.SafeFrom
// ---------------------------------------------------------------------------

func TestSelectBuilderSafeFrom(t *testing.T) {
	id, _ := QuoteIdent("users")
	sql, args, err := Select("*").SafeFrom(id).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "users"`, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderSafeFromSchemaQualified(t *testing.T) {
	id, _ := QuoteIdent("public.users")
	sql, args, err := Select("*").SafeFrom(id).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "public"."users"`, sql)
	assert.Nil(t, args)
}

func TestSelectBuilderSafeFromWithInjectionAttempt(t *testing.T) {
	// Even if user manages to pass malicious input, QuoteIdent makes it safe.
	id, _ := QuoteIdent("users; DROP TABLE users; --")
	sql, _, err := Select("*").SafeFrom(id).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "users; DROP TABLE users; --"`, sql)
}

func TestSelectBuilderSafeFromValidated(t *testing.T) {
	id, _ := ValidateIdent("users")
	sql, _, err := Select("*").SafeFrom(id).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
}

// ---------------------------------------------------------------------------
// SelectBuilder.SafeColumns
// ---------------------------------------------------------------------------

func TestSelectBuilderSafeColumns(t *testing.T) {
	cols, _ := QuoteIdents("id", "name", "email")
	sql, _, err := Select().SafeColumns(cols...).From("users").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT "id", "name", "email" FROM users`, sql)
}

// ---------------------------------------------------------------------------
// SelectBuilder.SafeGroupBy
// ---------------------------------------------------------------------------

func TestSelectBuilderSafeGroupBy(t *testing.T) {
	col, _ := QuoteIdent("category")
	sql, _, err := Select("category", "count(*)").From("items").SafeGroupBy(col).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT category, count(*) FROM items GROUP BY "category"`, sql)
}

// ---------------------------------------------------------------------------
// SelectBuilder.SafeOrderBy
// ---------------------------------------------------------------------------

func TestSelectBuilderSafeOrderBy(t *testing.T) {
	col, _ := QuoteIdent("name")
	sql, _, err := Select("*").From("users").SafeOrderBy(col).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM users ORDER BY "name"`, sql)
}

func TestSelectBuilderSafeOrderByMultiple(t *testing.T) {
	col1, _ := QuoteIdent("name")
	col2, _ := QuoteIdent("id")
	sql, _, err := Select("*").From("users").SafeOrderBy(col1, col2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM users ORDER BY "name", "id"`, sql)
}

func TestSelectBuilderSafeOrderByInjectionAttempt(t *testing.T) {
	col, _ := QuoteIdent("name; DROP TABLE users; --")
	sql, _, err := Select("*").From("users").SafeOrderBy(col).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM users ORDER BY "name; DROP TABLE users; --"`, sql)
}

// ---------------------------------------------------------------------------
// SelectBuilder.SafeOrderByDir
// ---------------------------------------------------------------------------

func TestSelectBuilderSafeOrderByDirAsc(t *testing.T) {
	col, _ := QuoteIdent("name")
	sql, _, err := Select("*").From("users").SafeOrderByDir(col, Asc).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM users ORDER BY "name" ASC`, sql)
}

func TestSelectBuilderSafeOrderByDirDesc(t *testing.T) {
	col, _ := QuoteIdent("name")
	sql, _, err := Select("*").From("users").SafeOrderByDir(col, Desc).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM users ORDER BY "name" DESC`, sql)
}

func TestSelectBuilderSafeOrderByDirInvalid(t *testing.T) {
	col, _ := QuoteIdent("name")
	// Invalid direction defaults to no direction.
	sql, _, err := Select("*").From("users").SafeOrderByDir(col, "INVALID").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM users ORDER BY "name"`, sql)
}

// ---------------------------------------------------------------------------
// InsertBuilder.SafeInto
// ---------------------------------------------------------------------------

func TestInsertBuilderSafeInto(t *testing.T) {
	id, _ := QuoteIdent("users")
	sql, args, err := Insert("").SafeInto(id).Columns("name").Values("John").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "users" (name) VALUES (?)`, sql)
	assert.Equal(t, []any{"John"}, args)
}

func TestInsertBuilderSafeIntoInjection(t *testing.T) {
	id, _ := QuoteIdent("users; DROP TABLE users; --")
	sql, _, err := Insert("").SafeInto(id).Columns("name").Values("John").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "users; DROP TABLE users; --" (name) VALUES (?)`, sql)
}

// ---------------------------------------------------------------------------
// InsertBuilder.SafeColumns
// ---------------------------------------------------------------------------

func TestInsertBuilderSafeColumns(t *testing.T) {
	cols, _ := QuoteIdents("id", "name")
	sql, args, err := Insert("users").SafeColumns(cols...).Values(1, "John").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO users ("id","name") VALUES (?,?)`, sql)
	assert.Equal(t, []any{1, "John"}, args)
}

// ---------------------------------------------------------------------------
// UpdateBuilder.SafeTable
// ---------------------------------------------------------------------------

func TestUpdateBuilderSafeTable(t *testing.T) {
	id, _ := QuoteIdent("users")
	sql, args, err := Update("").SafeTable(id).Set("name", "John").Where("id = ?", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "users" SET name = ? WHERE id = ?`, sql)
	assert.Equal(t, []any{"John", 1}, args)
}

func TestUpdateBuilderSafeTableInjection(t *testing.T) {
	id, _ := QuoteIdent("users; DROP TABLE users; --")
	sql, _, err := Update("").SafeTable(id).Set("name", "John").Where("id = ?", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "users; DROP TABLE users; --" SET name = ? WHERE id = ?`, sql)
}

// ---------------------------------------------------------------------------
// UpdateBuilder.SafeSet
// ---------------------------------------------------------------------------

func TestUpdateBuilderSafeSet(t *testing.T) {
	col, _ := QuoteIdent("name")
	sql, args, err := Update("users").SafeSet(col, "John").Where("id = ?", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE users SET "name" = ? WHERE id = ?`, sql)
	assert.Equal(t, []any{"John", 1}, args)
}

func TestUpdateBuilderSafeSetInjection(t *testing.T) {
	col, _ := QuoteIdent("name = 'hacked' WHERE 1=1; --")
	sql, args, err := Update("users").SafeSet(col, "John").Where("id = ?", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE users SET "name = 'hacked' WHERE 1=1; --" = ? WHERE id = ?`, sql)
	assert.Equal(t, []any{"John", 1}, args)
}

// ---------------------------------------------------------------------------
// DeleteBuilder.SafeFrom
// ---------------------------------------------------------------------------

func TestDeleteBuilderSafeFrom(t *testing.T) {
	id, _ := QuoteIdent("users")
	sql, args, err := Delete("").SafeFrom(id).Where("id = ?", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `DELETE FROM "users" WHERE id = ?`, sql)
	assert.Equal(t, []any{1}, args)
}

func TestDeleteBuilderSafeFromInjection(t *testing.T) {
	id, _ := QuoteIdent("users; DROP TABLE users; --")
	sql, _, err := Delete("").SafeFrom(id).Where("id = ?", 1).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `DELETE FROM "users; DROP TABLE users; --" WHERE id = ?`, sql)
}

// ---------------------------------------------------------------------------
// Combined Safe methods
// ---------------------------------------------------------------------------

func TestSafeMethodsCombined(t *testing.T) {
	table, _ := QuoteIdent("items")
	cols, _ := QuoteIdents("name", "category")
	orderCol, _ := QuoteIdent("name")
	groupCol, _ := QuoteIdent("category")

	sql, _, err := Select().
		SafeColumns(cols...).
		SafeFrom(table).
		SafeGroupBy(groupCol).
		SafeOrderByDir(orderCol, Desc).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT "name", "category" FROM "items" GROUP BY "category" ORDER BY "name" DESC`, sql)
}

func TestSafeMethodsWithPlaceholderFormat(t *testing.T) {
	table, _ := QuoteIdent("users")
	col, _ := QuoteIdent("name")
	sql, args, err := Select("*").SafeFrom(table).
		Where("id > ?", 100).
		SafeOrderByDir(col, Asc).
		Limit(10).
		PlaceholderFormat(Dollar).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "users" WHERE id > $1 ORDER BY "name" ASC LIMIT $2`, sql)
	assert.Equal(t, []any{100, uint64(10)}, args)
}

// ---------------------------------------------------------------------------
// ValidateIdent rejects common injection patterns
// ---------------------------------------------------------------------------

func TestValidateIdentRejectsInjectionPatterns(t *testing.T) {
	dangerous := []string{
		"users; DROP TABLE users; --",
		"users' OR '1'='1",
		`users" OR "1"="1`,
		"users/**/OR/**/1=1",
		"Robert'); DROP TABLE students;--",
		"1; SELECT * FROM secrets",
		"table\nname",
		"table\tname",
		"name--comment",
	}

	for _, input := range dangerous {
		_, err := ValidateIdent(input)
		assert.ErrorIs(t, err, ErrInvalidIdentifier, "input: %q", input)
	}
}

// ---------------------------------------------------------------------------
// QuoteIdent safely wraps even dangerous strings
// ---------------------------------------------------------------------------

func TestQuoteIdentHandlesDangerousStrings(t *testing.T) {
	dangerous := []struct {
		input    string
		expected string
	}{
		{
			input:    "users; DROP TABLE users; --",
			expected: `"users; DROP TABLE users; --"`,
		},
		{
			input:    `Robert"`,
			expected: `"Robert"""`,
		},
		{
			input:    `a""b`,
			expected: `"a""""b"`,
		},
		{
			input:    "hello world",
			expected: `"hello world"`,
		},
	}

	for _, tc := range dangerous {
		id, err := QuoteIdent(tc.input)
		assert.NoError(t, err, "input: %q", tc.input)
		assert.Equal(t, tc.expected, id.String(), "input: %q", tc.input)
	}
}

// ---------------------------------------------------------------------------
// identsToStrings
// ---------------------------------------------------------------------------

func TestIdentsToStrings(t *testing.T) {
	ids, _ := QuoteIdents("a", "b", "c")
	strs := identsToStrings(ids)
	assert.Equal(t, []string{`"a"`, `"b"`, `"c"`}, strs)
}

// ---------------------------------------------------------------------------
// OrderDir constants
// ---------------------------------------------------------------------------

func TestOrderDirConstants(t *testing.T) {
	assert.Equal(t, Asc, OrderDir("ASC"))
	assert.Equal(t, Desc, OrderDir("DESC"))
}

// ---------------------------------------------------------------------------
// Edge case: mixing safe and unsafe in one query
// ---------------------------------------------------------------------------

func TestMixingSafeAndUnsafeMethods(t *testing.T) {
	// Safe table, but unsafe OrderBy — still works (backward compat).
	table, _ := QuoteIdent("users")
	sql, _, err := Select("*").SafeFrom(table).OrderBy("name ASC").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "users" ORDER BY name ASC`, sql)
}
