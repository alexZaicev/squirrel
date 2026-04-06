package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// QuoteIdent / ValidateIdent (pure functions, no database needed)
// ---------------------------------------------------------------------------

func TestIdentQuoteBasic(t *testing.T) {
	// Arrange & Act
	id, err := sqrl.QuoteIdent("sq_items")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, `"sq_items"`, id.String())
	assert.Equal(t, "sq_items", id.Raw())
}

func TestIdentQuoteSchemaQualified(t *testing.T) {
	// Arrange & Act
	id, err := sqrl.QuoteIdent("public.sq_items")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, `"public"."sq_items"`, id.String())
}

func TestIdentQuoteInjectionAttempt(t *testing.T) {
	// Arrange & Act
	id, err := sqrl.QuoteIdent("sq_items; DROP TABLE sq_items; --")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, `"sq_items; DROP TABLE sq_items; --"`, id.String())
}

func TestIdentQuoteEmpty(t *testing.T) {
	// Arrange & Act
	_, err := sqrl.QuoteIdent("")

	// Assert
	assert.Error(t, err)
}

func TestIdentValidateBasic(t *testing.T) {
	// Arrange & Act
	id, err := sqrl.ValidateIdent("sq_items")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "sq_items", id.String())
}

func TestIdentValidateRejectsInjection(t *testing.T) {
	// Arrange & Act
	_, err := sqrl.ValidateIdent("sq_items; DROP TABLE sq_items; --")

	// Assert
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// safeIdent is a test helper that produces a validated identifier.
// ValidateIdent emits unquoted names and works on all databases (SQLite,
// MySQL, PostgreSQL). QuoteIdent produces ANSI double-quoted identifiers
// which MySQL does not support unless ANSI_QUOTES mode is enabled.
// ---------------------------------------------------------------------------

func safeIdent(t *testing.T, name string) sqrl.Ident {
	t.Helper()
	id, err := sqrl.ValidateIdent(name)
	require.NoError(t, err)
	return id
}

func safeIdents(t *testing.T, names ...string) []sqrl.Ident {
	t.Helper()
	ids, err := sqrl.ValidateIdents(names...)
	require.NoError(t, err)
	return ids
}

// ---------------------------------------------------------------------------
// Safe SELECT — runs against the real database
// ---------------------------------------------------------------------------

func TestIdentSafeSelectFrom(t *testing.T) {
	// Arrange
	table := safeIdent(t, "sq_items")
	q := sb.Select("name").SafeFrom(table).OrderBy("id")

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant", "mystery"}, names)
}

func TestIdentSafeSelectFromValidated(t *testing.T) {
	// Arrange
	table, err := sqrl.ValidateIdent("sq_items")
	require.NoError(t, err)

	q := sb.Select("name").SafeFrom(table).Where(sqrl.Eq{"id": 1})

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"apple"}, names)
}

func TestIdentSafeSelectColumns(t *testing.T) {
	// Arrange
	cols := safeIdents(t, "id", "name")
	table := safeIdent(t, "sq_items")

	q := sb.Select().SafeColumns(cols...).SafeFrom(table).Where(sqrl.Eq{"id": 1})

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var id int
	var name string
	require.NoError(t, rows.Scan(&id, &name))

	// Assert
	assert.Equal(t, 1, id)
	assert.Equal(t, "apple", name)
	assert.False(t, rows.Next())
}

func TestIdentSafeSelectOrderBy(t *testing.T) {
	// Arrange
	col := safeIdent(t, "name")
	table := safeIdent(t, "sq_items")

	q := sb.Select("name").SafeFrom(table).SafeOrderByDir(col, sqrl.Desc).Limit(3)

	// Act
	names := queryStrings(t, q)

	// Assert — descending by name, top 3
	assert.Equal(t, []string{"mystery", "eggplant", "donut"}, names)
}

func TestIdentSafeSelectOrderByAsc(t *testing.T) {
	// Arrange
	col := safeIdent(t, "name")
	table := safeIdent(t, "sq_items")

	q := sb.Select("name").SafeFrom(table).SafeOrderByDir(col, sqrl.Asc).Limit(3)

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"apple", "banana", "carrot"}, names)
}

func TestIdentSafeSelectGroupBy(t *testing.T) {
	// Arrange
	col := safeIdent(t, "category")

	q := sb.Select("category").From("sq_items").
		Where(sqrl.NotEq{"category": nil}).
		SafeGroupBy(col).
		OrderBy("category")

	// Act
	vals := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"fruit", "pastry", "vegetable"}, vals)
}

// ---------------------------------------------------------------------------
// Safe INSERT — creates temp table, inserts, verifies
// ---------------------------------------------------------------------------

func TestIdentSafeInsert(t *testing.T) {
	// Arrange
	createTable(t, "sq_safe_insert_test", "(id INTEGER, name TEXT)")

	table := safeIdent(t, "sq_safe_insert_test")
	cols := safeIdents(t, "id", "name")

	q := sb.Insert("").SafeInto(table).SafeColumns(cols...).Values(1, "test_item")

	// Act
	_, err := q.Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_safe_insert_test").Where(sqrl.Eq{"id": 1}))
	assert.Equal(t, []string{"test_item"}, names)
}

// ---------------------------------------------------------------------------
// Safe UPDATE — modifies data, verifies
// ---------------------------------------------------------------------------

func TestIdentSafeUpdate(t *testing.T) {
	// Arrange
	createTable(t, "sq_safe_update_test", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_safe_update_test (id, name) VALUES (1, 'original')")

	table := safeIdent(t, "sq_safe_update_test")
	col := safeIdent(t, "name")

	q := sb.Update("").SafeTable(table).SafeSet(col, "updated").Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_safe_update_test").Where(sqrl.Eq{"id": 1}))
	assert.Equal(t, []string{"updated"}, names)
}

// ---------------------------------------------------------------------------
// Safe DELETE — removes data, verifies
// ---------------------------------------------------------------------------

func TestIdentSafeDelete(t *testing.T) {
	// Arrange
	createTable(t, "sq_safe_delete_test", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_safe_delete_test (id, name) VALUES (1, 'to_delete'), (2, 'to_keep')")

	table := safeIdent(t, "sq_safe_delete_test")
	q := sb.Delete("").SafeFrom(table).Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_safe_delete_test").OrderBy("id"))
	assert.Equal(t, []string{"to_keep"}, names)
}

// ---------------------------------------------------------------------------
// Combined safe operations
// ---------------------------------------------------------------------------

func TestIdentSafeCombinedQuery(t *testing.T) {
	// Arrange
	table := safeIdent(t, "sq_items")
	cols := safeIdents(t, "name")
	orderCol := safeIdent(t, "name")

	q := sb.Select().
		SafeColumns(cols...).
		SafeFrom(table).
		Where(sqrl.NotEq{"category": nil}).
		SafeOrderByDir(orderCol, sqrl.Asc).
		Limit(2)

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"apple", "banana"}, names)
}

// ---------------------------------------------------------------------------
// QuoteIdent (ANSI double-quoting) — only runs on databases that support it.
// MySQL requires ANSI_QUOTES mode for double-quoted identifiers; skip there.
// ---------------------------------------------------------------------------

func TestIdentQuoteIdentExecution(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support ANSI double-quoted identifiers by default")
	}

	// Arrange
	table, err := sqrl.QuoteIdent("sq_items")
	require.NoError(t, err)

	col, err := sqrl.QuoteIdent("name")
	require.NoError(t, err)

	q := sb.Select("name").SafeFrom(table).SafeOrderByDir(col, sqrl.Asc).Limit(3)

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"apple", "banana", "carrot"}, names)
}

func TestIdentQuoteIdentInsertExecution(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support ANSI double-quoted identifiers by default")
	}

	// Arrange
	createTable(t, "sq_qi_insert_test", "(id INTEGER, name TEXT)")

	table, err := sqrl.QuoteIdent("sq_qi_insert_test")
	require.NoError(t, err)
	cols, err := sqrl.QuoteIdents("id", "name")
	require.NoError(t, err)

	q := sb.Insert("").SafeInto(table).SafeColumns(cols...).Values(1, "quoted")

	// Act
	_, err = q.Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_qi_insert_test").Where(sqrl.Eq{"id": 1}))
	assert.Equal(t, []string{"quoted"}, names)
}

// ---------------------------------------------------------------------------
// ValidateIdent rejects injection — pure function tests
// ---------------------------------------------------------------------------

func TestIdentValidateRejectsCommonInjections(t *testing.T) {
	// Arrange
	attacks := []string{
		"table; DROP TABLE table; --",
		"table' OR '1'='1",
		"table/**/OR/**/1=1",
		"Robert'); DROP TABLE students;--",
		"1; SELECT * FROM secrets",
	}

	for _, input := range attacks {
		// Act
		_, err := sqrl.ValidateIdent(input)

		// Assert
		assert.Error(t, err, "expected error for input: %q", input)
	}
}
