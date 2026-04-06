package integration

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// Basic INSERT
// ---------------------------------------------------------------------------

func TestInsertSingleRow(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_single", "(id INTEGER, name TEXT)")

	// Act
	_, err := sb.Insert("sq_ins_single").
		Columns("id", "name").
		Values(1, "test").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ins_single WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "test", name)
}

func TestInsertMultipleRows(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_multi", "(id INTEGER, name TEXT)")

	// Act
	_, err := sb.Insert("sq_ins_multi").
		Columns("id", "name").
		Values(1, "alpha").
		Values(2, "beta").
		Values(3, "gamma").
		Exec()

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_ins_multi").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestInsertWithNullValue(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_null", "(id INTEGER, name TEXT)")

	// Act
	_, err := sb.Insert("sq_ins_null").
		Columns("id", "name").
		Values(1, nil).
		Exec()

	// Assert
	require.NoError(t, err)

	var name sql.NullString
	err = db.QueryRow("SELECT name FROM sq_ins_null WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.False(t, name.Valid) // NULL
}

// ---------------------------------------------------------------------------
// Insert without explicit Columns
// ---------------------------------------------------------------------------

func TestInsertWithoutExplicitColumns(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_nocol", "(id INTEGER, name TEXT)")

	// Act — values without Columns() specifying column names
	_, err := sb.Insert("sq_ins_nocol").
		Values(1, "no_columns").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ins_nocol WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "no_columns", name)
}

// ---------------------------------------------------------------------------
// RowsAffected
// ---------------------------------------------------------------------------

func TestInsertExecRowsAffected(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_ra", "(id INTEGER, name TEXT)")

	// Act
	res, err := sb.Insert("sq_ins_ra").
		Columns("id", "name").
		Values(1, "a").
		Values(2, "b").
		Values(3, "c").
		Exec()

	// Assert
	require.NoError(t, err)

	affected, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(3), affected)
}

// ---------------------------------------------------------------------------
// Insert with Sqlizer value in Values
// ---------------------------------------------------------------------------

func TestInsertWithSqlizerValue(t *testing.T) {
	// Arrange — use an Expr() Sqlizer as a value (numeric expression
	// is portable across SQLite, MySQL, and PostgreSQL).
	// PostgreSQL requires explicit casts to resolve "operator is not unique:
	// unknown + unknown", while MySQL uses SIGNED instead of INTEGER.
	createTable(t, "sq_ins_sqlz", "(id INTEGER, computed INTEGER)")

	var valExpr sqrl.Sqlizer
	if isPostgres() {
		valExpr = sqrl.Expr("CAST(? AS INTEGER) + CAST(? AS INTEGER)", 10, 32)
	} else {
		valExpr = sqrl.Expr("? + ?", 10, 32)
	}

	// Act
	_, err := sb.Insert("sq_ins_sqlz").
		Columns("id", "computed").
		Values(1, valExpr).
		Exec()

	// Assert
	require.NoError(t, err)

	var computed int
	err = db.QueryRow("SELECT computed FROM sq_ins_sqlz WHERE id = 1").Scan(&computed)
	require.NoError(t, err)
	assert.Equal(t, 42, computed)
}

// ---------------------------------------------------------------------------
// SetMap
// ---------------------------------------------------------------------------

func TestInsertSetMap(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_map", "(id INTEGER, name TEXT, price INTEGER)")

	// Act
	_, err := sb.Insert("sq_ins_map").
		SetMap(map[string]interface{}{
			"id":    1,
			"name":  "widget",
			"price": 42,
		}).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	var price int
	err = db.QueryRow("SELECT name, price FROM sq_ins_map WHERE id = 1").Scan(&name, &price)
	require.NoError(t, err)
	assert.Equal(t, "widget", name)
	assert.Equal(t, 42, price)
}

// ---------------------------------------------------------------------------
// INSERT ... SELECT
// ---------------------------------------------------------------------------

func TestInsertSelect(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_sel", "(id INTEGER, name TEXT)")

	sel := sqrl.Select("id", "name").
		From("sq_items").
		Where(sqrl.Eq{"category": "fruit"})

	// Act
	_, err := sb.Insert("sq_ins_sel").
		Columns("id", "name").
		Select(sel).
		Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_ins_sel").OrderBy("id"))
	assert.Equal(t, []string{"apple", "banana"}, names)
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

func TestInsertOptions(t *testing.T) {
	if isPostgres() {
		t.Skip("INSERT OR IGNORE not supported on PostgreSQL")
	}

	// Arrange — create table with unique constraint
	createTable(t, "sq_ins_opts", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_ins_opts VALUES (1, 'original')")

	// Act — insert duplicate with OR IGNORE (SQLite/MySQL)
	option := "OR IGNORE"
	if isMySQL() {
		option = "IGNORE"
	}

	_, err := sb.Insert("sq_ins_opts").
		Options(option).
		Columns("id", "name").
		Values(1, "duplicate").
		Exec()

	// Assert — no error and original row unchanged
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ins_opts WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "original", name)
}

// ---------------------------------------------------------------------------
// REPLACE
// ---------------------------------------------------------------------------

func TestInsertReplace(t *testing.T) {
	if isPostgres() {
		t.Skip("REPLACE not supported on PostgreSQL")
	}

	// Arrange
	createTable(t, "sq_replace", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_replace VALUES (1, 'original')")

	// Act
	_, err := sqrl.Replace("sq_replace").
		Columns("id", "name").
		Values(1, "replaced").
		RunWith(db).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_replace WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "replaced", name)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_replace").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// ---------------------------------------------------------------------------
// Prefix and Suffix
// ---------------------------------------------------------------------------

func TestInsertPrefix(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_pfx", "(id INTEGER, name TEXT)")

	// Act
	_, err := sb.Insert("sq_ins_pfx").
		Prefix("/* insert prefix */").
		Columns("id", "name").
		Values(1, "prefixed").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ins_pfx").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "prefixed", name)
}

// ---------------------------------------------------------------------------
// PrefixExpr / SuffixExpr
// ---------------------------------------------------------------------------

func TestInsertPrefixExpr(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_pfe", "(id INTEGER, name TEXT)")

	// Act
	_, err := sb.Insert("sq_ins_pfe").
		PrefixExpr(sqrl.Expr("/* insert prefix expr */")).
		Columns("id", "name").
		Values(1, "prefixed_expr").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ins_pfe").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "prefixed_expr", name)
}

func TestInsertSuffixExpr(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_sfe", "(id INTEGER, name TEXT)")

	// Act
	_, err := sb.Insert("sq_ins_sfe").
		Columns("id", "name").
		Values(1, "suffixed_expr").
		SuffixExpr(sqrl.Expr("/* insert suffix expr */")).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ins_sfe").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "suffixed_expr", name)
}

func TestInsertSuffix(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_sfx", "(id INTEGER, name TEXT)")

	// Act
	_, err := sb.Insert("sq_ins_sfx").
		Columns("id", "name").
		Values(1, "suffixed").
		Suffix("/* insert suffix */").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ins_sfx").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "suffixed", name)
}

// ---------------------------------------------------------------------------
// Query and QueryRow (e.g. with RETURNING)
// ---------------------------------------------------------------------------

func TestInsertQueryWithReturning(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_ins_ret", "(id INTEGER, name TEXT)")

	// Act
	rows, err := sb.Insert("sq_ins_ret").
		Columns("id", "name").
		Values(1, "returned").
		Suffix("RETURNING name").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))
	assert.Equal(t, "returned", name)
}

func TestInsertScanWithReturning(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_ins_scan", "(id INTEGER, name TEXT)")

	// Act
	var name string
	err := sb.Insert("sq_ins_scan").
		Columns("id", "name").
		Values(1, "scanned").
		Suffix("RETURNING name").
		Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "scanned", name)
}

// ---------------------------------------------------------------------------
// Builder immutability
// ---------------------------------------------------------------------------

func TestInsertBuilderImmutability(t *testing.T) {
	// Arrange
	createTable(t, "sq_ins_imm", "(id INTEGER, name TEXT)")

	base := sb.Insert("sq_ins_imm").Columns("id", "name")
	q1 := base.Values(1, "first")
	q2 := base.Values(2, "second")

	// Act
	_, err := q1.Exec()
	require.NoError(t, err)
	_, err = q2.Exec()
	require.NoError(t, err)

	// Assert — both rows should exist
	names := queryStrings(t, sb.Select("name").From("sq_ins_imm").OrderBy("id"))
	assert.Equal(t, []string{"first", "second"}, names)
}

// ---------------------------------------------------------------------------
// PlaceholderFormat
// ---------------------------------------------------------------------------

func TestInsertPlaceholderFormat(t *testing.T) {
	// Arrange
	q := sqrl.Insert("items").Columns("a", "b").Values(1, 2).PlaceholderFormat(sqrl.Dollar)

	// Act
	sqlStr, args, err := q.ToSQL()

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO items (a,b) VALUES ($1,$2)", sqlStr)
	assert.Equal(t, []interface{}{1, 2}, args)
}

// ---------------------------------------------------------------------------
// ToSQL and MustSQL
// ---------------------------------------------------------------------------

func TestInsertToSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("items").Columns("id", "name").Values(1, "test")

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO items (id,name) VALUES (?,?)", sqlStr)
		assert.Equal(t, []interface{}{1, "test"}, args)
	})

	t.Run("SetMapSortsColumns", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("items").SetMap(map[string]interface{}{
			"b": 2,
			"a": 1,
		})

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO items (a,b) VALUES (?,?)", sqlStr)
		assert.Equal(t, []interface{}{1, 2}, args)
	})
}

func TestInsertMustSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("items").Columns("id").Values(1)

		// Act
		sqlStr, args := q.MustSQL()

		// Assert
		assert.Equal(t, "INSERT INTO items (id) VALUES (?)", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("PanicsOnError", func(t *testing.T) {
		// Arrange — missing Into
		q := sqrl.Insert("").Columns("id").Values(1)

		// Act & Assert
		assert.Panics(t, func() { q.MustSQL() })
	})
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

func TestInsertErrors(t *testing.T) {
	t.Run("NoTable", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("").Columns("id").Values(1)

		// Act
		_, _, err := q.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must specify a table")
	})

	t.Run("NoValues", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("items").Columns("id")

		// Act
		_, _, err := q.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one set of values")
	})

	t.Run("NoRunnerExec", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("items").Columns("id").Values(1)

		// Act
		_, err := q.Exec()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerQuery", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("items").Columns("id").Values(1)

		// Act
		_, err := q.Query()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerScan", func(t *testing.T) {
		// Arrange
		q := sqrl.Insert("items").Columns("id").Values(1)

		// Act
		var v int
		err := q.Scan(&v)

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}
