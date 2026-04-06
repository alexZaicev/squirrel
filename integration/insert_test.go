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

// ---------------------------------------------------------------------------
// ON CONFLICT — PostgreSQL / SQLite upsert
// ---------------------------------------------------------------------------

func TestInsertOnConflictDoNothingSingleRow(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_nothing", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_nothing VALUES (1, 'original')")

	// Act — insert duplicate, expect DO NOTHING
	_, err := sb.Insert("sq_oc_nothing").
		Columns("id", "name").
		Values(1, "duplicate").
		OnConflictColumns("id").
		OnConflictDoNothing().
		Exec()

	// Assert — no error, original row unchanged
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_nothing WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "original", name)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_oc_nothing").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestInsertOnConflictDoNothingNoConflict(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_noconf", "(id INTEGER PRIMARY KEY, name TEXT)")

	// Act — insert non-duplicate
	_, err := sb.Insert("sq_oc_noconf").
		Columns("id", "name").
		Values(1, "new_row").
		OnConflictColumns("id").
		OnConflictDoNothing().
		Exec()

	// Assert — row inserted normally
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_noconf WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new_row", name)
}

func TestInsertOnConflictDoNothingNoTarget(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_notgt", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_notgt VALUES (1, 'original')")

	// Act — ON CONFLICT without target columns
	_, err := sb.Insert("sq_oc_notgt").
		Columns("id", "name").
		Values(1, "duplicate").
		OnConflictDoNothing().
		Exec()

	// Assert — no error, original row unchanged
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_notgt WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "original", name)
}

func TestInsertOnConflictDoUpdateLiteralValues(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_upd", "(id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	seedTable(t, "INSERT INTO sq_oc_upd VALUES (1, 'old_name', 'old@example.com')")

	// Act — upsert with literal values
	_, err := sb.Insert("sq_oc_upd").
		Columns("id", "name", "email").
		Values(1, "new_name", "new@example.com").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", "new_name").
		OnConflictDoUpdate("email", "new@example.com").
		Exec()

	// Assert — row updated
	require.NoError(t, err)

	var name, email string
	err = db.QueryRow("SELECT name, email FROM sq_oc_upd WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	assert.Equal(t, "new_name", name)
	assert.Equal(t, "new@example.com", email)
}

func TestInsertOnConflictDoUpdateExcluded(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_excl", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_excl VALUES (1, 'old')")

	// Act — upsert using EXCLUDED pseudo-table
	_, err := sb.Insert("sq_oc_excl").
		Columns("id", "name").
		Values(1, "new_via_excluded").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_excl WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new_via_excluded", name)
}

func TestInsertOnConflictDoUpdateMap(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_map", "(id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	seedTable(t, "INSERT INTO sq_oc_map VALUES (1, 'old', 'old@test.com')")

	// Act — upsert using map
	_, err := sb.Insert("sq_oc_map").
		Columns("id", "name", "email").
		Values(1, "mapped", "mapped@test.com").
		OnConflictColumns("id").
		OnConflictDoUpdateMap(map[string]interface{}{
			"name":  sqrl.Expr("EXCLUDED.name"),
			"email": sqrl.Expr("EXCLUDED.email"),
		}).
		Exec()

	// Assert
	require.NoError(t, err)

	var name, email string
	err = db.QueryRow("SELECT name, email FROM sq_oc_map WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	assert.Equal(t, "mapped", name)
	assert.Equal(t, "mapped@test.com", email)
}

func TestInsertOnConflictDoUpdateWithWhere(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange — two rows, one active, one inactive
	createTable(t, "sq_oc_where", "(id INTEGER PRIMARY KEY, name TEXT, active INTEGER)")
	seedTable(t, "INSERT INTO sq_oc_where VALUES (1, 'active_row', 1)")
	seedTable(t, "INSERT INTO sq_oc_where VALUES (2, 'inactive_row', 0)")

	// Act — upsert id=1 (active), WHERE restricts update to active=1
	_, err := sb.Insert("sq_oc_where").
		Columns("id", "name", "active").
		Values(1, "updated_active", 1).
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		OnConflictWhere(sqrl.Eq{"sq_oc_where.active": 1}).
		Exec()
	require.NoError(t, err)

	// Act — upsert id=2 (inactive), WHERE restricts update to active=1 → no update
	_, err = sb.Insert("sq_oc_where").
		Columns("id", "name", "active").
		Values(2, "should_not_change", 0).
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		OnConflictWhere(sqrl.Eq{"sq_oc_where.active": 1}).
		Exec()
	require.NoError(t, err)

	// Assert — active row was updated
	var name1 string
	err = db.QueryRow("SELECT name FROM sq_oc_where WHERE id = 1").Scan(&name1)
	require.NoError(t, err)
	assert.Equal(t, "updated_active", name1)

	// Assert — inactive row was NOT updated
	var name2 string
	err = db.QueryRow("SELECT name FROM sq_oc_where WHERE id = 2").Scan(&name2)
	require.NoError(t, err)
	assert.Equal(t, "inactive_row", name2)
}

func TestInsertOnConflictMultipleTargetColumns(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange — composite unique constraint
	if isPostgres() {
		createTable(t, "sq_oc_multi", "(org_id INTEGER, user_id INTEGER, name TEXT, UNIQUE(org_id, user_id))")
	} else {
		createTable(t, "sq_oc_multi", "(org_id INTEGER, user_id INTEGER, name TEXT, UNIQUE(org_id, user_id))")
	}
	seedTable(t, "INSERT INTO sq_oc_multi VALUES (1, 100, 'original')")

	// Act — conflict on composite key
	_, err := sb.Insert("sq_oc_multi").
		Columns("org_id", "user_id", "name").
		Values(1, 100, "updated_composite").
		OnConflictColumns("org_id", "user_id").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_multi WHERE org_id = 1 AND user_id = 100").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "updated_composite", name)
}

func TestInsertOnConflictMultiRowInsert(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_batch", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_batch VALUES (1, 'existing1')")
	seedTable(t, "INSERT INTO sq_oc_batch VALUES (3, 'existing3')")

	// Act — multi-row insert, some conflict, some new
	_, err := sb.Insert("sq_oc_batch").
		Columns("id", "name").
		Values(1, "updated1").
		Values(2, "new2").
		Values(3, "updated3").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_oc_batch").OrderBy("id"))
	assert.Equal(t, []string{"updated1", "new2", "updated3"}, names)
}

func TestInsertOnConflictDoNothingMultiRow(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_noth_mr", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_noth_mr VALUES (1, 'existing')")

	// Act — multi-row insert with DO NOTHING
	_, err := sb.Insert("sq_oc_noth_mr").
		Columns("id", "name").
		Values(1, "duplicate").
		Values(2, "new_row").
		OnConflictColumns("id").
		OnConflictDoNothing().
		Exec()

	// Assert — existing row unchanged, new row inserted
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_oc_noth_mr").OrderBy("id"))
	assert.Equal(t, []string{"existing", "new_row"}, names)
}

func TestInsertOnConflictWithReturning(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT / RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_oc_ret", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_ret VALUES (1, 'old')")

	// Act — upsert with RETURNING
	var name string
	err := sb.Insert("sq_oc_ret").
		Columns("id", "name").
		Values(1, "returned_name").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		Suffix("RETURNING name").
		Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "returned_name", name)
}

func TestInsertOnConflictDoUpdateInsertNewRow(t *testing.T) {
	if isMySQL() {
		t.Skip("ON CONFLICT not supported on MySQL")
	}

	// Arrange — empty table, no conflict should occur
	createTable(t, "sq_oc_new", "(id INTEGER PRIMARY KEY, name TEXT)")

	// Act
	_, err := sb.Insert("sq_oc_new").
		Columns("id", "name").
		Values(1, "inserted_not_conflicted").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		Exec()

	// Assert — row inserted normally
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_new WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "inserted_not_conflicted", name)
}

// ---------------------------------------------------------------------------
// ON CONFLICT ON CONSTRAINT — PostgreSQL only
// ---------------------------------------------------------------------------

func TestInsertOnConflictOnConstraintDoNothing(t *testing.T) {
	if !isPostgres() {
		t.Skip("ON CONFLICT ON CONSTRAINT only supported on PostgreSQL")
	}

	// Arrange — PostgreSQL named constraint
	createTable(t, "sq_oc_constr", "(id INTEGER CONSTRAINT sq_oc_constr_pkey PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_constr VALUES (1, 'original')")

	// Act
	_, err := sb.Insert("sq_oc_constr").
		Columns("id", "name").
		Values(1, "duplicate").
		OnConflictOnConstraint("sq_oc_constr_pkey").
		OnConflictDoNothing().
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_constr WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "original", name)
}

func TestInsertOnConflictOnConstraintDoUpdate(t *testing.T) {
	if !isPostgres() {
		t.Skip("ON CONFLICT ON CONSTRAINT only supported on PostgreSQL")
	}

	// Arrange
	createTable(t, "sq_oc_constr_u", "(id INTEGER CONSTRAINT sq_oc_constr_u_pkey PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_oc_constr_u VALUES (1, 'old')")

	// Act
	_, err := sb.Insert("sq_oc_constr_u").
		Columns("id", "name").
		Values(1, "new_via_constraint").
		OnConflictOnConstraint("sq_oc_constr_u_pkey").
		OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_oc_constr_u WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new_via_constraint", name)
}

// ---------------------------------------------------------------------------
// ON DUPLICATE KEY UPDATE — MySQL only
// ---------------------------------------------------------------------------

func TestInsertOnDuplicateKeyUpdateLiteral(t *testing.T) {
	if !isMySQL() {
		t.Skip("ON DUPLICATE KEY UPDATE is MySQL-specific")
	}

	// Arrange
	createTable(t, "sq_odku_lit", "(id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	seedTable(t, "INSERT INTO sq_odku_lit VALUES (1, 'old', 'old@test.com')")

	// Act
	_, err := sb.Insert("sq_odku_lit").
		Columns("id", "name", "email").
		Values(1, "new_lit", "new_lit@test.com").
		OnDuplicateKeyUpdate("name", "new_lit").
		OnDuplicateKeyUpdate("email", "new_lit@test.com").
		Exec()

	// Assert
	require.NoError(t, err)

	var name, email string
	err = db.QueryRow("SELECT name, email FROM sq_odku_lit WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	assert.Equal(t, "new_lit", name)
	assert.Equal(t, "new_lit@test.com", email)
}

func TestInsertOnDuplicateKeyUpdateValues(t *testing.T) {
	if !isMySQL() {
		t.Skip("ON DUPLICATE KEY UPDATE is MySQL-specific")
	}

	// Arrange
	createTable(t, "sq_odku_val", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_odku_val VALUES (1, 'old')")

	// Act — use VALUES(name) to refer to the value being inserted
	_, err := sb.Insert("sq_odku_val").
		Columns("id", "name").
		Values(1, "via_values").
		OnDuplicateKeyUpdate("name", sqrl.Expr("VALUES(name)")).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_odku_val WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "via_values", name)
}

func TestInsertOnDuplicateKeyUpdateMap(t *testing.T) {
	if !isMySQL() {
		t.Skip("ON DUPLICATE KEY UPDATE is MySQL-specific")
	}

	// Arrange
	createTable(t, "sq_odku_map", "(id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	seedTable(t, "INSERT INTO sq_odku_map VALUES (1, 'old', 'old@test.com')")

	// Act — use map
	_, err := sb.Insert("sq_odku_map").
		Columns("id", "name", "email").
		Values(1, "mapped", "mapped@test.com").
		OnDuplicateKeyUpdateMap(map[string]interface{}{
			"name":  sqrl.Expr("VALUES(name)"),
			"email": sqrl.Expr("VALUES(email)"),
		}).
		Exec()

	// Assert
	require.NoError(t, err)

	var name, email string
	err = db.QueryRow("SELECT name, email FROM sq_odku_map WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	assert.Equal(t, "mapped", name)
	assert.Equal(t, "mapped@test.com", email)
}

func TestInsertOnDuplicateKeyUpdateMultiRow(t *testing.T) {
	if !isMySQL() {
		t.Skip("ON DUPLICATE KEY UPDATE is MySQL-specific")
	}

	// Arrange
	createTable(t, "sq_odku_mr", "(id INTEGER PRIMARY KEY, name TEXT)")
	seedTable(t, "INSERT INTO sq_odku_mr VALUES (1, 'existing1')")
	seedTable(t, "INSERT INTO sq_odku_mr VALUES (3, 'existing3')")

	// Act — multi-row insert, some conflicts
	_, err := sb.Insert("sq_odku_mr").
		Columns("id", "name").
		Values(1, "upd1").
		Values(2, "new2").
		Values(3, "upd3").
		OnDuplicateKeyUpdate("name", sqrl.Expr("VALUES(name)")).
		Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_odku_mr").OrderBy("id"))
	assert.Equal(t, []string{"upd1", "new2", "upd3"}, names)
}

func TestInsertOnDuplicateKeyUpdateNoConflict(t *testing.T) {
	if !isMySQL() {
		t.Skip("ON DUPLICATE KEY UPDATE is MySQL-specific")
	}

	// Arrange — empty table
	createTable(t, "sq_odku_new", "(id INTEGER PRIMARY KEY, name TEXT)")

	// Act — insert with no conflict
	_, err := sb.Insert("sq_odku_new").
		Columns("id", "name").
		Values(1, "fresh_insert").
		OnDuplicateKeyUpdate("name", sqrl.Expr("VALUES(name)")).
		Exec()

	// Assert — row inserted normally
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_odku_new WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "fresh_insert", name)
}

// ---------------------------------------------------------------------------
// Upsert — ToSQL generation (no database required)
// ---------------------------------------------------------------------------

func TestInsertOnConflictToSQL(t *testing.T) {
	t.Run("DoNothingWithColumns", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "name").Values(1, "a").
			OnConflictColumns("id").OnConflictDoNothing()

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,name) VALUES (?,?) ON CONFLICT (id) DO NOTHING", sqlStr)
		assert.Equal(t, []interface{}{1, "a"}, args)
	})

	t.Run("DoNothingNoTarget", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id").Values(1).
			OnConflictDoNothing()

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id) VALUES (?) ON CONFLICT DO NOTHING", sqlStr)
	})

	t.Run("DoUpdateWithDollar", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "name").Values(1, "a").
			OnConflictColumns("id").
			OnConflictDoUpdate("name", "b").
			PlaceholderFormat(sqrl.Dollar)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name = $3", sqlStr)
		assert.Equal(t, []interface{}{1, "a", "b"}, args)
	})

	t.Run("DoUpdateWithExcluded", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "name").Values(1, "a").
			OnConflictColumns("id").
			OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name"))

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,name) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name", sqlStr)
		assert.Equal(t, []interface{}{1, "a"}, args)
	})

	t.Run("DoUpdateWithWhere", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "v").Values(1, 10).
			OnConflictColumns("id").
			OnConflictDoUpdate("v", sqrl.Expr("EXCLUDED.v")).
			OnConflictWhere(sqrl.Eq{"t.active": true})

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,v) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE t.active = ?", sqlStr)
		assert.Equal(t, []interface{}{1, 10, true}, args)
	})

	t.Run("OnConstraint", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id").Values(1).
			OnConflictOnConstraint("t_pkey").OnConflictDoNothing()

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id) VALUES (?) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING", sqlStr)
	})

	t.Run("MultipleConflictColumns", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("a", "b", "c").Values(1, 2, 3).
			OnConflictColumns("a", "b").OnConflictDoNothing()

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (a,b,c) VALUES (?,?,?) ON CONFLICT (a,b) DO NOTHING", sqlStr)
	})

	t.Run("WithSuffix", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "name").Values(1, "a").
			OnConflictColumns("id").
			OnConflictDoUpdate("name", sqrl.Expr("EXCLUDED.name")).
			Suffix("RETURNING id")

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,name) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id", sqlStr)
	})
}

func TestInsertOnConflictToSQLErrors(t *testing.T) {
	t.Run("DoNothingAndDoUpdateMutuallyExclusive", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id").Values(1).
			OnConflictColumns("id").
			OnConflictDoNothing().
			OnConflictDoUpdate("id", 1)

		_, _, err := q.ToSQL()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mutually exclusive")
	})

	t.Run("ConflictColumnsWithoutAction", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id").Values(1).
			OnConflictColumns("id")

		_, _, err := q.ToSQL()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must use DO NOTHING or DO UPDATE")
	})
}

func TestInsertOnDuplicateKeyUpdateToSQL(t *testing.T) {
	t.Run("LiteralValue", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "name").Values(1, "a").
			OnDuplicateKeyUpdate("name", "b")

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,name) VALUES (?,?) ON DUPLICATE KEY UPDATE name = ?", sqlStr)
		assert.Equal(t, []interface{}{1, "a", "b"}, args)
	})

	t.Run("ExprValue", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "name").Values(1, "a").
			OnDuplicateKeyUpdate("name", sqrl.Expr("VALUES(name)"))

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,name) VALUES (?,?) ON DUPLICATE KEY UPDATE name = VALUES(name)", sqlStr)
		assert.Equal(t, []interface{}{1, "a"}, args)
	})

	t.Run("MapSorted", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "b", "a").Values(1, 2, 3).
			OnDuplicateKeyUpdateMap(map[string]interface{}{
				"b": 20,
				"a": 30,
			})

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,b,a) VALUES (?,?,?) ON DUPLICATE KEY UPDATE a = ?, b = ?", sqlStr)
		assert.Equal(t, []interface{}{1, 2, 3, 30, 20}, args)
	})

	t.Run("MultiRow", func(t *testing.T) {
		q := sqrl.Insert("t").Columns("id", "name").
			Values(1, "a").Values(2, "b").
			OnDuplicateKeyUpdate("name", sqrl.Expr("VALUES(name)"))

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (id,name) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE name = VALUES(name)", sqlStr)
		assert.Equal(t, []interface{}{1, "a", 2, "b"}, args)
	})
}
