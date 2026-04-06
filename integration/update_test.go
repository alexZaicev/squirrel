package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// Basic UPDATE
// ---------------------------------------------------------------------------

func TestUpdateSet(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_set", "(id INTEGER, name TEXT, price INTEGER)")
	seedTable(t, "INSERT INTO sq_upd_set VALUES (1, 'item', 10)")

	// Act
	_, err := sb.Update("sq_upd_set").
		Set("name", "updated").
		Set("price", 99).
		Where(sqrl.Eq{"id": 1}).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	var price int
	err = db.QueryRow("SELECT name, price FROM sq_upd_set WHERE id = 1").Scan(&name, &price)
	require.NoError(t, err)
	assert.Equal(t, "updated", name)
	assert.Equal(t, 99, price)
}

func TestUpdateSetToNull(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_null", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_null VALUES (1, 'notnull')")

	// Act
	_, err := sb.Update("sq_upd_null").
		Set("name", nil).
		Where(sqrl.Eq{"id": 1}).
		Exec()

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_upd_null WHERE name IS NULL").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// ---------------------------------------------------------------------------
// SetMap
// ---------------------------------------------------------------------------

func TestUpdateSetMap(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_map", "(id INTEGER, name TEXT, price INTEGER)")
	seedTable(t, "INSERT INTO sq_upd_map VALUES (1, 'old', 10), (2, 'keep', 20)")

	// Act
	_, err := sb.Update("sq_upd_map").
		SetMap(map[string]interface{}{
			"name":  "new",
			"price": 42,
		}).
		Where(sqrl.Eq{"id": 1}).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	var price int
	err = db.QueryRow("SELECT name, price FROM sq_upd_map WHERE id = 1").Scan(&name, &price)
	require.NoError(t, err)
	assert.Equal(t, "new", name)
	assert.Equal(t, 42, price)

	// Verify other row is untouched
	err = db.QueryRow("SELECT name FROM sq_upd_map WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "keep", name)
}

// ---------------------------------------------------------------------------
// WHERE
// ---------------------------------------------------------------------------

func TestUpdateWhere(t *testing.T) {
	t.Run("SingleCondition", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_upd_wh1", "(id INTEGER, name TEXT)")
		seedTable(t, "INSERT INTO sq_upd_wh1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")

		// Act
		_, err := sb.Update("sq_upd_wh1").
			Set("name", "x").
			Where(sqrl.Eq{"id": 2}).
			Exec()

		// Assert
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_upd_wh1").OrderBy("id"))
		assert.Equal(t, []string{"a", "x", "c"}, names)
	})

	t.Run("MultipleConditions", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_upd_wh2", "(id INTEGER, name TEXT, category TEXT)")
		seedTable(t, "INSERT INTO sq_upd_wh2 VALUES (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'y')")

		// Act — update where category='x' AND id=1
		_, err := sb.Update("sq_upd_wh2").
			Set("name", "updated").
			Where(sqrl.Eq{"category": "x"}).
			Where(sqrl.Eq{"id": 1}).
			Exec()

		// Assert — only row 1 updated
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_upd_wh2").OrderBy("id"))
		assert.Equal(t, []string{"updated", "b", "c"}, names)
	})

	t.Run("NoWhere_UpdatesAll", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_upd_all", "(id INTEGER, name TEXT)")
		seedTable(t, "INSERT INTO sq_upd_all VALUES (1, 'a'), (2, 'b')")

		// Act
		_, err := sb.Update("sq_upd_all").
			Set("name", "same").
			Exec()

		// Assert — both rows updated
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_upd_all").OrderBy("id"))
		assert.Equal(t, []string{"same", "same"}, names)
	})
}

// ---------------------------------------------------------------------------
// ORDER BY, LIMIT, OFFSET
// ---------------------------------------------------------------------------

func TestUpdateOrderByLimit(t *testing.T) {
	if isPostgres() {
		t.Skip("UPDATE with ORDER BY/LIMIT not supported on PostgreSQL")
	}
	// SQLite only supports ORDER BY/LIMIT on UPDATE if compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT.
	// The default go-sqlite3 build does not enable this flag.
	if driverName == "sqlite3" {
		t.Skip("UPDATE with ORDER BY/LIMIT not supported on default SQLite builds")
	}

	// Arrange
	createTable(t, "sq_upd_lim", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_lim VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Act — update first row in ascending order only
	_, err := sb.Update("sq_upd_lim").
		Set("name", "first").
		OrderBy("id ASC").
		Limit(1).
		Exec()

	// Assert — only id=1 updated
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_upd_lim").OrderBy("id"))
	assert.Equal(t, []string{"first", "b", "c"}, names)
}

// ---------------------------------------------------------------------------
// Prefix and Suffix
// ---------------------------------------------------------------------------

func TestUpdatePrefix(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_pfx", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_pfx VALUES (1, 'old')")

	// Act
	_, err := sb.Update("sq_upd_pfx").
		Prefix("/* update prefix */").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_upd_pfx WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new", name)
}

func TestUpdateSuffix(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_sfx", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_sfx VALUES (1, 'old')")

	// Act
	_, err := sb.Update("sq_upd_sfx").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		Suffix("/* update suffix */").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_upd_sfx WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new", name)
}

// ---------------------------------------------------------------------------
// PrefixExpr / SuffixExpr
// ---------------------------------------------------------------------------

func TestUpdatePrefixExpr(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_pfe", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_pfe VALUES (1, 'old')")

	// Act
	_, err := sb.Update("sq_upd_pfe").
		PrefixExpr(sqrl.Expr("/* update prefix expr */")).
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_upd_pfe WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new", name)
}

func TestUpdateSuffixExpr(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_sfe", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_sfe VALUES (1, 'old')")

	// Act
	_, err := sb.Update("sq_upd_sfe").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		SuffixExpr(sqrl.Expr("/* update suffix expr */")).
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_upd_sfe WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new", name)
}

// ---------------------------------------------------------------------------
// Builder immutability
// ---------------------------------------------------------------------------

func TestUpdateBuilderImmutability(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_imm", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_imm VALUES (1, 'a'), (2, 'b')")

	base := sb.Update("sq_upd_imm").Set("name", "changed")
	q1 := base.Where(sqrl.Eq{"id": 1})
	q2 := base.Where(sqrl.Eq{"id": 2})

	// Act
	_, err := q1.Exec()
	require.NoError(t, err)

	// Assert — only id=1 changed, id=2 still 'b'
	names := queryStrings(t, sb.Select("name").From("sq_upd_imm").OrderBy("id"))
	assert.Equal(t, []string{"changed", "b"}, names)

	_, err = q2.Exec()
	require.NoError(t, err)
	names = queryStrings(t, sb.Select("name").From("sq_upd_imm").OrderBy("id"))
	assert.Equal(t, []string{"changed", "changed"}, names)
}

// ---------------------------------------------------------------------------
// RowsAffected
// ---------------------------------------------------------------------------

func TestUpdateExecRowsAffected(t *testing.T) {
	// Arrange
	createTable(t, "sq_upd_ra", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_ra VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Act
	res, err := sb.Update("sq_upd_ra").
		Set("name", "updated").
		Where(sqrl.Lt{"id": 3}).
		Exec()

	// Assert
	require.NoError(t, err)

	affected, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(2), affected)
}

// ---------------------------------------------------------------------------
// Update From (PostgreSQL / SQLite 3.33+)
// ---------------------------------------------------------------------------

func TestUpdateFrom(t *testing.T) {
	if isMySQL() {
		t.Skip("UPDATE ... FROM not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_from", "(id INTEGER, name TEXT, cat TEXT)")
	seedTable(t, "INSERT INTO sq_upd_from VALUES (1, 'old', 'fruit')")

	// Act — update using FROM to join with sq_categories
	_, err := sb.Update("sq_upd_from").
		Set("name", sqrl.Expr("sq_categories.description")).
		From("sq_categories").
		Where("sq_upd_from.cat = sq_categories.name").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_upd_from WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Fresh fruits", name)
}

func TestUpdateFromSelect(t *testing.T) {
	if isMySQL() {
		t.Skip("UPDATE ... FROM (subquery) not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_fsel", "(id INTEGER, new_name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_fsel VALUES (1, 'placeholder')")

	inner := sqrl.Select("id", "name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	_, err := sb.Update("sq_upd_fsel").
		Set("new_name", sqrl.Expr("sub.name")).
		FromSelect(inner, "sub").
		Where("sq_upd_fsel.id = sub.id").
		Exec()

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT new_name FROM sq_upd_fsel WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

// ---------------------------------------------------------------------------
// PlaceholderFormat
// ---------------------------------------------------------------------------

func TestUpdatePlaceholderFormat(t *testing.T) {
	// Arrange
	q := sqrl.Update("items").Set("a", 1).Where(sqrl.Eq{"id": 2}).PlaceholderFormat(sqrl.Dollar)

	// Act
	sqlStr, args, err := q.ToSQL()

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "UPDATE items SET a = $1 WHERE id = $2", sqlStr)
	assert.Equal(t, []interface{}{1, 2}, args)
}

// ---------------------------------------------------------------------------
// ToSQL and MustSQL
// ---------------------------------------------------------------------------

func TestUpdateToSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("items").Set("name", "x").Where(sqrl.Eq{"id": 1})

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "UPDATE items SET name = ? WHERE id = ?", sqlStr)
		assert.Equal(t, []interface{}{"x", 1}, args)
	})

	t.Run("MultipleSetClauses", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("items").Set("a", 1).Set("b", 2)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "UPDATE items SET a = ?, b = ?", sqlStr)
		assert.Equal(t, []interface{}{1, 2}, args)
	})
}

func TestUpdateMustSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("items").Set("name", "x")

		// Act
		sqlStr, _ := q.MustSQL()

		// Assert
		assert.Equal(t, "UPDATE items SET name = ?", sqlStr)
	})

	t.Run("PanicsOnError", func(t *testing.T) {
		// Arrange — no table
		q := sqrl.Update("").Set("name", "x")

		// Act & Assert
		assert.Panics(t, func() { q.MustSQL() })
	})
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

func TestUpdateErrors(t *testing.T) {
	t.Run("NoTable", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("").Set("name", "x")

		// Act
		_, _, err := q.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must specify a table")
	})

	t.Run("NoSetClauses", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("items")

		// Act
		_, _, err := q.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one Set clause")
	})

	t.Run("NoRunnerExec", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("items").Set("name", "x")

		// Act
		_, err := q.Exec()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerQuery", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("items").Set("name", "x")

		// Act
		_, err := q.Query()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerScan", func(t *testing.T) {
		// Arrange
		q := sqrl.Update("items").Set("name", "x")

		// Act
		var v string
		err := q.Scan(&v)

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}
