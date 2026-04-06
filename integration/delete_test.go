package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// Basic DELETE
// ---------------------------------------------------------------------------

func TestDeleteAll(t *testing.T) {
	// Arrange
	createTable(t, "sq_del_all", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_all VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Act
	_, err := sb.Delete("sq_del_all").Exec()

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_del_all").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// ---------------------------------------------------------------------------
// WHERE
// ---------------------------------------------------------------------------

func TestDeleteWhere(t *testing.T) {
	t.Run("SingleCondition", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_wh1", "(id INTEGER, name TEXT)")
		seedTable(t, "INSERT INTO sq_del_wh1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")

		// Act
		_, err := sb.Delete("sq_del_wh1").
			Where(sqrl.Eq{"id": 2}).
			Exec()

		// Assert
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_wh1").OrderBy("id"))
		assert.Equal(t, []string{"a", "c"}, names)
	})

	t.Run("MultipleConditions", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_wh2", "(id INTEGER, name TEXT, cat TEXT)")
		seedTable(t, "INSERT INTO sq_del_wh2 VALUES (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'y')")

		// Act
		_, err := sb.Delete("sq_del_wh2").
			Where(sqrl.Eq{"cat": "x"}).
			Where(sqrl.Gt{"id": 1}).
			Exec()

		// Assert — only id=2 deleted (cat='x' AND id>1)
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_wh2").OrderBy("id"))
		assert.Equal(t, []string{"a", "c"}, names)
	})

	t.Run("StringPredicate", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_whs", "(id INTEGER, name TEXT)")
		seedTable(t, "INSERT INTO sq_del_whs VALUES (1, 'a'), (2, 'b')")

		// Act
		_, err := sb.Delete("sq_del_whs").
			Where("id = ?", 1).
			Exec()

		// Assert
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_whs"))
		assert.Equal(t, []string{"b"}, names)
	})

	t.Run("NoMatch", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_no", "(id INTEGER, name TEXT)")
		seedTable(t, "INSERT INTO sq_del_no VALUES (1, 'a'), (2, 'b')")

		// Act — delete non-existent row
		res, err := sb.Delete("sq_del_no").
			Where(sqrl.Eq{"id": 999}).
			Exec()

		// Assert — no error, no rows affected
		require.NoError(t, err)

		affected, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(0), affected)
	})
}

// ---------------------------------------------------------------------------
// ORDER BY, LIMIT, OFFSET
// ---------------------------------------------------------------------------

func TestDeleteOrderByLimit(t *testing.T) {
	if isPostgres() {
		t.Skip("DELETE with ORDER BY/LIMIT not supported on PostgreSQL")
	}
	// SQLite only supports ORDER BY/LIMIT on DELETE if compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT.
	// The default go-sqlite3 build does not enable this flag.
	if driverName == "sqlite3" {
		t.Skip("DELETE with ORDER BY/LIMIT not supported on default SQLite builds")
	}

	// Arrange
	createTable(t, "sq_del_lim", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_lim VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Act — delete first row by ascending id
	_, err := sb.Delete("sq_del_lim").
		OrderBy("id ASC").
		Limit(1).
		Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_del_lim").OrderBy("id"))
	assert.Equal(t, []string{"b", "c"}, names)
}

// ---------------------------------------------------------------------------
// Prefix and Suffix
// ---------------------------------------------------------------------------

func TestDeletePrefix(t *testing.T) {
	// Arrange
	createTable(t, "sq_del_pfx", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_pfx VALUES (1, 'a')")

	// Act
	_, err := sb.Delete("sq_del_pfx").
		Prefix("/* delete prefix */").
		Where(sqrl.Eq{"id": 1}).
		Exec()

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_del_pfx").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestDeleteSuffix(t *testing.T) {
	// Arrange
	createTable(t, "sq_del_sfx", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_sfx VALUES (1, 'a')")

	// Act
	_, err := sb.Delete("sq_del_sfx").
		Where(sqrl.Eq{"id": 1}).
		Suffix("/* delete suffix */").
		Exec()

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_del_sfx").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// ---------------------------------------------------------------------------
// PrefixExpr / SuffixExpr
// ---------------------------------------------------------------------------

func TestDeletePrefixExpr(t *testing.T) {
	// Arrange
	createTable(t, "sq_del_pfe", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_pfe VALUES (1, 'gone')")

	// Act
	_, err := sb.Delete("sq_del_pfe").
		PrefixExpr(sqrl.Expr("/* delete prefix expr */")).
		Where(sqrl.Eq{"id": 1}).
		Exec()

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_del_pfe").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestDeleteSuffixExpr(t *testing.T) {
	// Arrange
	createTable(t, "sq_del_sfe", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_sfe VALUES (1, 'gone')")

	// Act
	_, err := sb.Delete("sq_del_sfe").
		Where(sqrl.Eq{"id": 1}).
		SuffixExpr(sqrl.Expr("/* delete suffix expr */")).
		Exec()

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_del_sfe").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// ---------------------------------------------------------------------------
// Builder immutability
// ---------------------------------------------------------------------------

func TestDeleteBuilderImmutability(t *testing.T) {
	// Arrange
	createTable(t, "sq_del_imm", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_imm VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	base := sb.Delete("sq_del_imm")
	q1 := base.Where(sqrl.Eq{"id": 1})
	q2 := base.Where(sqrl.Eq{"id": 3})

	// Act — delete id=1
	_, err := q1.Exec()
	require.NoError(t, err)

	// Assert — id=2 and id=3 still present
	names := queryStrings(t, sb.Select("name").From("sq_del_imm").OrderBy("id"))
	assert.Equal(t, []string{"b", "c"}, names)

	// Act — delete id=3 from the independent branch
	_, err = q2.Exec()
	require.NoError(t, err)

	names = queryStrings(t, sb.Select("name").From("sq_del_imm").OrderBy("id"))
	assert.Equal(t, []string{"b"}, names)
}

// ---------------------------------------------------------------------------
// RowsAffected
// ---------------------------------------------------------------------------

func TestDeleteRowsAffected(t *testing.T) {
	// Arrange
	createTable(t, "sq_del_ra", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_ra VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Act
	res, err := sb.Delete("sq_del_ra").
		Where(sqrl.Lt{"id": 3}).
		Exec()

	// Assert
	require.NoError(t, err)

	affected, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(2), affected)
}

// ---------------------------------------------------------------------------
// ToSQL and MustSQL
// ---------------------------------------------------------------------------

func TestDeleteToSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		q := sqrl.Delete("items").Where(sqrl.Eq{"id": 1})

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM items WHERE id = ?", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("WithOrderByLimitOffset", func(t *testing.T) {
		// Arrange
		q := sqrl.Delete("items").OrderBy("id").Limit(5).Offset(10)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM items ORDER BY id LIMIT 5 OFFSET 10", sqlStr)
	})
}

func TestDeleteMustSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		q := sqrl.Delete("items")

		// Act
		sqlStr, _ := q.MustSQL()

		// Assert
		assert.Equal(t, "DELETE FROM items", sqlStr)
	})

	t.Run("PanicsOnError", func(t *testing.T) {
		// Arrange — no From table
		q := sqrl.Delete("")

		// Act & Assert
		assert.Panics(t, func() { q.MustSQL() })
	})
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

func TestDeleteErrors(t *testing.T) {
	t.Run("NoFrom", func(t *testing.T) {
		// Arrange
		q := sqrl.Delete("")

		// Act
		_, _, err := q.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must specify a From table")
	})

	t.Run("NoRunnerExec", func(t *testing.T) {
		// Arrange
		q := sqrl.Delete("items")

		// Act
		_, err := q.Exec()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerQuery", func(t *testing.T) {
		// Arrange
		q := sqrl.Delete("items")

		// Act
		_, err := q.Query()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}
