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

// ---------------------------------------------------------------------------
// RETURNING (first-class)
// ---------------------------------------------------------------------------

func TestUpdateReturningSingleColumn(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_ret1", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_ret1 VALUES (1, 'old')")

	// Act
	rows, err := sb.Update("sq_upd_ret1").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		Returning("name").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))
	assert.Equal(t, "new", name)
	assert.False(t, rows.Next())
}

func TestUpdateReturningMultipleColumns(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_ret2", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_ret2 VALUES (1, 'old')")

	// Act
	rows, err := sb.Update("sq_upd_ret2").
		Set("name", "updated").
		Where(sqrl.Eq{"id": 1}).
		Returning("id", "name").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var id int
	var name string
	require.NoError(t, rows.Scan(&id, &name))
	assert.Equal(t, 1, id)
	assert.Equal(t, "updated", name)
	assert.False(t, rows.Next())
}

func TestUpdateReturningStar(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_retstar", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_retstar VALUES (1, 'old')")

	// Act
	rows, err := sb.Update("sq_upd_retstar").
		Set("name", "star").
		Where(sqrl.Eq{"id": 1}).
		Returning("*").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var id int
	var name string
	require.NoError(t, rows.Scan(&id, &name))
	assert.Equal(t, 1, id)
	assert.Equal(t, "star", name)
	assert.False(t, rows.Next())
}

func TestUpdateReturningWithScan(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_retscan", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_retscan VALUES (1, 'old')")

	// Act
	var name string
	err := sb.Update("sq_upd_retscan").
		Set("name", "scanned").
		Where(sqrl.Eq{"id": 1}).
		Returning("name").
		Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "scanned", name)
}

func TestUpdateReturningMultipleRows(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_retmulti", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_retmulti VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Act — update all rows
	rows, err := sb.Update("sq_upd_retmulti").
		Set("name", "updated").
		Returning("id", "name").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		id   int
		name string
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())
	assert.Len(t, results, 3)
	for _, r := range results {
		assert.Equal(t, "updated", r.name)
	}
}

func TestUpdateReturningWithSuffix(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_upd_retsfx", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_upd_retsfx VALUES (1, 'old')")

	// Act — RETURNING appears before suffix
	rows, err := sb.Update("sq_upd_retsfx").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		Returning("name").
		Suffix("/* post-returning */").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))
	assert.Equal(t, "new", name)
}

func TestUpdateReturningToSQL(t *testing.T) {
	t.Run("SingleColumn", func(t *testing.T) {
		q := sqrl.Update("t").Set("a", 1).Where(sqrl.Eq{"id": 2}).
			Returning("id")

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = ? WHERE id = ? RETURNING id", sqlStr)
		assert.Equal(t, []interface{}{1, 2}, args)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		q := sqrl.Update("t").Set("a", 1).
			Returning("id", "a")

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = ? RETURNING id, a", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("Star", func(t *testing.T) {
		q := sqrl.Update("t").Set("a", 1).
			Returning("*")

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = ? RETURNING *", sqlStr)
	})

	t.Run("WithDollarPlaceholders", func(t *testing.T) {
		q := sqrl.Update("t").Set("a", 1).Where(sqrl.Eq{"id": 2}).
			Returning("id").
			PlaceholderFormat(sqrl.Dollar)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = $1 WHERE id = $2 RETURNING id", sqlStr)
		assert.Equal(t, []interface{}{1, 2}, args)
	})

	t.Run("ChainedCalls", func(t *testing.T) {
		q := sqrl.Update("t").Set("a", 1).
			Returning("id").
			Returning("a")

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = ? RETURNING id, a", sqlStr)
	})
}

// ---------------------------------------------------------------------------
// Set with subquery — Dollar placeholder numbering (GitHub #326)
// ---------------------------------------------------------------------------

func TestUpdateSetSubqueryDollarPlaceholders(t *testing.T) {
	// Regression test: Dollar placeholders must number sequentially across the
	// outer SET clause and the inner subquery.
	t.Run("SingleSubquery", func(t *testing.T) {
		q := sqrl.Update("t").
			Set("a", 1).
			Set("b", sqrl.Select("x").From("y").Where("z = ?", 2)).
			Where("id = ?", 3).
			PlaceholderFormat(sqrl.Dollar)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = $1, b = (SELECT x FROM y WHERE z = $2) WHERE id = $3", sqlStr)
		assert.Equal(t, []interface{}{1, 2, 3}, args)
	})

	t.Run("MultipleSubqueries", func(t *testing.T) {
		q := sqrl.Update("t").
			Set("a", sqrl.Select("x").From("y").Where("y.id = ?", 1)).
			Set("b", sqrl.Select("p").From("q").Where("q.id = ?", 2)).
			Where("id = ?", 3).
			PlaceholderFormat(sqrl.Dollar)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t,
			"UPDATE t "+
				"SET a = (SELECT x FROM y WHERE y.id = $1), "+
				"b = (SELECT p FROM q WHERE q.id = $2) "+
				"WHERE id = $3", sqlStr)
		assert.Equal(t, []interface{}{1, 2, 3}, args)
	})

	t.Run("SubqueryWithColon", func(t *testing.T) {
		q := sqrl.Update("t").
			Set("a", 1).
			Set("b", sqrl.Select("x").From("y").Where("z = ?", 2)).
			Where("id = ?", 3).
			PlaceholderFormat(sqrl.Colon)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = :1, b = (SELECT x FROM y WHERE z = :2) WHERE id = :3", sqlStr)
		assert.Equal(t, []interface{}{1, 2, 3}, args)
	})

	t.Run("SubqueryWithAtP", func(t *testing.T) {
		q := sqrl.Update("t").
			Set("a", 1).
			Set("b", sqrl.Select("x").From("y").Where("z = ?", 2)).
			Where("id = ?", 3).
			PlaceholderFormat(sqrl.AtP)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = @p1, b = (SELECT x FROM y WHERE z = @p2) WHERE id = @p3", sqlStr)
		assert.Equal(t, []interface{}{1, 2, 3}, args)
	})
}

func TestUpdateSetSubqueryExecution(t *testing.T) {
	// End-to-end execution: SET col = (SELECT ...) with the current driver's
	// placeholder format.
	createTable(t, "sq_upd_sub_src", "(id INTEGER, val INTEGER)")
	createTable(t, "sq_upd_sub_dst", "(id INTEGER, total INTEGER)")
	seedTable(t, "INSERT INTO sq_upd_sub_src VALUES (1, 42)")
	seedTable(t, "INSERT INTO sq_upd_sub_dst VALUES (1, 0)")

	_, err := sb.Update("sq_upd_sub_dst").
		Set("total", sqrl.Select("val").From("sq_upd_sub_src").Where(sqrl.Eq{"sq_upd_sub_src.id": 1})).
		Where(sqrl.Eq{"id": 1}).
		Exec()
	require.NoError(t, err)

	var total int
	err = db.QueryRow("SELECT total FROM sq_upd_sub_dst WHERE id = 1").Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 42, total)
}

func TestUpdateSetMapSubqueryDollarPlaceholders(t *testing.T) {
	// Regression: SetMap with a Sqlizer value should produce correctly
	// numbered Dollar placeholders, same as Set().
	q := sqrl.Update("t").
		SetMap(map[string]interface{}{
			"a": 1,
			"b": sqrl.Select("x").From("y").Where("z = ?", 2),
		}).
		Where("id = ?", 3).
		PlaceholderFormat(sqrl.Dollar)

	sqlStr, args, err := q.ToSQL()
	require.NoError(t, err)
	assert.Equal(t, "UPDATE t SET a = $1, b = (SELECT x FROM y WHERE z = $2) WHERE id = $3", sqlStr)
	assert.Equal(t, []interface{}{1, 2, 3}, args)
}

func TestUpdateSetSubqueryWithWhereSubqueryDollarPlaceholders(t *testing.T) {
	// Mixed: Set with subquery + Where with Eq subquery, Dollar placeholders.
	q := sqrl.Update("t").
		Set("a", 1).
		Set("b", sqrl.Select("x").From("y").Where("z = ?", 2)).
		Where(sqrl.Eq{"t.id": sqrl.Select("id").From("s").Where("s.active = ?", true)}).
		PlaceholderFormat(sqrl.Dollar)

	sqlStr, args, err := q.ToSQL()
	require.NoError(t, err)
	assert.Equal(t,
		"UPDATE t SET a = $1, b = (SELECT x FROM y WHERE z = $2) "+
			"WHERE t.id IN (SELECT id FROM s WHERE s.active = $3)", sqlStr)
	assert.Equal(t, []interface{}{1, 2, true}, args)
}

func TestUpdateSetMultipleSubqueriesExecution(t *testing.T) {
	// End-to-end: multiple SET clauses each with a subquery.
	createTable(t, "sq_upd_msub_s1", "(id INTEGER, v1 INTEGER)")
	createTable(t, "sq_upd_msub_s2", "(id INTEGER, v2 INTEGER)")
	createTable(t, "sq_upd_msub_dst", "(id INTEGER, a INTEGER, b INTEGER)")
	seedTable(t, "INSERT INTO sq_upd_msub_s1 VALUES (1, 10)")
	seedTable(t, "INSERT INTO sq_upd_msub_s2 VALUES (1, 20)")
	seedTable(t, "INSERT INTO sq_upd_msub_dst VALUES (1, 0, 0)")

	_, err := sb.Update("sq_upd_msub_dst").
		Set("a", sqrl.Select("v1").From("sq_upd_msub_s1").Where(sqrl.Eq{"sq_upd_msub_s1.id": 1})).
		Set("b", sqrl.Select("v2").From("sq_upd_msub_s2").Where(sqrl.Eq{"sq_upd_msub_s2.id": 1})).
		Where(sqrl.Eq{"id": 1}).
		Exec()
	require.NoError(t, err)

	var a, b int
	err = db.QueryRow("SELECT a, b FROM sq_upd_msub_dst WHERE id = 1").Scan(&a, &b)
	require.NoError(t, err)
	assert.Equal(t, 10, a)
	assert.Equal(t, 20, b)
}
