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
		sqlStr, args, err := q.ToSQL()

		// Assert — LIMIT and OFFSET are now parameterized
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM items ORDER BY id LIMIT ? OFFSET ?", sqlStr)
		assert.Equal(t, []interface{}{uint64(5), uint64(10)}, args)
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

// ---------------------------------------------------------------------------
// RETURNING (first-class)
// ---------------------------------------------------------------------------

func TestDeleteReturningSingleColumn(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_del_ret1", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_ret1 VALUES (1, 'gone')")

	// Act
	rows, err := sb.Delete("sq_del_ret1").
		Where(sqrl.Eq{"id": 1}).
		Returning("name").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))
	assert.Equal(t, "gone", name)
	assert.False(t, rows.Next())

	// Verify row is actually deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_del_ret1").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestDeleteReturningMultipleColumns(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_del_ret2", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_ret2 VALUES (1, 'deleted')")

	// Act
	rows, err := sb.Delete("sq_del_ret2").
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
	assert.Equal(t, "deleted", name)
	assert.False(t, rows.Next())
}

func TestDeleteReturningStar(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_del_retstar", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_retstar VALUES (1, 'star_del')")

	// Act
	rows, err := sb.Delete("sq_del_retstar").
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
	assert.Equal(t, "star_del", name)
	assert.False(t, rows.Next())
}

func TestDeleteReturningWithQuery(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_del_retscan", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_retscan VALUES (1, 'scanned_del')")

	// Act
	rows, err := sb.Delete("sq_del_retscan").
		Where(sqrl.Eq{"id": 1}).
		Returning("name").
		Query()

	// Assert
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))
	assert.Equal(t, "scanned_del", name)
	assert.False(t, rows.Next())
}

func TestDeleteReturningMultipleRows(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_del_retmulti", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_retmulti VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	// Act — delete all rows
	rows, err := sb.Delete("sq_del_retmulti").
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

	// Verify table is empty
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_del_retmulti").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestDeleteReturningWithSuffix(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	createTable(t, "sq_del_retsfx", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_del_retsfx VALUES (1, 'suffixed_del')")

	// Act — RETURNING appears before suffix
	rows, err := sb.Delete("sq_del_retsfx").
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
	assert.Equal(t, "suffixed_del", name)
}

func TestDeleteReturningToSQL(t *testing.T) {
	t.Run("SingleColumn", func(t *testing.T) {
		q := sqrl.Delete("t").Where(sqrl.Eq{"id": 1}).
			Returning("id")

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM t WHERE id = ? RETURNING id", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		q := sqrl.Delete("t").Where(sqrl.Eq{"id": 1}).
			Returning("id", "name")

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM t WHERE id = ? RETURNING id, name", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("Star", func(t *testing.T) {
		q := sqrl.Delete("t").
			Returning("*")

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM t RETURNING *", sqlStr)
	})

	t.Run("WithDollarPlaceholders", func(t *testing.T) {
		q := sqrl.Delete("t").Where(sqrl.Eq{"id": 1}).
			Returning("id").
			PlaceholderFormat(sqrl.Dollar)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM t WHERE id = $1 RETURNING id", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("ChainedCalls", func(t *testing.T) {
		q := sqrl.Delete("t").
			Returning("id").
			Returning("name")

		sqlStr, _, err := q.ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM t RETURNING id, name", sqlStr)
	})
}

// ---------------------------------------------------------------------------
// JOIN clauses
// ---------------------------------------------------------------------------

func TestDeleteJoin(t *testing.T) {
	if isPostgres() {
		t.Skip("DELETE ... JOIN is MySQL syntax; PostgreSQL uses DELETE ... USING")
	}
	if driverName == "sqlite3" {
		t.Skip("DELETE ... JOIN is not supported by SQLite")
	}

	t.Run("BasicJoin", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_j1", "(id INTEGER, name TEXT, ref_id INTEGER)")
		createTable(t, "sq_del_j1_ref", "(id INTEGER, active INTEGER)")
		seedTable(t, "INSERT INTO sq_del_j1 VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
		seedTable(t, "INSERT INTO sq_del_j1_ref VALUES (10, 0), (20, 1), (30, 0)")

		// Act — delete rows where joined table has active=0
		_, err := sb.Delete("sq_del_j1").
			Join("sq_del_j1_ref ON sq_del_j1.ref_id = sq_del_j1_ref.id").
			Where(sqrl.Eq{"sq_del_j1_ref.active": 0}).
			Exec()

		// Assert — rows 1 and 3 deleted (active=0)
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_j1").OrderBy("id"))
		assert.Equal(t, []string{"b"}, names)
	})

	t.Run("LeftJoin", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_lj", "(id INTEGER, name TEXT, ref_id INTEGER)")
		createTable(t, "sq_del_lj_ref", "(id INTEGER)")
		seedTable(t, "INSERT INTO sq_del_lj VALUES (1, 'a', 10), (2, 'b', 20)")
		seedTable(t, "INSERT INTO sq_del_lj_ref VALUES (10)")

		// Act — delete orphan rows (no match in ref table)
		_, err := sb.Delete("sq_del_lj").
			LeftJoin("sq_del_lj_ref ON sq_del_lj.ref_id = sq_del_lj_ref.id").
			Where("sq_del_lj_ref.id IS NULL").
			Exec()

		// Assert — row 2 (orphan) is deleted
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_lj").OrderBy("id"))
		assert.Equal(t, []string{"a"}, names)
	})

	t.Run("JoinWithPlaceholderArgs", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_jp", "(id INTEGER, name TEXT, ref_id INTEGER)")
		createTable(t, "sq_del_jp_ref", "(id INTEGER, status TEXT)")
		seedTable(t, "INSERT INTO sq_del_jp VALUES (1, 'a', 10), (2, 'b', 20)")
		seedTable(t, "INSERT INTO sq_del_jp_ref VALUES (10, 'inactive'), (20, 'active')")

		// Act — delete rows where joined ref has status='inactive'
		_, err := sb.Delete("sq_del_jp").
			Join("sq_del_jp_ref ON sq_del_jp.ref_id = sq_del_jp_ref.id AND sq_del_jp_ref.status = ?", "inactive").
			Exec()

		// Assert — row 1 deleted
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_jp").OrderBy("id"))
		assert.Equal(t, []string{"b"}, names)
	})

	t.Run("MultipleJoins", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_mj", "(id INTEGER, name TEXT, cat_id INTEGER)")
		createTable(t, "sq_del_mj_cat", "(id INTEGER, grp_id INTEGER)")
		createTable(t, "sq_del_mj_grp", "(id INTEGER, label TEXT)")
		seedTable(t, "INSERT INTO sq_del_mj VALUES (1, 'a', 10), (2, 'b', 20)")
		seedTable(t, "INSERT INTO sq_del_mj_cat VALUES (10, 100), (20, 200)")
		seedTable(t, "INSERT INTO sq_del_mj_grp VALUES (100, 'grpA'), (200, 'grpB')")

		// Act — delete through two joins where grp label = grpA
		_, err := sb.Delete("sq_del_mj").
			Join("sq_del_mj_cat ON sq_del_mj.cat_id = sq_del_mj_cat.id").
			Join("sq_del_mj_grp ON sq_del_mj_cat.grp_id = sq_del_mj_grp.id").
			Where(sqrl.Eq{"sq_del_mj_grp.label": "grpA"}).
			Exec()

		// Assert — row 1 deleted
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_mj").OrderBy("id"))
		assert.Equal(t, []string{"b"}, names)
	})
}

// ---------------------------------------------------------------------------
// USING clause (PostgreSQL)
// ---------------------------------------------------------------------------

func TestDeleteUsing(t *testing.T) {
	if !isPostgres() {
		t.Skip("DELETE ... USING is PostgreSQL-specific")
	}

	t.Run("BasicUsing", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_u1", "(id INTEGER, name TEXT, ref_id INTEGER)")
		createTable(t, "sq_del_u1_ref", "(id INTEGER, active INTEGER)")
		seedTable(t, "INSERT INTO sq_del_u1 VALUES (1, 'a', 10), (2, 'b', 20)")
		seedTable(t, "INSERT INTO sq_del_u1_ref VALUES (10, 0), (20, 1)")

		// Act — delete rows using reference table
		_, err := sb.Delete("sq_del_u1").
			Using("sq_del_u1_ref").
			Where("sq_del_u1.ref_id = sq_del_u1_ref.id AND sq_del_u1_ref.active = ?", 0).
			Exec()

		// Assert — row 1 deleted
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_u1").OrderBy("id"))
		assert.Equal(t, []string{"b"}, names)
	})

	t.Run("UsingMultipleTables", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_del_um", "(id INTEGER, name TEXT, ref_id INTEGER)")
		createTable(t, "sq_del_um_r1", "(id INTEGER, r2_id INTEGER)")
		createTable(t, "sq_del_um_r2", "(id INTEGER, label TEXT)")
		seedTable(t, "INSERT INTO sq_del_um VALUES (1, 'a', 10), (2, 'b', 20)")
		seedTable(t, "INSERT INTO sq_del_um_r1 VALUES (10, 100), (20, 200)")
		seedTable(t, "INSERT INTO sq_del_um_r2 VALUES (100, 'del'), (200, 'keep')")

		// Act — delete through two USING tables
		_, err := sb.Delete("sq_del_um").
			Using("sq_del_um_r1", "sq_del_um_r2").
			Where("sq_del_um.ref_id = sq_del_um_r1.id AND sq_del_um_r1.r2_id = sq_del_um_r2.id AND sq_del_um_r2.label = ?", "del").
			Exec()

		// Assert
		require.NoError(t, err)

		names := queryStrings(t, sb.Select("name").From("sq_del_um").OrderBy("id"))
		assert.Equal(t, []string{"b"}, names)
	})
}

// ---------------------------------------------------------------------------
// JOIN / USING SQL generation
// ---------------------------------------------------------------------------

func TestDeleteJoinSQLGeneration(t *testing.T) {
	t.Run("JoinDollar", func(t *testing.T) {
		sql, args, err := sqrl.Delete("t1").
			Join("t2 ON t1.id = t2.t1_id AND t2.status = ?", "inactive").
			Where("t1.id = ?", 1).
			PlaceholderFormat(sqrl.Dollar).
			ToSQL()
		require.NoError(t, err)
		assert.Equal(t,
			"DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id AND t2.status = $1 WHERE t1.id = $2",
			sql)
		assert.Equal(t, []interface{}{"inactive", 1}, args)
	})

	t.Run("UsingDollar", func(t *testing.T) {
		sql, args, err := sqrl.Delete("t1").
			Using("t2").
			Where("t1.id = t2.t1_id AND t2.active = ?", false).
			PlaceholderFormat(sqrl.Dollar).
			ToSQL()
		require.NoError(t, err)
		assert.Equal(t,
			"DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id AND t2.active = $1",
			sql)
		assert.Equal(t, []interface{}{false}, args)
	})

	t.Run("JoinExpr", func(t *testing.T) {
		sql, args, err := sqrl.Delete("orders").
			JoinClause(
				sqrl.JoinExpr("customers").
					Type(sqrl.JoinLeft).
					On("orders.customer_id = customers.id").
					On("customers.active = ?", false),
			).
			PlaceholderFormat(sqrl.Dollar).
			ToSQL()
		require.NoError(t, err)
		assert.Equal(t,
			"DELETE orders FROM orders LEFT JOIN customers ON orders.customer_id = customers.id AND customers.active = $1",
			sql)
		assert.Equal(t, []interface{}{false}, args)
	})

	t.Run("JoinUsing", func(t *testing.T) {
		sql, _, err := sqrl.Delete("t1").
			JoinUsing("t2", "id", "region").
			ToSQL()
		require.NoError(t, err)
		assert.Equal(t, "DELETE t1 FROM t1 JOIN t2 USING (id, region)", sql)
	})

	t.Run("JoinWithReturning", func(t *testing.T) {
		sql, args, err := sqrl.Delete("t1").
			Join("t2 ON t1.id = t2.t1_id").
			Where("t2.active = ?", false).
			Returning("t1.id").
			PlaceholderFormat(sqrl.Dollar).
			ToSQL()
		require.NoError(t, err)
		assert.Equal(t,
			"DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t2.active = $1 RETURNING t1.id",
			sql)
		assert.Equal(t, []interface{}{false}, args)
	})
}
