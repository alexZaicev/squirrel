package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// Placeholders
// ---------------------------------------------------------------------------

func TestPlaceholders(t *testing.T) {
	t.Run("Zero", func(t *testing.T) {
		assert.Equal(t, "", sqrl.Placeholders(0))
	})

	t.Run("Negative", func(t *testing.T) {
		assert.Equal(t, "", sqrl.Placeholders(-1))
	})

	t.Run("One", func(t *testing.T) {
		assert.Equal(t, "?", sqrl.Placeholders(1))
	})

	t.Run("Three", func(t *testing.T) {
		assert.Equal(t, "?,?,?", sqrl.Placeholders(3))
	})

	t.Run("Five", func(t *testing.T) {
		assert.Equal(t, "?,?,?,?,?", sqrl.Placeholders(5))
	})
}

// ---------------------------------------------------------------------------
// PlaceholderFormat
// ---------------------------------------------------------------------------

func TestPlaceholderFormats(t *testing.T) {
	t.Run("Question", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("a").From("t").Where("x = ?", 1).PlaceholderFormat(sqrl.Question)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT a FROM t WHERE x = ?", sqlStr)
	})

	t.Run("Dollar", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("a").From("t").Where("x = ? AND y = ?", 1, 2).PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT a FROM t WHERE x = $1 AND y = $2", sqlStr)
	})

	t.Run("Colon", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("a").From("t").Where("x = ?", 1).PlaceholderFormat(sqrl.Colon)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT a FROM t WHERE x = :1", sqlStr)
	})

	t.Run("AtP", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("a").From("t").Where("x = ?", 1).PlaceholderFormat(sqrl.AtP)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT a FROM t WHERE x = @p1", sqlStr)
	})

	t.Run("EscapedDoublePlaceholder", func(t *testing.T) {
		// Arrange — ?? should become literal ?
		q := sqrl.Select("a").From("t").Where("x ?? y").PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT a FROM t WHERE x ? y", sqlStr)
	})
}

// ---------------------------------------------------------------------------
// DebugSqlizer
// ---------------------------------------------------------------------------

func TestDebugSqlizer(t *testing.T) {
	t.Run("SimpleQuery", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items").Where("id = ?", 42)

		// Act
		result := sqrl.DebugSqlizer(q)

		// Assert
		assert.Equal(t, "SELECT name FROM items WHERE id = '42'", result)
	})

	t.Run("MultipleArgs", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items").Where("id > ? AND id < ?", 1, 10)

		// Act
		result := sqrl.DebugSqlizer(q)

		// Assert
		assert.Equal(t, "SELECT name FROM items WHERE id > '1' AND id < '10'", result)
	})

	t.Run("NoArgs", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items")

		// Act
		result := sqrl.DebugSqlizer(q)

		// Assert
		assert.Equal(t, "SELECT name FROM items", result)
	})

	t.Run("ErrorInToSQL", func(t *testing.T) {
		// Arrange — no columns → ToSQL error
		q := sqrl.Select()

		// Act
		result := sqrl.DebugSqlizer(q)

		// Assert
		assert.Contains(t, result, "[ToSQL error:")
	})

	t.Run("StringArg", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items").Where("cat = ?", "fruit")

		// Act
		result := sqrl.DebugSqlizer(q)

		// Assert
		assert.Equal(t, "SELECT name FROM items WHERE cat = 'fruit'", result)
	})
}

// ---------------------------------------------------------------------------
// DebugSqlizer — additional
// ---------------------------------------------------------------------------

func TestDebugSqlizerAdditional(t *testing.T) {
	t.Run("DollarPlaceholder", func(t *testing.T) {
		// Arrange — SelectBuilder with Dollar format does not implement
		// placeholderDebugger, so DebugSqlizer cannot find the $N placeholders
		// and returns an error message. This is expected behavior.
		q := sqrl.Select("name").From("items").Where("id = ?", 42).PlaceholderFormat(sqrl.Dollar)

		// Act
		result := sqrl.DebugSqlizer(q)

		// Assert — DebugSqlizer can't handle Dollar-format builders
		assert.Contains(t, result, "[DebugSqlizer error:")
	})

	t.Run("TooManyPlaceholders", func(t *testing.T) {
		// Arrange — more ? than args is an error
		result := sqrl.DebugSqlizer(sqrl.Expr("? AND ?", 1))

		// Assert
		assert.Contains(t, result, "[DebugSqlizer error: too many placeholders")
	})

	t.Run("TooManyArgs", func(t *testing.T) {
		// Arrange — more args than ? is an error
		result := sqrl.DebugSqlizer(sqrl.Expr("x = ?", 1, 2))

		// Assert
		assert.Contains(t, result, "[DebugSqlizer error: not enough placeholders")
	})
}

// ---------------------------------------------------------------------------
// StatementBuilder convenience functions
// ---------------------------------------------------------------------------

func TestStatementBuilderConvenience(t *testing.T) {
	t.Run("Select", func(t *testing.T) {
		// Act
		sqlStr, _, err := sqrl.Select("a", "b").From("t").ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT a, b FROM t", sqlStr)
	})

	t.Run("Insert", func(t *testing.T) {
		// Act
		sqlStr, args, err := sqrl.Insert("t").Columns("a").Values(1).ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO t (a) VALUES (?)", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("Replace", func(t *testing.T) {
		// Act
		sqlStr, _, err := sqrl.Replace("t").Columns("a").Values(1).ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "REPLACE INTO t (a) VALUES (?)", sqlStr)
	})

	t.Run("Update", func(t *testing.T) {
		// Act
		sqlStr, args, err := sqrl.Update("t").Set("a", 1).ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "UPDATE t SET a = ?", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("Delete", func(t *testing.T) {
		// Act
		sqlStr, _, err := sqrl.Delete("t").ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM t", sqlStr)
	})

	t.Run("Case", func(t *testing.T) {
		// Act
		sqlStr, _, err := sqrl.Case().When("1=1", "'yes'").ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "CASE")
		assert.Contains(t, sqlStr, "WHEN")
	})
}

// ---------------------------------------------------------------------------
// StatementBuilder with RunWith propagation
// ---------------------------------------------------------------------------

func TestStatementBuilderRunWith(t *testing.T) {
	// Arrange — create a StatementBuilder with RunWith set
	mySB := sqrl.StatementBuilder.PlaceholderFormat(phf()).RunWith(db)

	// Act — use it to build and execute a query
	q := mySB.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"apple"}, names)
}

func TestStatementBuilderPlaceholderPropagation(t *testing.T) {
	// Arrange — create a StatementBuilder with Dollar format
	mySB := sqrl.StatementBuilder.PlaceholderFormat(sqrl.Dollar)

	// Act
	q := mySB.Select("name").From("items").Where("id = ?", 1)
	sqlStr, _, err := q.ToSQL()

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "SELECT name FROM items WHERE id = $1", sqlStr)
}

// ---------------------------------------------------------------------------
// ExecWith / QueryWith / QueryRowWith
// ---------------------------------------------------------------------------

func TestExecWith(t *testing.T) {
	// Arrange
	createTable(t, "sq_exec_with", "(id INTEGER, name TEXT)")
	q := sqrl.Insert("sq_exec_with").Columns("id", "name").Values(1, "exec_with").PlaceholderFormat(phf())

	// Act
	_, err := sqrl.ExecWith(db, q)

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_exec_with").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "exec_with", name)
}

func TestQueryWith(t *testing.T) {
	// Arrange
	q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf())

	// Act
	rows, err := sqrl.QueryWith(db, q)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))

	// Assert
	assert.Equal(t, "apple", name)
}

func TestQueryRowWith(t *testing.T) {
	// Arrange
	q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf())

	// Act
	var name string
	err := sqrl.QueryRowWith(sqrl.WrapStdSQL(db), q).Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

// ---------------------------------------------------------------------------
// WrapStdSQL / WrapStdSQLCtx
// ---------------------------------------------------------------------------

func TestWrapStdSQL(t *testing.T) {
	// Arrange
	runner := sqrl.WrapStdSQL(db)
	q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf())

	// Act
	var name string
	err := sqrl.QueryRowWith(runner, q).Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

func TestWrapStdSQLCtx(t *testing.T) {
	// Arrange — WrapStdSQLCtx gives a RunnerContext
	runner := sqrl.WrapStdSQLCtx(db)

	q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf()).RunWith(runner)

	// Act
	var name string
	err := q.Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

// ---------------------------------------------------------------------------
// StmtCache
// ---------------------------------------------------------------------------

func TestStmtCache(t *testing.T) {
	t.Run("BasicUsage", func(t *testing.T) {
		// Arrange
		cache := sqrl.NewStmtCache(db)
		defer cache.Clear()

		q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf()).RunWith(cache)

		// Act
		var name string
		err := q.Scan(&name)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "apple", name)
	})

	t.Run("CachedReusesPreparedStatement", func(t *testing.T) {
		// Arrange — run the same query twice; second call uses cached stmt
		cache := sqrl.NewStmtCache(db)
		defer cache.Clear()

		q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf()).RunWith(cache)

		// Act — first execution
		var name1 string
		err := q.Scan(&name1)
		require.NoError(t, err)

		// Act — second execution (uses cached prepared statement)
		var name2 string
		err = q.Scan(&name2)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "apple", name1)
		assert.Equal(t, "apple", name2)
	})

	t.Run("Exec", func(t *testing.T) {
		// Arrange
		cache := sqrl.NewStmtCache(db)
		defer cache.Clear()
		createTable(t, "sq_cache_exec", "(id INTEGER, name TEXT)")

		q := sqrl.Insert("sq_cache_exec").Columns("id", "name").Values(1, "cached").PlaceholderFormat(phf()).RunWith(cache)

		// Act
		_, err := q.Exec()

		// Assert
		require.NoError(t, err)

		var name string
		err = db.QueryRow("SELECT name FROM sq_cache_exec").Scan(&name)
		require.NoError(t, err)
		assert.Equal(t, "cached", name)
	})

	t.Run("Query", func(t *testing.T) {
		// Arrange
		cache := sqrl.NewStmtCache(db)
		defer cache.Clear()

		q := sqrl.Select("name").From("sq_items").OrderBy("id").Limit(2).PlaceholderFormat(phf()).RunWith(cache)

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var n string
			require.NoError(t, rows.Scan(&n))
			names = append(names, n)
		}

		// Assert
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("Clear", func(t *testing.T) {
		// Arrange
		cache := sqrl.NewStmtCache(db)

		// Warm the cache with a query
		q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf()).RunWith(cache)
		var name string
		require.NoError(t, q.Scan(&name))

		// Act — clear all cached statements
		err := cache.Clear()

		// Assert
		assert.NoError(t, err)

		// Should still work after clearing (will re-prepare)
		require.NoError(t, q.Scan(&name))
		assert.Equal(t, "apple", name)
	})
}

// ---------------------------------------------------------------------------
// StmtCacheProxy
// ---------------------------------------------------------------------------

func TestStmtCacheProxy(t *testing.T) {
	// Arrange
	proxy := sqrl.NewStmtCacheProxy(db)
	q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf()).RunWith(proxy)

	// Act
	var name string
	err := q.Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

func TestStmtCacheProxyBegin(t *testing.T) {
	// Arrange
	proxy := sqrl.NewStmtCacheProxy(db)

	// Act
	tx, err := proxy.Begin()

	// Assert
	require.NoError(t, err)
	require.NotNil(t, tx)
	tx.Rollback()
}

// ---------------------------------------------------------------------------
// StatementBuilder.Where propagation
// ---------------------------------------------------------------------------

func TestStatementBuilderWhere(t *testing.T) {
	// Arrange — build a StatementBuilder with Where pre-set
	mySB := sqrl.StatementBuilder.PlaceholderFormat(phf()).RunWith(db).Where(sqrl.NotEq{"category": nil})

	// Act — the Where should be inherited by the Select
	q := mySB.Select("name").From("sq_items").OrderBy("name")
	names := queryStrings(t, q)

	// Assert — mystery (NULL category) is excluded
	assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant"}, names)
}

// ---------------------------------------------------------------------------
// Transaction usage
// ---------------------------------------------------------------------------

func TestTransactionCommit(t *testing.T) {
	// Arrange
	createTable(t, "sq_tx_cm", "(id INTEGER, name TEXT)")

	tx, err := db.Begin()
	require.NoError(t, err)

	// Act — insert within transaction and commit
	_, err = sqrl.Insert("sq_tx_cm").
		Columns("id", "name").
		Values(1, "committed").
		PlaceholderFormat(phf()).
		RunWith(tx).
		Exec()
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Assert — visible outside the transaction
	var name string
	err = db.QueryRow("SELECT name FROM sq_tx_cm WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "committed", name)
}

func TestTransactionRollback(t *testing.T) {
	// Arrange
	createTable(t, "sq_tx_rb", "(id INTEGER, name TEXT)")

	tx, err := db.Begin()
	require.NoError(t, err)

	// Act — insert within transaction and rollback
	_, err = sqrl.Insert("sq_tx_rb").
		Columns("id", "name").
		Values(1, "will_rollback").
		PlaceholderFormat(phf()).
		RunWith(tx).
		Exec()
	require.NoError(t, err)

	tx.Rollback()

	// Assert — not visible outside the transaction
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_tx_rb").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestTransactionSelectInsertUpdate(t *testing.T) {
	// Arrange
	createTable(t, "sq_tx_full", "(id INTEGER, name TEXT)")

	tx, err := db.Begin()
	require.NoError(t, err)

	txSB := sqrl.StatementBuilder.PlaceholderFormat(phf()).RunWith(tx)

	// Act — insert
	_, err = txSB.Insert("sq_tx_full").Columns("id", "name").Values(1, "original").Exec()
	require.NoError(t, err)

	// Act — update within same transaction
	_, err = txSB.Update("sq_tx_full").Set("name", "updated").Where(sqrl.Eq{"id": 1}).Exec()
	require.NoError(t, err)

	// Act — select within same transaction
	var name string
	err = txSB.Select("name").From("sq_tx_full").Where(sqrl.Eq{"id": 1}).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "updated", name)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Assert — visible outside
	err = db.QueryRow("SELECT name FROM sq_tx_full WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "updated", name)
}

// ---------------------------------------------------------------------------
// PlaceholderFormats — additional
// ---------------------------------------------------------------------------

func TestPlaceholderFormatMultipleArgs(t *testing.T) {
	// Arrange — verify correct numbering with many args
	q := sqrl.Select("a").From("t").
		Where("x = ? AND y = ? AND z = ?", 1, 2, 3).
		PlaceholderFormat(sqrl.Dollar)

	// Act
	sqlStr, args, err := q.ToSQL()

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "SELECT a FROM t WHERE x = $1 AND y = $2 AND z = $3", sqlStr)
	assert.Equal(t, []interface{}{1, 2, 3}, args)
}
