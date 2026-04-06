package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// SELECT context methods
// ---------------------------------------------------------------------------

func TestSelectQueryContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	rows, err := q.QueryContext(ctx)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))

	// Assert
	assert.Equal(t, "apple", name)
}

func TestSelectExecContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	assert.NoError(t, err)
}

func TestSelectScanContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	var name string
	err := q.ScanContext(ctx, &name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

func TestSelectQueryRowContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q := sb.Select("COUNT(*)").From("sq_items")

	// Act
	var count int
	err := q.QueryRowContext(ctx).Scan(&count)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 6, count)
}

// ---------------------------------------------------------------------------
// SELECT context — cancelled
// ---------------------------------------------------------------------------

func TestSelectQueryContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := sb.Select("name").From("sq_items")

	// Act
	_, err := q.QueryContext(ctx)

	// Assert
	assert.Error(t, err)
}

func TestSelectExecContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := sb.Select("name").From("sq_items")

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	assert.Error(t, err)
}

func TestSelectScanContextNoRows(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 999})

	// Act
	var name string
	err := q.ScanContext(ctx, &name)

	// Assert
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// INSERT context methods
// ---------------------------------------------------------------------------

func TestInsertExecContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_ins", "(id INTEGER, name TEXT)")

	q := sb.Insert("sq_ctx_ins").
		Columns("id", "name").
		Values(1, "ctx_test")

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ctx_ins").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "ctx_test", name)
}

func TestInsertQueryContextWithReturning(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_ins_q", "(id INTEGER, name TEXT)")

	q := sb.Insert("sq_ctx_ins_q").
		Columns("id", "name").
		Values(1, "ctx_ret").
		Suffix("RETURNING name")

	// Act
	rows, err := q.QueryContext(ctx)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))

	// Assert
	assert.Equal(t, "ctx_ret", name)
}

func TestInsertScanContext(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_ins_s", "(id INTEGER, name TEXT)")

	// Act
	var name string
	err := sb.Insert("sq_ctx_ins_s").
		Columns("id", "name").
		Values(1, "ctx_scan").
		Suffix("RETURNING name").
		ScanContext(ctx, &name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "ctx_scan", name)
}

func TestInsertExecContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	createTable(t, "sq_ctx_ins_c", "(id INTEGER, name TEXT)")

	q := sb.Insert("sq_ctx_ins_c").
		Columns("id", "name").
		Values(1, "fail")

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// UPDATE context methods
// ---------------------------------------------------------------------------

func TestUpdateExecContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_upd", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_upd VALUES (1, 'old')")

	q := sb.Update("sq_ctx_upd").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ctx_upd WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new", name)
}

func TestUpdateExecContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	createTable(t, "sq_ctx_upd_c", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_upd_c VALUES (1, 'old')")

	q := sb.Update("sq_ctx_upd_c").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	assert.Error(t, err)
}

func TestUpdateQueryContextWithReturning(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_upd_r", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_upd_r VALUES (1, 'old')")

	// Act
	rows, err := sb.Update("sq_ctx_upd_r").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		Suffix("RETURNING name").
		QueryContext(ctx)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))

	// Assert
	assert.Equal(t, "new", name)
}

// ---------------------------------------------------------------------------
// DELETE context methods
// ---------------------------------------------------------------------------

func TestDeleteExecContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_del", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_del VALUES (1, 'gone')")

	q := sb.Delete("sq_ctx_del").Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sq_ctx_del").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestDeleteExecContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	createTable(t, "sq_ctx_del_c", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_del_c VALUES (1, 'stay')")

	q := sb.Delete("sq_ctx_del_c").Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.ExecContext(ctx)

	// Assert
	assert.Error(t, err)
}

func TestDeleteQueryContextWithReturning(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_del_r", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_del_r VALUES (1, 'gone')")

	// Act
	rows, err := sb.Delete("sq_ctx_del_r").
		Where(sqrl.Eq{"id": 1}).
		Suffix("RETURNING name").
		QueryContext(ctx)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))

	// Assert
	assert.Equal(t, "gone", name)
}

// ---------------------------------------------------------------------------
// Context error paths — no runner set
// ---------------------------------------------------------------------------

func TestContextNoRunner(t *testing.T) {
	ctx := context.Background()

	t.Run("SelectExecContext", func(t *testing.T) {
		_, err := sqrl.Select("1").ExecContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("SelectQueryContext", func(t *testing.T) {
		_, err := sqrl.Select("1").QueryContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("SelectScanContext", func(t *testing.T) {
		var v int
		err := sqrl.Select("1").ScanContext(ctx, &v)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("InsertExecContext", func(t *testing.T) {
		_, err := sqrl.Insert("t").Columns("a").Values(1).ExecContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("InsertQueryContext", func(t *testing.T) {
		_, err := sqrl.Insert("t").Columns("a").Values(1).QueryContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("UpdateExecContext", func(t *testing.T) {
		_, err := sqrl.Update("t").Set("a", 1).ExecContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("UpdateQueryContext", func(t *testing.T) {
		_, err := sqrl.Update("t").Set("a", 1).QueryContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("DeleteExecContext", func(t *testing.T) {
		_, err := sqrl.Delete("t").ExecContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("DeleteQueryContext", func(t *testing.T) {
		_, err := sqrl.Delete("t").QueryContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}

// ---------------------------------------------------------------------------
// Context — Update/Delete ScanContext with RETURNING
// ---------------------------------------------------------------------------

func TestUpdateScanContext(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_upd_s", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_upd_s VALUES (1, 'old')")

	// Act
	var name string
	err := sb.Update("sq_ctx_upd_s").
		Set("name", "new").
		Where(sqrl.Eq{"id": 1}).
		Suffix("RETURNING name").
		ScanContext(ctx, &name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "new", name)
}

func TestDeleteScanContext(t *testing.T) {
	if isMySQL() {
		t.Skip("RETURNING not supported on MySQL")
	}

	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ctx_del_s", "(id INTEGER, name TEXT)")
	seedTable(t, "INSERT INTO sq_ctx_del_s VALUES (1, 'gone')")

	// Act
	var name string
	err := sb.Delete("sq_ctx_del_s").
		Where(sqrl.Eq{"id": 1}).
		Suffix("RETURNING name").
		ScanContext(ctx, &name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "gone", name)
}

// ---------------------------------------------------------------------------
// ExecContextWith / QueryContextWith / QueryRowContextWith
// ---------------------------------------------------------------------------

func TestExecContextWith(t *testing.T) {
	// Arrange
	ctx := context.Background()
	createTable(t, "sq_ectx_w", "(id INTEGER, name TEXT)")
	q := sqrl.Insert("sq_ectx_w").Columns("id", "name").Values(1, "ctx_exec_with").PlaceholderFormat(phf())

	// Act
	_, err := sqrl.ExecContextWith(ctx, db, q)

	// Assert
	require.NoError(t, err)

	var name string
	err = db.QueryRow("SELECT name FROM sq_ectx_w").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "ctx_exec_with", name)
}

func TestQueryContextWith(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf())

	// Act
	rows, err := sqrl.QueryContextWith(ctx, db, q)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	require.NoError(t, rows.Scan(&name))

	// Assert
	assert.Equal(t, "apple", name)
}

func TestQueryRowContextWith(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1}).PlaceholderFormat(phf())

	// Act
	var name string
	err := sqrl.QueryRowContextWith(ctx, sqrl.WrapStdSQLCtx(db), q).Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

// ---------------------------------------------------------------------------
// Context — QueryRowContext cancelled
// ---------------------------------------------------------------------------

func TestSelectQueryRowContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	var name string
	err := q.QueryRowContext(ctx).Scan(&name)

	// Assert
	assert.Error(t, err)
}
