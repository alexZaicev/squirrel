//go:build go1.8

package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderContextRunners(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("x = ?", 1).RunWith(db)

	expectedSQL := "DELETE FROM test WHERE x = ?"

	_, err := b.ExecContext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastExecSQL)

	_, err = b.QueryContext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastQuerySQL)

	b.QueryRowContext(ctx)
	assert.Equal(t, expectedSQL, db.LastQueryRowSQL)

	err = b.ScanContext(ctx)
	assert.NoError(t, err)
}

func TestDeleteBuilderContextNoRunner(t *testing.T) {
	b := Delete("test").Where("x != ?", 0).Suffix("RETURNING x")

	_, err := b.ExecContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.QueryContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.ScanContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)
}
