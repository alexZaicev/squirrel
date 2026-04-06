//go:build go1.8

package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilderContextRunners(t *testing.T) {
	db := &DBStub{}
	b := Update("test").Set("x", 1).RunWith(db)

	expectedSQL := "UPDATE test SET x = ?"

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

func TestUpdateBuilderContextNoRunner(t *testing.T) {
	b := Update("test").Set("x", 1)

	_, err := b.ExecContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.QueryContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.ScanContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)
}
