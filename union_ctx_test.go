//go:build go1.8

package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnionBuilderContextRunners(t *testing.T) {
	db := &DBStub{}
	q1 := Select("id").From("t1")
	q2 := Select("id").From("t2")
	b := Union(q1, q2).RunWith(db)

	expectedSQL := "SELECT id FROM t1 UNION SELECT id FROM t2"

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

func TestUnionBuilderContextNoRunner(t *testing.T) {
	q1 := Select("id").From("t1")
	b := Union(q1)

	_, err := b.ExecContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.QueryContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.ScanContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)
}
