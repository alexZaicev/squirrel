//go:build go1.8

package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCteBuilderContextRunners(t *testing.T) {
	db := &DBStub{}
	b := With("cte", Select("1 as n")).
		Statement(Select("n").From("cte")).
		RunWith(db)

	expectedSQL := "WITH cte AS (SELECT 1 as n) SELECT n FROM cte"

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

func TestCteBuilderContextNoRunner(t *testing.T) {
	b := With("cte", Select("1")).
		Statement(Select("*").From("cte"))

	_, err := b.ExecContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.QueryContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.ScanContext(ctx)
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestCteBuilderContextNoContextSupport(t *testing.T) {
	b := With("cte", Select("1")).
		Statement(Select("*").From("cte")).
		RunWith(&fakeBaseRunner{})

	_, err := b.ExecContext(ctx)
	assert.Equal(t, ErrNoContextSupport, err)

	_, err = b.QueryContext(ctx)
	assert.Equal(t, ErrNoContextSupport, err)

	err = b.ScanContext(ctx)
	assert.Equal(t, ErrRunnerNotQueryRunner, err)
}
