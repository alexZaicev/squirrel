package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStmtCachePrepare(t *testing.T) {
	db := &DBStub{}
	sc := NewStmtCache(db)
	query := "SELECT 1"

	_, err := sc.Prepare(query)
	assert.NoError(t, err)
	assert.Equal(t, query, db.LastPrepareSQL)

	_, err = sc.Prepare(query)
	assert.NoError(t, err)
	assert.Equal(t, 1, db.PrepareCount, "expected 1 Prepare, got %d", db.PrepareCount)

	// clear statement cache
	assert.NoError(t, sc.Clear())

	// should prepare the query again
	_, err = sc.Prepare(query)
	assert.NoError(t, err)
	assert.Equal(t, 2, db.PrepareCount, "expected 2 Prepare, got %d", db.PrepareCount)
}
