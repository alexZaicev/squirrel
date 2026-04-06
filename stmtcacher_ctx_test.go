//go:build go1.8

package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStmtCacherPrepareContext(t *testing.T) {
	db := &DBStub{}
	sc := NewStmtCache(db)
	query := "SELECT 1"

	_, err := sc.PrepareContext(ctx, query)
	assert.NoError(t, err)
	assert.Equal(t, query, db.LastPrepareSQL)

	_, err = sc.PrepareContext(ctx, query)
	assert.NoError(t, err)
	assert.Equal(t, 1, db.PrepareCount, "expected 1 Prepare, got %d", db.PrepareCount)
}
