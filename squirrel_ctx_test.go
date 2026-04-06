//go:build go1.8

package squirrel

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (s *DBStub) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	s.LastPrepareSQL = query
	s.PrepareCount++
	return nil, nil
}

func (s *DBStub) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	s.LastExecSQL = query
	s.LastExecArgs = args
	return nil, nil
}

func (s *DBStub) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	s.LastQuerySQL = query
	s.LastQueryArgs = args
	return nil, nil
}

func (s *DBStub) QueryRowContext(ctx context.Context, query string, args ...any) RowScanner {
	s.LastQueryRowSQL = query
	s.LastQueryRowArgs = args
	return &Row{RowScanner: &RowStub{}}
}

var ctx = context.Background()

func TestExecContextWith(t *testing.T) {
	db := &DBStub{}
	_, err := ExecContextWith(ctx, db, sqlizer)
	assert.NoError(t, err)
	assert.Equal(t, sqlStr, db.LastExecSQL)
}

func TestQueryContextWith(t *testing.T) {
	db := &DBStub{}
	_, err := QueryContextWith(ctx, db, sqlizer)
	assert.NoError(t, err)
	assert.Equal(t, sqlStr, db.LastQuerySQL)
}

func TestQueryRowContextWith(t *testing.T) {
	db := &DBStub{}
	QueryRowContextWith(ctx, db, sqlizer)
	assert.Equal(t, sqlStr, db.LastQueryRowSQL)
}
