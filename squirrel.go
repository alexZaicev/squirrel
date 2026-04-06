// Package squirrel provides a fluent SQL generator.
//
// See https://github.com/alexZaicev/squirrel for examples.
package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

// Sqlizer is the interface that wraps the ToSQL method.
//
// ToSQL returns a SQL representation of the Sqlizer, along with a slice of args
// as passed to e.g. database/sql.Exec. It can also return an error.
type Sqlizer interface {
	ToSQL() (string, []any, error)
}

// rawSqlizer is expected to do what Sqlizer does, but without finalizing placeholders.
// This is useful for nested queries.
type rawSqlizer interface {
	toSQLRaw() (string, []any, error)
}

// Execer is the interface that wraps the Exec method.
//
// Exec executes the given query as implemented by database/sql.Exec.
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// Queryer is the interface that wraps the Query method.
//
// Query executes the given query as implemented by database/sql.Query.
type Queryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// QueryRower is the interface that wraps the QueryRow method.
//
// QueryRow executes the given query as implemented by database/sql.QueryRow.
type QueryRower interface {
	QueryRow(query string, args ...any) RowScanner
}

// BaseRunner groups the Execer and Queryer interfaces.
type BaseRunner interface {
	Execer
	Queryer
}

// Runner groups the Execer, Queryer, and QueryRower interfaces.
type Runner interface {
	Execer
	Queryer
	QueryRower
}

// WrapStdSQL wraps a type implementing the standard SQL interface with methods that
// squirrel expects.
func WrapStdSQL(stdSQL StdSQL) Runner {
	return &stdsqlRunner{stdSQL}
}

// StdSQL encompasses the standard methods of the *sql.DB type, and other types that
// wrap these methods.
type StdSQL interface {
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
	Exec(string, ...any) (sql.Result, error)
}

type stdsqlRunner struct {
	StdSQL
}

func (r *stdsqlRunner) QueryRow(query string, args ...any) RowScanner {
	return r.StdSQL.QueryRow(query, args...)
}

func setRunWith(b any, runner BaseRunner) any {
	switch r := runner.(type) {
	case StdSQLCtx:
		runner = WrapStdSQLCtx(r)
	case StdSQL:
		runner = WrapStdSQL(r)
	}
	return builder.Set(b, "RunWith", runner)
}

// ErrRunnerNotSet is returned by methods that need a Runner if it isn't set.
var ErrRunnerNotSet = fmt.Errorf("cannot run; no Runner set (RunWith)")

// ErrRunnerNotQueryRunner is returned by QueryRow if the RunWith value doesn't implement QueryRower.
var ErrRunnerNotQueryRunner = fmt.Errorf("cannot QueryRow; Runner is not a QueryRower")

// ExecWith Execs the SQL returned by s with db.
func ExecWith(db Execer, s Sqlizer) (res sql.Result, err error) {
	query, args, err := s.ToSQL()
	if err != nil {
		return
	}
	return db.Exec(query, args...)
}

// QueryWith Querys the SQL returned by s with db.
func QueryWith(db Queryer, s Sqlizer) (rows *sql.Rows, err error) {
	query, args, err := s.ToSQL()
	if err != nil {
		return
	}
	return db.Query(query, args...)
}

// QueryRowWith QueryRows the SQL returned by s with db.
func QueryRowWith(db QueryRower, s Sqlizer) RowScanner {
	query, args, err := s.ToSQL()
	return &Row{RowScanner: db.QueryRow(query, args...), err: err}
}

// DebugSqlizer calls ToSQL on s and shows the approximate SQL to be executed
//
// If ToSQL returns an error, the result of this method will look like:
// "[ToSQL error: %s]" or "[DebugSqlizer error: %s]"
//
// IMPORTANT: As its name suggests, this function should only be used for
// debugging. While the string result *might* be valid SQL, this function does
// not try very hard to ensure it. Additionally, executing the output of this
// function with any untrusted user input is certainly insecure.
func DebugSqlizer(s Sqlizer) string {
	sql, args, err := s.ToSQL()
	if err != nil {
		return fmt.Sprintf("[ToSQL error: %s]", err)
	}

	var placeholder string
	downCast, ok := s.(placeholderDebugger)
	if !ok {
		placeholder = "?"
	} else {
		placeholder = downCast.debugPlaceholder()
	}
	// TODO: dedupe this with placeholder.go
	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, placeholder)
		if p == -1 {
			break
		}
		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			if i+1 > len(args) {
				return fmt.Sprintf(
					"[DebugSqlizer error: too many placeholders in %#v for %d args]",
					sql, len(args))
			}
			buf.WriteString(sql[:p])
			fmt.Fprintf(buf, "'%v'", args[i])
			// advance our sql string "cursor" beyond the arg we placed
			sql = sql[p+1:]
			i++
		}
	}
	if i < len(args) {
		return fmt.Sprintf(
			"[DebugSqlizer error: not enough placeholders in %#v for %d args]",
			sql, len(args))
	}
	// "append" any remaning sql that won't need interpolating
	buf.WriteString(sql)
	return buf.String()
}
