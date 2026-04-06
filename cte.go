package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

// ctePart represents a single CTE definition within a WITH clause.
type ctePart struct {
	Name    string
	Columns []string
	As      Sqlizer
}

type cteData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Recursive         bool
	Ctes              []ctePart
	Statement         Sqlizer
	Suffixes          []Sqlizer
}

func (d *cteData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *cteData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *cteData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *cteData) ToSQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toSQLRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *cteData) toSQLRaw() (sqlStr string, args []any, err error) {
	if len(d.Ctes) == 0 {
		err = fmt.Errorf("CTE statements must have at least one CTE definition")
		return
	}

	if d.Statement == nil {
		err = fmt.Errorf("CTE statements must have a main statement (use .Statement())")
		return
	}

	sql := &bytes.Buffer{}

	if d.Recursive {
		sql.WriteString("WITH RECURSIVE ")
	} else {
		sql.WriteString("WITH ")
	}

	for i, cte := range d.Ctes {
		if i > 0 {
			sql.WriteString(", ")
		}

		sql.WriteString(cte.Name)

		if len(cte.Columns) > 0 {
			sql.WriteString(" (")
			sql.WriteString(strings.Join(cte.Columns, ", "))
			sql.WriteString(")")
		}

		sql.WriteString(" AS (")

		var cteSQL string
		var cteArgs []any
		cteSQL, cteArgs, err = nestedToSQL(cte.As)
		if err != nil {
			return
		}

		sql.WriteString(cteSQL)
		args = append(args, cteArgs...)
		sql.WriteString(")")
	}

	sql.WriteString(" ")

	var stmtSQL string
	var stmtArgs []any
	stmtSQL, stmtArgs, err = nestedToSQL(d.Statement)
	if err != nil {
		return
	}

	sql.WriteString(stmtSQL)
	args = append(args, stmtArgs...)

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")

		args, err = appendToSQL(d.Suffixes, sql, " ", args)
		if err != nil {
			return
		}
	}

	sqlStr = sql.String()
	return
}

// CteBuilder builds SQL WITH (Common Table Expression) statements.
type CteBuilder builder.Builder

func init() {
	builder.Register(CteBuilder{}, cteData{})
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b CteBuilder) PlaceholderFormat(f PlaceholderFormat) CteBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(CteBuilder)
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
// For most cases runner will be a database connection.
func (b CteBuilder) RunWith(runner BaseRunner) CteBuilder {
	return setRunWith(b, runner).(CteBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b CteBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(cteData)
	return data.Exec()
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b CteBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(cteData)
	return data.Query()
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b CteBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(cteData)
	return data.QueryRow()
}

// Scan is a shortcut for QueryRow().Scan.
func (b CteBuilder) Scan(dest ...any) error {
	return b.QueryRow().Scan(dest...)
}

// ToSQL builds the query into a SQL string and bound args.
func (b CteBuilder) ToSQL() (string, []any, error) {
	data := builder.GetStruct(b).(cteData)
	return data.ToSQL()
}

func (b CteBuilder) toSQLRaw() (string, []any, error) {
	data := builder.GetStruct(b).(cteData)
	return data.toSQLRaw()
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b CteBuilder) MustSQL() (string, []any) {
	sql, args, err := b.ToSQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// With adds a CTE definition to the builder.
//
// Ex:
//
//	With("cte", Select("id").From("t1")).
//		With("cte2", Select("name").From("t2")).
//		Statement(Select("*").From("cte").Join("cte2 ON cte.id = cte2.id"))
func (b CteBuilder) With(name string, as Sqlizer) CteBuilder {
	return builder.Append(b, "Ctes", ctePart{Name: name, As: as}).(CteBuilder)
}

// WithRecursive adds a CTE definition and marks the WITH clause as RECURSIVE.
// In standard SQL, RECURSIVE is a clause-level keyword — if any CTE is
// recursive, the entire WITH clause uses WITH RECURSIVE.
//
// Ex:
//
//	WithRecursive("tree",
//		Union(
//			Select("id", "parent_id").From("categories").Where(Eq{"parent_id": nil}),
//			Select("c.id", "c.parent_id").From("categories c").Join("tree t ON c.parent_id = t.id"),
//		),
//	).Statement(Select("*").From("tree"))
func (b CteBuilder) WithRecursive(name string, as Sqlizer) CteBuilder {
	b = builder.Set(b, "Recursive", true).(CteBuilder)
	return builder.Append(b, "Ctes", ctePart{Name: name, As: as}).(CteBuilder)
}

// WithColumns adds a CTE definition with explicit column names.
//
// Ex:
//
//	WithColumns("cte", []string{"x", "y"}, Select("a", "b").From("t1")).
//		Statement(Select("x", "y").From("cte"))
//	// WITH cte (x, y) AS (SELECT a, b FROM t1) SELECT x, y FROM cte
func (b CteBuilder) WithColumns(name string, columns []string, as Sqlizer) CteBuilder {
	return builder.Append(b, "Ctes", ctePart{Name: name, Columns: columns, As: as}).(CteBuilder)
}

// WithRecursiveColumns adds a CTE definition with explicit column names and
// marks the WITH clause as RECURSIVE.
//
// Ex:
//
//	WithRecursiveColumns("cnt", []string{"x"},
//		Union(Select("1"), Select("x + 1").From("cnt").Where("x < ?", 100)),
//	).Statement(Select("x").From("cnt"))
//	// WITH RECURSIVE cnt (x) AS (SELECT 1 UNION SELECT x + 1 FROM cnt WHERE x < ?) SELECT x FROM cnt
func (b CteBuilder) WithRecursiveColumns(name string, columns []string, as Sqlizer) CteBuilder {
	b = builder.Set(b, "Recursive", true).(CteBuilder)
	return builder.Append(b, "Ctes", ctePart{Name: name, Columns: columns, As: as}).(CteBuilder)
}

// Statement sets the main SQL statement that follows the WITH clause.
// The statement can be any Sqlizer (SelectBuilder, InsertBuilder, UpdateBuilder,
// DeleteBuilder, UnionBuilder, etc.).
//
// Ex:
//
//	With("active", Select("id").From("users").Where(Eq{"active": true})).
//		Statement(Select("*").From("active"))
func (b CteBuilder) Statement(stmt Sqlizer) CteBuilder {
	return builder.Set(b, "Statement", stmt).(CteBuilder)
}

// Suffix adds an expression to the end of the query.
func (b CteBuilder) Suffix(sql string, args ...any) CteBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b CteBuilder) SuffixExpr(expr Sqlizer) CteBuilder {
	return builder.Append(b, "Suffixes", expr).(CteBuilder)
}
