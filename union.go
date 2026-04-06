package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"

	"github.com/lann/builder"
)

// unionPart holds a set operation keyword and the corresponding Sqlizer.
// The first part in a union has an empty keyword.
type unionPart struct {
	keyword string
	sqlizer Sqlizer
}

type unionData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	Parts             []unionPart
	OrderByParts      []Sqlizer
	Limit             *uint64
	Offset            *uint64
	Suffixes          []Sqlizer
}

func (d *unionData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *unionData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *unionData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *unionData) ToSQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toSQLRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *unionData) toSQLRaw() (sqlStr string, args []any, err error) {
	if len(d.Parts) == 0 {
		err = fmt.Errorf("union statements must have at least one part")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, err = appendToSQL(d.Prefixes, sql, " ", args)
		if err != nil {
			return
		}

		sql.WriteString(" ")
	}

	for i, p := range d.Parts {
		if i > 0 {
			sql.WriteString(" ")
			sql.WriteString(p.keyword)
			sql.WriteString(" ")
		}

		var partSQL string
		var partArgs []any
		partSQL, partArgs, err = nestedToSQL(p.sqlizer)
		if err != nil {
			return
		}

		sql.WriteString(partSQL)
		args = append(args, partArgs...)
	}

	if len(d.OrderByParts) > 0 {
		sql.WriteString(" ORDER BY ")
		args, err = appendToSQL(d.OrderByParts, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if d.Limit != nil {
		sql.WriteString(" LIMIT ?")
		args = append(args, *d.Limit)
	}

	if d.Offset != nil {
		sql.WriteString(" OFFSET ?")
		args = append(args, *d.Offset)
	}

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

// UnionBuilder builds SQL UNION / UNION ALL / INTERSECT / EXCEPT statements.
type UnionBuilder builder.Builder

func init() {
	builder.Register(UnionBuilder{}, unionData{})
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b UnionBuilder) PlaceholderFormat(f PlaceholderFormat) UnionBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(UnionBuilder)
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b UnionBuilder) RunWith(runner BaseRunner) UnionBuilder {
	return setRunWith(b, runner).(UnionBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b UnionBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(unionData)
	return data.Exec()
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b UnionBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(unionData)
	return data.Query()
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b UnionBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(unionData)
	return data.QueryRow()
}

// Scan is a shortcut for QueryRow().Scan.
func (b UnionBuilder) Scan(dest ...any) error {
	return b.QueryRow().Scan(dest...)
}

// ToSQL builds the query into a SQL string and bound args.
func (b UnionBuilder) ToSQL() (string, []any, error) {
	data := builder.GetStruct(b).(unionData)
	return data.ToSQL()
}

func (b UnionBuilder) toSQLRaw() (string, []any, error) {
	data := builder.GetStruct(b).(unionData)
	return data.toSQLRaw()
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b UnionBuilder) MustSQL() (string, []any) {
	sql, args, err := b.ToSQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b UnionBuilder) Prefix(sql string, args ...any) UnionBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query.
func (b UnionBuilder) PrefixExpr(expr Sqlizer) UnionBuilder {
	return builder.Append(b, "Prefixes", expr).(UnionBuilder)
}

// Suffix adds an expression to the end of the query.
func (b UnionBuilder) Suffix(sql string, args ...any) UnionBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b UnionBuilder) SuffixExpr(expr Sqlizer) UnionBuilder {
	return builder.Append(b, "Suffixes", expr).(UnionBuilder)
}

// Union adds one or more SELECT queries joined by UNION.
func (b UnionBuilder) Union(selects ...SelectBuilder) UnionBuilder {
	for _, s := range selects {
		b = builder.Append(b, "Parts", unionPart{
			keyword: "UNION",
			sqlizer: s.PlaceholderFormat(Question),
		}).(UnionBuilder)
	}
	return b
}

// UnionAll adds one or more SELECT queries joined by UNION ALL.
func (b UnionBuilder) UnionAll(selects ...SelectBuilder) UnionBuilder {
	for _, s := range selects {
		b = builder.Append(b, "Parts", unionPart{
			keyword: "UNION ALL",
			sqlizer: s.PlaceholderFormat(Question),
		}).(UnionBuilder)
	}
	return b
}

// Intersect adds one or more SELECT queries joined by INTERSECT.
func (b UnionBuilder) Intersect(selects ...SelectBuilder) UnionBuilder {
	for _, s := range selects {
		b = builder.Append(b, "Parts", unionPart{
			keyword: "INTERSECT",
			sqlizer: s.PlaceholderFormat(Question),
		}).(UnionBuilder)
	}
	return b
}

// Except adds one or more SELECT queries joined by EXCEPT.
func (b UnionBuilder) Except(selects ...SelectBuilder) UnionBuilder {
	for _, s := range selects {
		b = builder.Append(b, "Parts", unionPart{
			keyword: "EXCEPT",
			sqlizer: s.PlaceholderFormat(Question),
		}).(UnionBuilder)
	}
	return b
}

// OrderByClause adds an ORDER BY clause to the combined result.
func (b UnionBuilder) OrderByClause(pred any, args ...any) UnionBuilder {
	return builder.Append(b, "OrderByParts", newPart(pred, args...)).(UnionBuilder)
}

// OrderBy adds ORDER BY expressions to the combined result.
func (b UnionBuilder) OrderBy(orderBys ...string) UnionBuilder {
	for _, orderBy := range orderBys {
		b = b.OrderByClause(orderBy)
	}
	return b
}

// Limit sets a LIMIT clause on the combined result.
func (b UnionBuilder) Limit(limit uint64) UnionBuilder {
	return builder.Set(b, "Limit", &limit).(UnionBuilder)
}

// RemoveLimit removes the LIMIT clause.
func (b UnionBuilder) RemoveLimit() UnionBuilder {
	return builder.Delete(b, "Limit").(UnionBuilder)
}

// Offset sets an OFFSET clause on the combined result.
func (b UnionBuilder) Offset(offset uint64) UnionBuilder {
	return builder.Set(b, "Offset", &offset).(UnionBuilder)
}

// RemoveOffset removes the OFFSET clause.
func (b UnionBuilder) RemoveOffset() UnionBuilder {
	return builder.Delete(b, "Offset").(UnionBuilder)
}

// newUnionBuilder creates a UnionBuilder with the first SelectBuilder as the
// initial part and a given set operation keyword for subsequent selects.
func newUnionBuilder(keyword string, selects []SelectBuilder) UnionBuilder {
	b := UnionBuilder(builder.EmptyBuilder).PlaceholderFormat(Question)
	for i, s := range selects {
		kw := keyword
		if i == 0 {
			kw = ""
		}
		b = builder.Append(b, "Parts", unionPart{
			keyword: kw,
			sqlizer: s.PlaceholderFormat(Question),
		}).(UnionBuilder)
	}
	return b
}
