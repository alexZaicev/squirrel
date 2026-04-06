package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/lann/builder"
)

type updateData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	Table             string
	SetClauses        []setClause
	From              Sqlizer
	WhereParts        []Sqlizer
	OrderBys          []string
	Limit             *uint64
	Offset            *uint64
	Returning         []string
	Suffixes          []Sqlizer
}

type setClause struct {
	column string
	value  any
}

func (d *updateData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *updateData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *updateData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *updateData) ToSQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toSQLRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *updateData) toSQLRaw() (sqlStr string, args []any, err error) {
	if len(d.Table) == 0 {
		err = fmt.Errorf("update statements must specify a table")
		return
	}
	if len(d.SetClauses) == 0 {
		err = fmt.Errorf("update statements must have at least one Set clause")
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

	sql.WriteString("UPDATE ")
	sql.WriteString(d.Table)

	sql.WriteString(" SET ")
	setSqls := make([]string, len(d.SetClauses))
	for i, setClause := range d.SetClauses {
		var valSQL string
		if vs, ok := setClause.value.(Sqlizer); ok {
			vsql, vargs, err := vs.ToSQL()
			if err != nil {
				return "", nil, err
			}
			if _, ok := vs.(SelectBuilder); ok {
				valSQL = fmt.Sprintf("(%s)", vsql)
			} else {
				valSQL = vsql
			}
			args = append(args, vargs...)
		} else {
			valSQL = "?"
			args = append(args, setClause.value)
		}
		setSqls[i] = fmt.Sprintf("%s = %s", setClause.column, valSQL)
	}
	sql.WriteString(strings.Join(setSqls, ", "))

	if d.From != nil {
		sql.WriteString(" FROM ")
		args, err = appendToSQL([]Sqlizer{d.From}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSQL(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.OrderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(d.OrderBys, ", "))
	}

	if d.Limit != nil {
		sql.WriteString(" LIMIT ?")
		args = append(args, *d.Limit)
	}

	if d.Offset != nil {
		sql.WriteString(" OFFSET ?")
		args = append(args, *d.Offset)
	}

	if len(d.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		sql.WriteString(strings.Join(d.Returning, ", "))
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

// Builder

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder builder.Builder

func init() {
	builder.Register(UpdateBuilder{}, updateData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) UpdateBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(UpdateBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b UpdateBuilder) RunWith(runner BaseRunner) UpdateBuilder {
	return setRunWith(b, runner).(UpdateBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b UpdateBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(updateData)
	return data.Exec()
}

func (b UpdateBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(updateData)
	return data.Query()
}

func (b UpdateBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(updateData)
	return data.QueryRow()
}

func (b UpdateBuilder) Scan(dest ...any) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSQL builds the query into a SQL string and bound args.
func (b UpdateBuilder) ToSQL() (string, []any, error) {
	data := builder.GetStruct(b).(updateData)
	return data.ToSQL()
}

func (b UpdateBuilder) toSQLRaw() (string, []any, error) {
	data := builder.GetStruct(b).(updateData)
	return data.toSQLRaw()
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b UpdateBuilder) MustSQL() (string, []any) {
	sql, args, err := b.ToSQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b UpdateBuilder) Prefix(sql string, args ...any) UpdateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b UpdateBuilder) PrefixExpr(expr Sqlizer) UpdateBuilder {
	return builder.Append(b, "Prefixes", expr).(UpdateBuilder)
}

// Table sets the table to be updated.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	return builder.Set(b, "Table", table).(UpdateBuilder)
}

// Set adds SET clauses to the query.
func (b UpdateBuilder) Set(column string, value any) UpdateBuilder {
	return builder.Append(b, "SetClauses", setClause{column: column, value: value}).(UpdateBuilder)
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b UpdateBuilder) SetMap(clauses map[string]any) UpdateBuilder {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		val := clauses[key]
		b = b.Set(key, val)
	}
	return b
}

// From adds FROM clause to the query
// FROM is valid construct in postgresql only.
func (b UpdateBuilder) From(from string) UpdateBuilder {
	return builder.Set(b, "From", newPart(from)).(UpdateBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b UpdateBuilder) FromSelect(from SelectBuilder, alias string) UpdateBuilder {
	// Prevent misnumbered parameters in nested selects (#183).
	from = from.PlaceholderFormat(Question)
	return builder.Set(b, "From", Alias(from, alias)).(UpdateBuilder)
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b UpdateBuilder) Where(pred any, args ...any) UpdateBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(UpdateBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
func (b UpdateBuilder) OrderBy(orderBys ...string) UpdateBuilder {
	return builder.Extend(b, "OrderBys", orderBys).(UpdateBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b UpdateBuilder) Limit(limit uint64) UpdateBuilder {
	return builder.Set(b, "Limit", &limit).(UpdateBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b UpdateBuilder) Offset(offset uint64) UpdateBuilder {
	return builder.Set(b, "Offset", &offset).(UpdateBuilder)
}

// Suffix adds an expression to the end of the query
func (b UpdateBuilder) Suffix(sql string, args ...any) UpdateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b UpdateBuilder) SuffixExpr(expr Sqlizer) UpdateBuilder {
	return builder.Append(b, "Suffixes", expr).(UpdateBuilder)
}

// Returning adds RETURNING expressions to the query.
//
// Ex:
//
//	Update("users").Set("name", "John").Where("id = ?", 1).
//		Returning("id", "name")
//	// UPDATE users SET name = ? WHERE id = ? RETURNING id, name
func (b UpdateBuilder) Returning(columns ...string) UpdateBuilder {
	return builder.Extend(b, "Returning", columns).(UpdateBuilder)
}
