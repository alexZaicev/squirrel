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
	Joins             []Sqlizer
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

	if len(d.Joins) > 0 {
		sql.WriteString(" ")
		args, err = appendToSQL(d.Joins, sql, " ", args)
		if err != nil {
			return
		}
	}

	sql.WriteString(" SET ")
	setSqls := make([]string, len(d.SetClauses))
	for i, setClause := range d.SetClauses {
		var valSQL string
		if vs, ok := setClause.value.(Sqlizer); ok {
			vsql, vargs, err := nestedToSQL(vs)
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
		args, err = appendPrefixedToSQL(d.WhereParts, sql, " WHERE ", args)
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
//
// WARNING: The table name is interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
// For dynamic table names from user input, use SafeTable instead.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	return builder.Set(b, "Table", table).(UpdateBuilder)
}

// Set adds SET clauses to the query.
//
// WARNING: The column name is interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input as the column argument.
// For dynamic column names from user input, use SafeSet instead.
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
//
// WARNING: The table name is interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
func (b UpdateBuilder) From(from string) UpdateBuilder {
	return builder.Set(b, "From", newPart(from)).(UpdateBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b UpdateBuilder) FromSelect(from SelectBuilder, alias string) UpdateBuilder {
	// Prevent misnumbered parameters in nested selects (#183).
	from = from.PlaceholderFormat(Question)
	return builder.Set(b, "From", Alias(from, alias)).(UpdateBuilder)
}

// JoinClause adds a join clause to the query.
func (b UpdateBuilder) JoinClause(pred any, args ...any) UpdateBuilder {
	return builder.Append(b, "Joins", newPart(pred, args...)).(UpdateBuilder)
}

// Join adds a JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b UpdateBuilder) Join(join string, rest ...any) UpdateBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b UpdateBuilder) LeftJoin(join string, rest ...any) UpdateBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b UpdateBuilder) RightJoin(join string, rest ...any) UpdateBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// InnerJoin adds an INNER JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b UpdateBuilder) InnerJoin(join string, rest ...any) UpdateBuilder {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// CrossJoin adds a CROSS JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b UpdateBuilder) CrossJoin(join string, rest ...any) UpdateBuilder {
	return b.JoinClause("CROSS JOIN "+join, rest...)
}

// FullJoin adds a FULL OUTER JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b UpdateBuilder) FullJoin(join string, rest ...any) UpdateBuilder {
	return b.JoinClause("FULL OUTER JOIN "+join, rest...)
}

// JoinUsing adds a JOIN ... USING clause to the query.
func (b UpdateBuilder) JoinUsing(table string, columns ...string) UpdateBuilder {
	return b.JoinClause("JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// LeftJoinUsing adds a LEFT JOIN ... USING clause to the query.
func (b UpdateBuilder) LeftJoinUsing(table string, columns ...string) UpdateBuilder {
	return b.JoinClause("LEFT JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// RightJoinUsing adds a RIGHT JOIN ... USING clause to the query.
func (b UpdateBuilder) RightJoinUsing(table string, columns ...string) UpdateBuilder {
	return b.JoinClause("RIGHT JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// InnerJoinUsing adds an INNER JOIN ... USING clause to the query.
func (b UpdateBuilder) InnerJoinUsing(table string, columns ...string) UpdateBuilder {
	return b.JoinClause("INNER JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// CrossJoinUsing adds a CROSS JOIN ... USING clause to the query.
func (b UpdateBuilder) CrossJoinUsing(table string, columns ...string) UpdateBuilder {
	return b.JoinClause("CROSS JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// FullJoinUsing adds a FULL OUTER JOIN ... USING clause to the query.
func (b UpdateBuilder) FullJoinUsing(table string, columns ...string) UpdateBuilder {
	return b.JoinClause("FULL OUTER JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b UpdateBuilder) Where(pred any, args ...any) UpdateBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(UpdateBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
//
// WARNING: Order-by expressions are interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
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

// Safe identifier methods
//
// The following methods accept Ident values produced by QuoteIdent or
// ValidateIdent, guaranteeing that the identifiers are safe for interpolation
// into SQL.

// SafeTable sets the table to be updated using a safe Ident.
//
// Ex:
//
//	id, _ := sq.QuoteIdent(userInput)
//	sq.Update("").SafeTable(id).Set("name", "John").Where("id = ?", 1)
func (b UpdateBuilder) SafeTable(table Ident) UpdateBuilder {
	return builder.Set(b, "Table", table.String()).(UpdateBuilder)
}

// SafeSet adds a SET clause with a safe Ident column name.
//
// Ex:
//
//	col, _ := sq.QuoteIdent(userInput)
//	sq.Update("users").SafeSet(col, "value").Where("id = ?", 1)
func (b UpdateBuilder) SafeSet(column Ident, value any) UpdateBuilder {
	return builder.Append(b, "SetClauses", setClause{column: column.String(), value: value}).(UpdateBuilder)
}
