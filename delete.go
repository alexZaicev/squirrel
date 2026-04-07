package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

type deleteData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	From              string
	UsingParts        []Sqlizer
	Joins             []Sqlizer
	WhereParts        []Sqlizer
	OrderBys          []string
	Limit             *uint64
	Offset            *uint64
	Returning         []string
	Suffixes          []Sqlizer
}

func (d *deleteData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *deleteData) ToSQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toSQLRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *deleteData) toSQLRaw() (sqlStr string, args []any, err error) {
	if len(d.From) == 0 {
		err = fmt.Errorf("delete statements must specify a From table")
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

	if len(d.Joins) > 0 {
		// MySQL multi-table DELETE syntax: DELETE t FROM t JOIN ...
		sql.WriteString("DELETE ")
		sql.WriteString(d.From)
		sql.WriteString(" FROM ")
		sql.WriteString(d.From)
		sql.WriteString(" ")
		args, err = appendToSQL(d.Joins, sql, " ", args)
		if err != nil {
			return
		}
	} else {
		sql.WriteString("DELETE FROM ")
		sql.WriteString(d.From)
	}

	if len(d.UsingParts) > 0 {
		sql.WriteString(" USING ")
		args, err = appendToSQL(d.UsingParts, sql, ", ", args)
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

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder builder.Builder

func init() {
	builder.Register(DeleteBuilder{}, deleteData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b DeleteBuilder) PlaceholderFormat(f PlaceholderFormat) DeleteBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(DeleteBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b DeleteBuilder) RunWith(runner BaseRunner) DeleteBuilder {
	return setRunWith(b, runner).(DeleteBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b DeleteBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(deleteData)
	return data.Exec()
}

// SQL methods

// ToSQL builds the query into a SQL string and bound args.
func (b DeleteBuilder) ToSQL() (string, []any, error) {
	data := builder.GetStruct(b).(deleteData)
	return data.ToSQL()
}

func (b DeleteBuilder) toSQLRaw() (string, []any, error) {
	data := builder.GetStruct(b).(deleteData)
	return data.toSQLRaw()
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b DeleteBuilder) MustSQL() (string, []any) {
	sql, args, err := b.ToSQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b DeleteBuilder) Prefix(sql string, args ...any) DeleteBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b DeleteBuilder) PrefixExpr(expr Sqlizer) DeleteBuilder {
	return builder.Append(b, "Prefixes", expr).(DeleteBuilder)
}

// From sets the table to be deleted from.
//
// WARNING: The table name is interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
// For dynamic table names from user input, use SafeFrom instead.
func (b DeleteBuilder) From(from string) DeleteBuilder {
	return builder.Set(b, "From", from).(DeleteBuilder)
}

// Using adds a USING clause to the query (PostgreSQL).
//
// PostgreSQL DELETE ... USING allows referencing additional tables in the
// WHERE clause:
//
//	Delete("t1").Using("t2").Where("t1.id = t2.t1_id AND t2.active = ?", false)
//	// DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id AND t2.active = ?
//
// WARNING: The table name is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) Using(tables ...string) DeleteBuilder {
	parts := make([]any, 0, len(tables))
	for _, t := range tables {
		parts = append(parts, newPart(t))
	}
	return builder.Extend(b, "UsingParts", parts).(DeleteBuilder)
}

// JoinClause adds a join clause to the query.
func (b DeleteBuilder) JoinClause(pred any, args ...any) DeleteBuilder {
	return builder.Append(b, "Joins", newPart(pred, args...)).(DeleteBuilder)
}

// Join adds a JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) Join(join string, rest ...any) DeleteBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) LeftJoin(join string, rest ...any) DeleteBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) RightJoin(join string, rest ...any) DeleteBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// InnerJoin adds an INNER JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) InnerJoin(join string, rest ...any) DeleteBuilder {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// CrossJoin adds a CROSS JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) CrossJoin(join string, rest ...any) DeleteBuilder {
	return b.JoinClause("CROSS JOIN "+join, rest...)
}

// FullJoin adds a FULL OUTER JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) FullJoin(join string, rest ...any) DeleteBuilder {
	return b.JoinClause("FULL OUTER JOIN "+join, rest...)
}

// JoinUsing adds a JOIN ... USING clause to the query.
func (b DeleteBuilder) JoinUsing(table string, columns ...string) DeleteBuilder {
	return b.JoinClause("JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// LeftJoinUsing adds a LEFT JOIN ... USING clause to the query.
func (b DeleteBuilder) LeftJoinUsing(table string, columns ...string) DeleteBuilder {
	return b.JoinClause("LEFT JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// RightJoinUsing adds a RIGHT JOIN ... USING clause to the query.
func (b DeleteBuilder) RightJoinUsing(table string, columns ...string) DeleteBuilder {
	return b.JoinClause("RIGHT JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// InnerJoinUsing adds an INNER JOIN ... USING clause to the query.
func (b DeleteBuilder) InnerJoinUsing(table string, columns ...string) DeleteBuilder {
	return b.JoinClause("INNER JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// CrossJoinUsing adds a CROSS JOIN ... USING clause to the query.
func (b DeleteBuilder) CrossJoinUsing(table string, columns ...string) DeleteBuilder {
	return b.JoinClause("CROSS JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// FullJoinUsing adds a FULL OUTER JOIN ... USING clause to the query.
func (b DeleteBuilder) FullJoinUsing(table string, columns ...string) DeleteBuilder {
	return b.JoinClause("FULL OUTER JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b DeleteBuilder) Where(pred any, args ...any) DeleteBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(DeleteBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
//
// WARNING: Order-by expressions are interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b DeleteBuilder) OrderBy(orderBys ...string) DeleteBuilder {
	return builder.Extend(b, "OrderBys", orderBys).(DeleteBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b DeleteBuilder) Limit(limit uint64) DeleteBuilder {
	return builder.Set(b, "Limit", &limit).(DeleteBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b DeleteBuilder) Offset(offset uint64) DeleteBuilder {
	return builder.Set(b, "Offset", &offset).(DeleteBuilder)
}

// Suffix adds an expression to the end of the query
func (b DeleteBuilder) Suffix(sql string, args ...any) DeleteBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b DeleteBuilder) SuffixExpr(expr Sqlizer) DeleteBuilder {
	return builder.Append(b, "Suffixes", expr).(DeleteBuilder)
}

// Returning adds RETURNING expressions to the query.
//
// Ex:
//
//	Delete("users").Where("id = ?", 1).
//		Returning("id", "name")
//	// DELETE FROM users WHERE id = ? RETURNING id, name
func (b DeleteBuilder) Returning(columns ...string) DeleteBuilder {
	return builder.Extend(b, "Returning", columns).(DeleteBuilder)
}

func (b DeleteBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(deleteData)
	return data.Query()
}

func (d *deleteData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

// Safe identifier methods
//
// The following methods accept Ident values produced by QuoteIdent or
// ValidateIdent, guaranteeing that the identifiers are safe for interpolation
// into SQL.

// SafeFrom sets the table to be deleted from using a safe Ident.
//
// Ex:
//
//	id, _ := sq.QuoteIdent(userInput)
//	sq.Delete("").SafeFrom(id).Where("id = ?", 1)
func (b DeleteBuilder) SafeFrom(from Ident) DeleteBuilder {
	return builder.Set(b, "From", from.String()).(DeleteBuilder)
}
