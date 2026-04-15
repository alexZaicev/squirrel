package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

type selectData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	Distinct          bool
	DistinctOn        []string
	Options           []string
	Columns           []Sqlizer
	From              Sqlizer
	Joins             []Sqlizer
	WhereParts        []Sqlizer
	GroupBys          []string
	HavingParts       []Sqlizer
	OrderByParts      []Sqlizer
	Limit             *uint64
	Offset            *uint64
	Suffixes          []Sqlizer
}

func (d *selectData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *selectData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *selectData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *selectData) ToSQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toSQLRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *selectData) toSQLRaw() (sqlStr string, args []any, err error) {
	if len(d.Columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
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

	sql.WriteString("SELECT ")

	if len(d.DistinctOn) > 0 {
		sql.WriteString("DISTINCT ON (")
		sql.WriteString(strings.Join(d.DistinctOn, ", "))
		sql.WriteString(") ")
	} else if d.Distinct {
		sql.WriteString("DISTINCT ")
	}

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	if len(d.Columns) > 0 {
		args, err = appendToSQL(d.Columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if d.From != nil {
		sql.WriteString(" FROM ")
		args, err = appendToSQL([]Sqlizer{d.From}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(d.Joins) > 0 {
		sql.WriteString(" ")
		args, err = appendToSQL(d.Joins, sql, " ", args)
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

	if len(d.GroupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(d.GroupBys, ", "))
	}

	if len(d.HavingParts) > 0 {
		args, err = appendPrefixedToSQL(d.HavingParts, sql, " HAVING ", args)
		if err != nil {
			return
		}
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

// Builder

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder builder.Builder

func init() {
	builder.Register(SelectBuilder{}, selectData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b SelectBuilder) PlaceholderFormat(f PlaceholderFormat) SelectBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(SelectBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
// For most cases runner will be a database connection.
//
// Internally we use this to mock out the database connection for testing.
func (b SelectBuilder) RunWith(runner BaseRunner) SelectBuilder {
	return setRunWith(b, runner).(SelectBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b SelectBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(selectData)
	return data.Exec()
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b SelectBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(selectData)
	return data.Query()
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b SelectBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(selectData)
	return data.QueryRow()
}

// Scan is a shortcut for QueryRow().Scan.
func (b SelectBuilder) Scan(dest ...any) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSQL builds the query into a SQL string and bound args.
func (b SelectBuilder) ToSQL() (string, []any, error) {
	data := builder.GetStruct(b).(selectData)
	return data.ToSQL()
}

func (b SelectBuilder) toSQLRaw() (string, []any, error) {
	data := builder.GetStruct(b).(selectData)
	return data.toSQLRaw()
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b SelectBuilder) MustSQL() (string, []any) {
	sql, args, err := b.ToSQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b SelectBuilder) Prefix(sql string, args ...any) SelectBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b SelectBuilder) PrefixExpr(expr Sqlizer) SelectBuilder {
	return builder.Append(b, "Prefixes", expr).(SelectBuilder)
}

// Distinct adds a DISTINCT clause to the query. Multiple calls are
// idempotent — calling Distinct() more than once still produces a single
// DISTINCT keyword in the generated SQL.
func (b SelectBuilder) Distinct() SelectBuilder {
	return builder.Set(b, "Distinct", true).(SelectBuilder)
}

// DistinctOn adds a DISTINCT ON (columns...) clause to the query.
// This is a PostgreSQL-specific feature that eliminates rows where all the
// specified columns are equal, keeping only the first row of each group
// (according to the ORDER BY clause).
//
// When DistinctOn is set, it takes precedence over Distinct() — the generated
// SQL will use DISTINCT ON (...) rather than plain DISTINCT.
//
// Multiple calls accumulate columns.
//
// WARNING: Column names are interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
// For dynamic column names from user input, use SafeDistinctOn instead.
//
// Ex:
//
//	sq.Select("location", "time", "report").
//		From("weather_reports").
//		DistinctOn("location").
//		OrderBy("location", "time DESC")
//	// SELECT DISTINCT ON (location) location, time, report
//	//   FROM weather_reports ORDER BY location, time DESC
func (b SelectBuilder) DistinctOn(columns ...string) SelectBuilder {
	return builder.Extend(b, "DistinctOn", columns).(SelectBuilder)
}

// Options adds select option to the query.
//
// WARNING: Options are interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
func (b SelectBuilder) Options(options ...string) SelectBuilder {
	return builder.Extend(b, "Options", options).(SelectBuilder)
}

// Columns adds result columns to the query.
//
// WARNING: Column names are interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
// For dynamic column names from user input, use SafeColumns instead.
func (b SelectBuilder) Columns(columns ...string) SelectBuilder {
	parts := make([]any, 0, len(columns))
	for _, str := range columns {
		parts = append(parts, newPart(str))
	}
	return builder.Extend(b, "Columns", parts).(SelectBuilder)
}

// RemoveColumns remove all columns from query.
// Must add a new column with Column or Columns methods, otherwise
// return a error.
func (b SelectBuilder) RemoveColumns() SelectBuilder {
	return builder.Delete(b, "Columns").(SelectBuilder)
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//
//	Column("IF(col IN ("+squirrel.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b SelectBuilder) Column(column any, args ...any) SelectBuilder {
	return builder.Append(b, "Columns", newPart(column, args...)).(SelectBuilder)
}

// From sets the FROM clause of the query.
//
// WARNING: The table name is interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
// For dynamic table names from user input, use SafeFrom instead.
func (b SelectBuilder) From(from string) SelectBuilder {
	return builder.Set(b, "From", newPart(from)).(SelectBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilder) FromSelect(from SelectBuilder, alias string) SelectBuilder {
	// Prevent misnumbered parameters in nested selects (#183).
	from = from.PlaceholderFormat(Question)
	return builder.Set(b, "From", Alias(from, alias)).(SelectBuilder)
}

// JoinClause adds a join clause to the query.
func (b SelectBuilder) JoinClause(pred any, args ...any) SelectBuilder {
	return builder.Append(b, "Joins", newPart(pred, args...)).(SelectBuilder)
}

// Join adds a JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b SelectBuilder) Join(join string, rest ...any) SelectBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b SelectBuilder) LeftJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b SelectBuilder) RightJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// InnerJoin adds a INNER JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b SelectBuilder) InnerJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// CrossJoin adds a CROSS JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b SelectBuilder) CrossJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("CROSS JOIN "+join, rest...)
}

// FullJoin adds a FULL OUTER JOIN clause to the query.
//
// WARNING: The join clause is interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
func (b SelectBuilder) FullJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("FULL OUTER JOIN "+join, rest...)
}

// JoinUsing adds a JOIN ... USING clause to the query.
// It is a convenience for the common case where the join condition is a simple
// column equality: JOIN table USING (col1, col2, ...).
func (b SelectBuilder) JoinUsing(table string, columns ...string) SelectBuilder {
	return b.JoinClause("JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// LeftJoinUsing adds a LEFT JOIN ... USING clause to the query.
func (b SelectBuilder) LeftJoinUsing(table string, columns ...string) SelectBuilder {
	return b.JoinClause("LEFT JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// RightJoinUsing adds a RIGHT JOIN ... USING clause to the query.
func (b SelectBuilder) RightJoinUsing(table string, columns ...string) SelectBuilder {
	return b.JoinClause("RIGHT JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// InnerJoinUsing adds an INNER JOIN ... USING clause to the query.
func (b SelectBuilder) InnerJoinUsing(table string, columns ...string) SelectBuilder {
	return b.JoinClause("INNER JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// CrossJoinUsing adds a CROSS JOIN ... USING clause to the query.
func (b SelectBuilder) CrossJoinUsing(table string, columns ...string) SelectBuilder {
	return b.JoinClause("CROSS JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// FullJoinUsing adds a FULL OUTER JOIN ... USING clause to the query.
func (b SelectBuilder) FullJoinUsing(table string, columns ...string) SelectBuilder {
	return b.JoinClause("FULL OUTER JOIN " + table + " USING (" + strings.Join(columns, ", ") + ")")
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]any OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> IN
// (?,?,...)", with one placeholder for each item in the value. These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b SelectBuilder) Where(pred any, args ...any) SelectBuilder {
	if pred == nil || pred == "" {
		return b
	}
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(SelectBuilder)
}

// GroupBy adds GROUP BY expressions to the query.
//
// WARNING: Group-by expressions are interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
// For dynamic group-by columns from user input, use SafeGroupBy instead.
func (b SelectBuilder) GroupBy(groupBys ...string) SelectBuilder {
	return builder.Extend(b, "GroupBys", groupBys).(SelectBuilder)
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b SelectBuilder) Having(pred any, rest ...any) SelectBuilder {
	return builder.Append(b, "HavingParts", newWherePart(pred, rest...)).(SelectBuilder)
}

// OrderByClause adds ORDER BY clause to the query.
func (b SelectBuilder) OrderByClause(pred any, args ...any) SelectBuilder {
	return builder.Append(b, "OrderByParts", newPart(pred, args...)).(SelectBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
//
// WARNING: Order-by expressions are interpolated directly into the SQL string.
// NEVER pass unsanitized user input to this method.
// For dynamic order-by columns from user input, use SafeOrderBy instead.
func (b SelectBuilder) OrderBy(orderBys ...string) SelectBuilder {
	for _, orderBy := range orderBys {
		b = b.OrderByClause(orderBy)
	}

	return b
}

// Limit sets a LIMIT clause on the query.
func (b SelectBuilder) Limit(limit uint64) SelectBuilder {
	return builder.Set(b, "Limit", &limit).(SelectBuilder)
}

// Limit ALL allows to access all records with limit
func (b SelectBuilder) RemoveLimit() SelectBuilder {
	return builder.Delete(b, "Limit").(SelectBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b SelectBuilder) Offset(offset uint64) SelectBuilder {
	return builder.Set(b, "Offset", &offset).(SelectBuilder)
}

// RemoveOffset removes OFFSET clause.
func (b SelectBuilder) RemoveOffset() SelectBuilder {
	return builder.Delete(b, "Offset").(SelectBuilder)
}

// Suffix adds an expression to the end of the query
func (b SelectBuilder) Suffix(sql string, args ...any) SelectBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b SelectBuilder) SuffixExpr(expr Sqlizer) SelectBuilder {
	return builder.Append(b, "Suffixes", expr).(SelectBuilder)
}

// Safe identifier methods
//
// The following methods accept Ident values produced by QuoteIdent or
// ValidateIdent, guaranteeing that the identifiers are safe for interpolation
// into SQL. Use them when identifiers come from user input or any other
// untrusted source.

// SafeFrom sets the FROM clause of the query using a safe Ident.
//
// Ex:
//
//	id, _ := sq.QuoteIdent(userInput)
//	sq.Select("*").SafeFrom(id)
func (b SelectBuilder) SafeFrom(from Ident) SelectBuilder {
	return builder.Set(b, "From", newPart(from.String())).(SelectBuilder)
}

// SafeDistinctOn adds a DISTINCT ON (columns...) clause to the query using
// safe Ident values. This is the safe alternative to DistinctOn for cases
// where column names come from user input or other untrusted sources.
//
// Multiple calls accumulate columns.
//
// Ex:
//
//	col, _ := sq.QuoteIdent(userInput)
//	sq.Select("*").SafeDistinctOn(col).From("weather_reports")
func (b SelectBuilder) SafeDistinctOn(columns ...Ident) SelectBuilder {
	return builder.Extend(b, "DistinctOn", identsToStrings(columns)).(SelectBuilder)
}

// SafeColumns adds result columns to the query using safe Ident values.
//
// Ex:
//
//	cols, _ := sq.QuoteIdents("id", "name")
//	sq.Select().SafeColumns(cols...)
func (b SelectBuilder) SafeColumns(columns ...Ident) SelectBuilder {
	parts := make([]any, 0, len(columns))
	for _, id := range columns {
		parts = append(parts, newPart(id.String()))
	}
	return builder.Extend(b, "Columns", parts).(SelectBuilder)
}

// SafeGroupBy adds GROUP BY expressions using safe Ident values.
//
// Ex:
//
//	id, _ := sq.QuoteIdent(userInput)
//	sq.Select("count(*)").SafeFrom(tableId).SafeGroupBy(id)
func (b SelectBuilder) SafeGroupBy(groupBys ...Ident) SelectBuilder {
	return builder.Extend(b, "GroupBys", identsToStrings(groupBys)).(SelectBuilder)
}

// SafeOrderBy adds ORDER BY expressions using safe Ident values. Each Ident
// is used as a column name; to specify direction, use SafeOrderByDir.
//
// Ex:
//
//	id, _ := sq.QuoteIdent("name")
//	sq.Select("*").From("users").SafeOrderBy(id)
func (b SelectBuilder) SafeOrderBy(orderBys ...Ident) SelectBuilder {
	for _, id := range orderBys {
		b = b.OrderByClause(id.String())
	}
	return b
}

// OrderDir represents an ORDER BY sort direction.
type OrderDir string

const (
	// Asc sorts in ascending order.
	Asc OrderDir = "ASC"
	// Desc sorts in descending order.
	Desc OrderDir = "DESC"
)

// SafeOrderByDir adds a single ORDER BY expression with a safe Ident column
// and an explicit sort direction.
//
// Ex:
//
//	col, _ := sq.QuoteIdent(userSortColumn)
//	sq.Select("*").From("users").SafeOrderByDir(col, sq.Desc)
func (b SelectBuilder) SafeOrderByDir(column Ident, dir OrderDir) SelectBuilder {
	switch dir {
	case Asc, Desc:
		return b.OrderByClause(column.String() + " " + string(dir))
	default:
		// Default to no direction (database default, typically ASC).
		return b.OrderByClause(column.String())
	}
}
