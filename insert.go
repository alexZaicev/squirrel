package squirrel

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/lann/builder"
)

type insertData struct {
	PlaceholderFormat   PlaceholderFormat
	RunWith             BaseRunner
	Prefixes            []Sqlizer
	StatementKeyword    string
	Options             []string
	Into                string
	Columns             []string
	Values              [][]any
	Returning           []string
	Suffixes            []Sqlizer
	Select              *SelectBuilder
	ConflictColumns     []string
	ConflictConstraint  string
	ConflictDoNothing   bool
	ConflictDoUpdates   []setClause
	ConflictWhereParts  []Sqlizer
	DuplicateKeyUpdates []setClause
}

func (d *insertData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *insertData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *insertData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *insertData) ToSQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toSQLRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *insertData) toSQLRaw() (sqlStr string, args []any, err error) {
	if len(d.Into) == 0 {
		err = errors.New("insert statements must specify a table")
		return
	}
	if len(d.Values) == 0 && d.Select == nil {
		err = errors.New("insert statements must have at least one set of values or select clause")
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

	if d.StatementKeyword == "" {
		sql.WriteString("INSERT ")
	} else {
		sql.WriteString(d.StatementKeyword)
		sql.WriteString(" ")
	}

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(d.Into)
	sql.WriteString(" ")

	if len(d.Columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(d.Columns, ","))
		sql.WriteString(") ")
	}

	if d.Select != nil {
		args, err = d.appendSelectToSQL(sql, args)
	} else {
		args, err = d.appendValuesToSQL(sql, args)
	}
	if err != nil {
		return
	}

	if args, err = d.appendConflictToSQL(sql, args); err != nil {
		return
	}

	if args, err = d.appendDuplicateKeyToSQL(sql, args); err != nil {
		return
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

func (d *insertData) appendValuesToSQL(w io.Writer, args []any) ([]any, error) {
	if len(d.Values) == 0 {
		return args, errors.New("values for insert statements are not set")
	}

	if _, err := io.WriteString(w, "VALUES "); err != nil {
		return nil, err
	}

	valuesStrings := make([]string, len(d.Values))
	for r, row := range d.Values {
		valueStrings := make([]string, len(row))
		for v, val := range row {
			if vs, ok := val.(Sqlizer); ok {
				vsql, vargs, err := nestedToSQL(vs)
				if err != nil {
					return nil, err
				}
				valueStrings[v] = vsql
				args = append(args, vargs...)
			} else {
				valueStrings[v] = "?"
				args = append(args, val)
			}
		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}

	if _, err := io.WriteString(w, strings.Join(valuesStrings, ",")); err != nil {
		return nil, err
	}

	return args, nil
}

func (d *insertData) appendSelectToSQL(w io.Writer, args []any) ([]any, error) {
	if d.Select == nil {
		return args, errors.New("select clause for insert statements are not set")
	}

	selectClause, sArgs, err := nestedToSQL(d.Select)
	if err != nil {
		return args, err
	}

	if _, err := io.WriteString(w, selectClause); err != nil {
		return nil, err
	}
	args = append(args, sArgs...)

	return args, nil
}

func (d *insertData) appendConflictToSQL(w io.Writer, args []any) ([]any, error) {
	hasTarget := len(d.ConflictColumns) > 0 || len(d.ConflictConstraint) > 0
	hasAction := d.ConflictDoNothing || len(d.ConflictDoUpdates) > 0

	if !hasTarget && !hasAction {
		return args, nil
	}

	if d.ConflictDoNothing && len(d.ConflictDoUpdates) > 0 {
		return args, errors.New("insert on conflict: DO NOTHING and DO UPDATE are mutually exclusive")
	}

	if !d.ConflictDoNothing && len(d.ConflictDoUpdates) == 0 {
		return args, errors.New("insert on conflict: must use DO NOTHING or DO UPDATE")
	}

	if _, err := io.WriteString(w, " ON CONFLICT"); err != nil {
		return nil, err
	}

	if len(d.ConflictConstraint) > 0 {
		if _, err := io.WriteString(w, " ON CONSTRAINT "); err != nil {
			return nil, err
		}
		if _, err := io.WriteString(w, d.ConflictConstraint); err != nil {
			return nil, err
		}
	} else if len(d.ConflictColumns) > 0 {
		if _, err := io.WriteString(w, " ("); err != nil {
			return nil, err
		}
		if _, err := io.WriteString(w, strings.Join(d.ConflictColumns, ",")); err != nil {
			return nil, err
		}
		if _, err := io.WriteString(w, ")"); err != nil {
			return nil, err
		}
	}

	if d.ConflictDoNothing {
		if _, err := io.WriteString(w, " DO NOTHING"); err != nil {
			return nil, err
		}
		return args, nil
	}

	if _, err := io.WriteString(w, " DO UPDATE SET "); err != nil {
		return nil, err
	}

	args, err := appendSetClauses(d.ConflictDoUpdates, w, args)
	if err != nil {
		return nil, err
	}

	if len(d.ConflictWhereParts) > 0 {
		args, err = appendPrefixedToSQL(d.ConflictWhereParts, w, " WHERE ", args)
		if err != nil {
			return nil, err
		}
	}

	return args, nil
}

func (d *insertData) appendDuplicateKeyToSQL(w io.Writer, args []any) ([]any, error) {
	if len(d.DuplicateKeyUpdates) == 0 {
		return args, nil
	}

	if _, err := io.WriteString(w, " ON DUPLICATE KEY UPDATE "); err != nil {
		return nil, err
	}

	return appendSetClauses(d.DuplicateKeyUpdates, w, args)
}

func appendSetClauses(setClauses []setClause, w io.Writer, args []any) ([]any, error) {
	setSQLs := make([]string, len(setClauses))
	for i, sc := range setClauses {
		var valSQL string
		if vs, ok := sc.value.(Sqlizer); ok {
			vsql, vargs, err := nestedToSQL(vs)
			if err != nil {
				return nil, err
			}
			if _, ok := vs.(SelectBuilder); ok {
				valSQL = fmt.Sprintf("(%s)", vsql)
			} else {
				valSQL = vsql
			}
			args = append(args, vargs...)
		} else {
			valSQL = "?"
			args = append(args, sc.value)
		}
		setSQLs[i] = fmt.Sprintf("%s = %s", sc.column, valSQL)
	}
	_, err := io.WriteString(w, strings.Join(setSQLs, ", "))
	return args, err
}

// Builder

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder builder.Builder

func init() {
	builder.Register(InsertBuilder{}, insertData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b InsertBuilder) PlaceholderFormat(f PlaceholderFormat) InsertBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(InsertBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b InsertBuilder) RunWith(runner BaseRunner) InsertBuilder {
	return setRunWith(b, runner).(InsertBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b InsertBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(insertData)
	return data.Exec()
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b InsertBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(insertData)
	return data.Query()
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b InsertBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(insertData)
	return data.QueryRow()
}

// Scan is a shortcut for QueryRow().Scan.
func (b InsertBuilder) Scan(dest ...any) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSQL builds the query into a SQL string and bound args.
func (b InsertBuilder) ToSQL() (string, []any, error) {
	data := builder.GetStruct(b).(insertData)
	return data.ToSQL()
}

func (b InsertBuilder) toSQLRaw() (string, []any, error) {
	data := builder.GetStruct(b).(insertData)
	return data.toSQLRaw()
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b InsertBuilder) MustSQL() (string, []any) {
	sql, args, err := b.ToSQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b InsertBuilder) Prefix(sql string, args ...any) InsertBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b InsertBuilder) PrefixExpr(expr Sqlizer) InsertBuilder {
	return builder.Append(b, "Prefixes", expr).(InsertBuilder)
}

// Options adds keyword options before the INTO clause of the query.
//
// WARNING: Options are interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
func (b InsertBuilder) Options(options ...string) InsertBuilder {
	return builder.Extend(b, "Options", options).(InsertBuilder)
}

// Into sets the INTO clause of the query.
//
// WARNING: The table name is interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
// For dynamic table names from user input, use SafeInto instead.
func (b InsertBuilder) Into(into string) InsertBuilder {
	return builder.Set(b, "Into", into).(InsertBuilder)
}

// Columns adds insert columns to the query.
//
// WARNING: Column names are interpolated directly into the SQL string without
// sanitization. NEVER pass unsanitized user input to this method.
// For dynamic column names from user input, use SafeColumns instead.
func (b InsertBuilder) Columns(columns ...string) InsertBuilder {
	return builder.Extend(b, "Columns", columns).(InsertBuilder)
}

// Values adds a single row's values to the query.
func (b InsertBuilder) Values(values ...any) InsertBuilder {
	return builder.Append(b, "Values", values).(InsertBuilder)
}

// Suffix adds an expression to the end of the query
func (b InsertBuilder) Suffix(sql string, args ...any) InsertBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b InsertBuilder) SuffixExpr(expr Sqlizer) InsertBuilder {
	return builder.Append(b, "Suffixes", expr).(InsertBuilder)
}

// Returning adds RETURNING expressions to the query.
//
// Ex:
//
//	Insert("users").Columns("name").Values("John").
//		Returning("id", "created_at")
//	// INSERT INTO users (name) VALUES (?) RETURNING id, created_at
func (b InsertBuilder) Returning(columns ...string) InsertBuilder {
	return builder.Extend(b, "Returning", columns).(InsertBuilder)
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b InsertBuilder) SetMap(clauses map[string]any) InsertBuilder {
	// Keep the columns in a consistent order by sorting the column key string.
	cols := make([]string, 0, len(clauses))
	for col := range clauses {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	vals := make([]any, 0, len(clauses))
	for _, col := range cols {
		vals = append(vals, clauses[col])
	}

	b = builder.Set(b, "Columns", cols).(InsertBuilder)
	b = builder.Set(b, "Values", [][]any{vals}).(InsertBuilder)

	return b
}

// Select set Select clause for insert query
// If Values and Select are used, then Select has higher priority
func (b InsertBuilder) Select(sb SelectBuilder) InsertBuilder {
	return builder.Set(b, "Select", &sb).(InsertBuilder)
}

func (b InsertBuilder) statementKeyword(keyword string) InsertBuilder {
	return builder.Set(b, "StatementKeyword", keyword).(InsertBuilder)
}

// OnConflictColumns sets the conflict target columns for a PostgreSQL
// ON CONFLICT clause. Use with OnConflictDoNothing or OnConflictDoUpdate.
//
// Ex:
//
//	Insert("users").Columns("id", "name").Values(1, "John").
//		OnConflictColumns("id").OnConflictDoNothing()
//	// INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO NOTHING
func (b InsertBuilder) OnConflictColumns(columns ...string) InsertBuilder {
	return builder.Extend(b, "ConflictColumns", columns).(InsertBuilder)
}

// OnConflictOnConstraint sets the conflict target to a named constraint for a
// PostgreSQL ON CONFLICT ON CONSTRAINT clause.
//
// Ex:
//
//	Insert("users").Columns("id", "name").Values(1, "John").
//		OnConflictOnConstraint("users_pkey").OnConflictDoNothing()
//	// INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING
func (b InsertBuilder) OnConflictOnConstraint(name string) InsertBuilder {
	return builder.Set(b, "ConflictConstraint", name).(InsertBuilder)
}

// OnConflictDoNothing sets the conflict action to DO NOTHING for a PostgreSQL
// ON CONFLICT clause.
//
// Ex:
//
//	Insert("users").Columns("id", "name").Values(1, "John").
//		OnConflictColumns("id").OnConflictDoNothing()
//	// INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO NOTHING
func (b InsertBuilder) OnConflictDoNothing() InsertBuilder {
	return builder.Set(b, "ConflictDoNothing", true).(InsertBuilder)
}

// OnConflictDoUpdate adds a column = value SET clause to the DO UPDATE action
// for a PostgreSQL ON CONFLICT clause. The value can be a Sqlizer (e.g. Expr)
// for expressions like EXCLUDED.column.
//
// Ex:
//
//	Insert("users").Columns("id", "name").Values(1, "John").
//		OnConflictColumns("id").
//		OnConflictDoUpdate("name", sq.Expr("EXCLUDED.name"))
//	// INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
func (b InsertBuilder) OnConflictDoUpdate(column string, value any) InsertBuilder {
	return builder.Append(b, "ConflictDoUpdates", setClause{column: column, value: value}).(InsertBuilder)
}

// OnConflictDoUpdateMap is a convenience method that calls OnConflictDoUpdate for
// each key/value pair in clauses.
func (b InsertBuilder) OnConflictDoUpdateMap(clauses map[string]any) InsertBuilder {
	keys := make([]string, 0, len(clauses))
	for key := range clauses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		b = b.OnConflictDoUpdate(key, clauses[key])
	}
	return b
}

// OnConflictWhere adds a WHERE clause to the DO UPDATE action of a PostgreSQL
// ON CONFLICT clause.
//
// Ex:
//
//	Insert("users").Columns("id", "name").Values(1, "John").
//		OnConflictColumns("id").
//		OnConflictDoUpdate("name", sq.Expr("EXCLUDED.name")).
//		OnConflictWhere(sq.Eq{"users.active": true})
func (b InsertBuilder) OnConflictWhere(pred any, args ...any) InsertBuilder {
	return builder.Append(b, "ConflictWhereParts", newWherePart(pred, args...)).(InsertBuilder)
}

// OnDuplicateKeyUpdate adds a column = value clause to a MySQL
// ON DUPLICATE KEY UPDATE clause.
//
// Ex:
//
//	Insert("users").Columns("id", "name").Values(1, "John").
//		OnDuplicateKeyUpdate("name", "John")
//	// INSERT INTO users (id,name) VALUES (?,?) ON DUPLICATE KEY UPDATE name = ?
func (b InsertBuilder) OnDuplicateKeyUpdate(column string, value any) InsertBuilder {
	return builder.Append(b, "DuplicateKeyUpdates", setClause{column: column, value: value}).(InsertBuilder)
}

// OnDuplicateKeyUpdateMap is a convenience method that calls OnDuplicateKeyUpdate
// for each key/value pair in clauses.
func (b InsertBuilder) OnDuplicateKeyUpdateMap(clauses map[string]any) InsertBuilder {
	keys := make([]string, 0, len(clauses))
	for key := range clauses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		b = b.OnDuplicateKeyUpdate(key, clauses[key])
	}
	return b
}

// Safe identifier methods
//
// The following methods accept Ident values produced by QuoteIdent or
// ValidateIdent, guaranteeing that the identifiers are safe for interpolation
// into SQL.

// SafeInto sets the INTO clause of the query using a safe Ident.
//
// Ex:
//
//	id, _ := sq.QuoteIdent(userInput)
//	sq.Insert("").SafeInto(id).Columns("name").Values("John")
func (b InsertBuilder) SafeInto(into Ident) InsertBuilder {
	return builder.Set(b, "Into", into.String()).(InsertBuilder)
}

// SafeColumns adds insert columns to the query using safe Ident values.
//
// Ex:
//
//	cols, _ := sq.QuoteIdents("id", "name")
//	sq.Insert("users").SafeColumns(cols...).Values(1, "John")
func (b InsertBuilder) SafeColumns(columns ...Ident) InsertBuilder {
	return builder.Extend(b, "Columns", identsToStrings(columns)).(InsertBuilder)
}
