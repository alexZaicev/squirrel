package squirrel

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const (
	// Portable true/false literals.
	sqlTrue  = "(1=1)"
	sqlFalse = "(1=0)"
)

type expr struct {
	sql  string
	args []any
}

// Expr builds an expression from a SQL fragment and arguments.
//
// Ex:
//
//	Expr("FROM_UNIXTIME(?)", t)
func Expr(sql string, args ...any) Sqlizer {
	return expr{sql: sql, args: args}
}

func (e expr) ToSQL() (sql string, args []any, err error) {
	return e.toSQLInner(false)
}

func (e expr) toSQLRaw() (sql string, args []any, err error) {
	return e.toSQLInner(true)
}

func (e expr) toSQLInner(nested bool) (sql string, args []any, err error) {
	simple := true
	for _, arg := range e.args {
		if _, ok := arg.(Sqlizer); ok {
			simple = false
		}
	}
	if simple {
		return e.sql, e.args, nil
	}

	buf := &bytes.Buffer{}
	ap := e.args
	sp := e.sql

	var isql string
	var iargs []any

	for err == nil && len(ap) > 0 && len(sp) > 0 {
		i := strings.Index(sp, "?")
		if i < 0 {
			// no more placeholders
			break
		}
		if len(sp) > i+1 && sp[i+1:i+2] == "?" {
			// escaped "??"; append it and step past
			buf.WriteString(sp[:i+2])
			sp = sp[i+2:]
			continue
		}

		if as, ok := ap[0].(Sqlizer); ok {
			// sqlizer argument; expand it and append the result
			if nested {
				isql, iargs, err = nestedToSQL(as)
			} else {
				isql, iargs, err = as.ToSQL()
			}
			buf.WriteString(sp[:i])
			buf.WriteString(isql)
			args = append(args, iargs...)
		} else {
			// normal argument; append it and the placeholder
			buf.WriteString(sp[:i+1])
			args = append(args, ap[0])
		}

		// step past the argument and placeholder
		ap = ap[1:]
		sp = sp[i+1:]
	}

	// append the remaining sql and arguments
	buf.WriteString(sp)
	return buf.String(), append(args, ap...), err
}

type concatExpr []any

func (ce concatExpr) ToSQL() (sql string, args []any, err error) {
	for _, part := range ce {
		switch p := part.(type) {
		case string:
			sql += p
		case Sqlizer:
			pSQL, pArgs, err := p.ToSQL()
			if err != nil {
				return "", nil, err
			}
			sql += pSQL
			args = append(args, pArgs...)
		default:
			return "", nil, fmt.Errorf("%#v is not a string or Sqlizer", part)
		}
	}
	return
}

func (ce concatExpr) toSQLRaw() (sql string, args []any, err error) {
	for _, part := range ce {
		switch p := part.(type) {
		case string:
			sql += p
		case Sqlizer:
			pSQL, pArgs, err := nestedToSQL(p)
			if err != nil {
				return "", nil, err
			}
			sql += pSQL
			args = append(args, pArgs...)
		default:
			return "", nil, fmt.Errorf("%#v is not a string or Sqlizer", part)
		}
	}
	return
}

// ConcatExpr builds an expression by concatenating strings and other expressions.
//
// Ex:
//
//	name_expr := Expr("CONCAT(?, ' ', ?)", firstName, lastName)
//	ConcatExpr("COALESCE(full_name,", name_expr, ")")
func ConcatExpr(parts ...any) Sqlizer {
	return concatExpr(parts)
}

// aliasExpr helps to alias part of SQL query generated with underlying "expr"
type aliasExpr struct {
	expr  Sqlizer
	alias string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//
//	.Column(Alias(caseStmt, "case_column"))
func Alias(expr Sqlizer, alias string) Sqlizer {
	return aliasExpr{expr, alias}
}

func (e aliasExpr) ToSQL() (sql string, args []any, err error) {
	sql, args, err = e.expr.ToSQL()
	if err == nil {
		sql = fmt.Sprintf("(%s) AS %s", sql, e.alias)
	}
	return
}

func (e aliasExpr) toSQLRaw() (sql string, args []any, err error) {
	sql, args, err = nestedToSQL(e.expr)
	if err == nil {
		sql = fmt.Sprintf("(%s) AS %s", sql, e.alias)
	}
	return
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
type Eq map[string]any

func (eq Eq) toSQL(useNotOpr bool) (sql string, args []any, err error) {
	if len(eq) == 0 {
		// Empty Sql{} evaluates to true.
		sql = sqlTrue
		return
	}

	var (
		exprs       []string
		equalOpr    = "="
		inOpr       = "IN"
		nullOpr     = "IS"
		inEmptyExpr = sqlFalse
	)

	if useNotOpr {
		equalOpr = "<>"
		inOpr = "NOT IN"
		nullOpr = "IS NOT"
		inEmptyExpr = sqlTrue
	}

	sortedKeys := getSortedKeys(eq)
	for _, key := range sortedKeys {
		var expr string
		val := eq[key]

		// Sqlizer values (e.g. SelectBuilder) are treated as subqueries:
		//   Eq{"col": subquery}    → col IN (SELECT ...)
		//   NotEq{"col": subquery} → col NOT IN (SELECT ...)
		if sqlizer, ok := val.(Sqlizer); ok {
			subSQL, subArgs, serr := nestedToSQL(sqlizer)
			if serr != nil {
				err = serr
				return
			}
			expr = fmt.Sprintf("%s %s (%s)", key, inOpr, subSQL)
			args = append(args, subArgs...)
			exprs = append(exprs, expr)
			continue
		}

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		r := reflect.ValueOf(val)
		if r.Kind() == reflect.Ptr {
			if r.IsNil() {
				val = nil
			} else {
				val = r.Elem().Interface()
			}
		}

		if val == nil {
			expr = fmt.Sprintf("%s %s NULL", key, nullOpr)
		} else {
			if isListType(val) {
				valVal := reflect.ValueOf(val)
				if valVal.Kind() == reflect.Slice && valVal.IsNil() {
					// A nil slice (e.g. []uint64(nil)) is treated as NULL,
					// not as an empty IN list. GitHub #277.
					expr = fmt.Sprintf("%s %s NULL", key, nullOpr)
				} else if valVal.Len() == 0 {
					expr = inEmptyExpr
					if args == nil {
						args = []any{}
					}
				} else {
					for i := 0; i < valVal.Len(); i++ {
						args = append(args, valVal.Index(i).Interface())
					}
					expr = fmt.Sprintf("%s %s (%s)", key, inOpr, Placeholders(valVal.Len()))
				}
			} else {
				expr = fmt.Sprintf("%s %s ?", key, equalOpr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	if len(exprs) > 1 {
		sql = fmt.Sprintf("(%s)", sql)
	}
	return
}

func (eq Eq) ToSQL() (sql string, args []any, err error) {
	return eq.toSQL(false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NotEq{"id": 1}) == "id <> 1"
type NotEq Eq

func (neq NotEq) ToSQL() (sql string, args []any, err error) {
	return Eq(neq).toSQL(true)
}

// Like is syntactic sugar for use with LIKE conditions.
// Ex:
//
//	.Where(Like{"name": "%irrel"})
type Like map[string]any

func (lk Like) toSQL(opr string) (sql string, args []any, err error) {
	var exprs []string
	for key, val := range lk {
		expr := ""

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with like operators")
			return
		}
		if isListType(val) {
			err = fmt.Errorf("cannot use array or slice with like operators")
			return
		}
		expr = fmt.Sprintf("%s %s ?", key, opr)
		args = append(args, val)
		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	if len(exprs) > 1 {
		sql = fmt.Sprintf("(%s)", sql)
	}
	return
}

func (lk Like) ToSQL() (sql string, args []any, err error) {
	return lk.toSQL("LIKE")
}

// NotLike is syntactic sugar for use with LIKE conditions.
// Ex:
//
//	.Where(NotLike{"name": "%irrel"})
type NotLike Like

func (nlk NotLike) ToSQL() (sql string, args []any, err error) {
	return Like(nlk).toSQL("NOT LIKE")
}

// ILike is syntactic sugar for use with ILIKE conditions.
// Ex:
//
//	.Where(ILike{"name": "sq%"})
type ILike Like

func (ilk ILike) ToSQL() (sql string, args []any, err error) {
	return Like(ilk).toSQL("ILIKE")
}

// NotILike is syntactic sugar for use with ILIKE conditions.
// Ex:
//
//	.Where(NotILike{"name": "sq%"})
type NotILike Like

func (nilk NotILike) ToSQL() (sql string, args []any, err error) {
	return Like(nilk).toSQL("NOT ILIKE")
}

// Lt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Lt{"id": 1})
type Lt map[string]any

func (lt Lt) toSQL(opposite, orEq bool) (sql string, args []any, err error) {
	var (
		exprs []string
		opr   = "<"
	)

	if opposite {
		opr = ">"
	}

	if orEq {
		opr = fmt.Sprintf("%s%s", opr, "=")
	}

	sortedKeys := getSortedKeys(lt)
	for _, key := range sortedKeys {
		var expr string
		val := lt[key]

		// Sqlizer values (e.g. SelectBuilder) are treated as scalar subqueries:
		//   Lt{"col": subquery} → col < (SELECT ...)
		if sqlizer, ok := val.(Sqlizer); ok {
			subSQL, subArgs, serr := nestedToSQL(sqlizer)
			if serr != nil {
				err = serr
				return
			}
			expr = fmt.Sprintf("%s %s (%s)", key, opr, subSQL)
			args = append(args, subArgs...)
			exprs = append(exprs, expr)
			continue
		}

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with less than or greater than operators")
			return
		}
		if isListType(val) {
			err = fmt.Errorf("cannot use array or slice with less than or greater than operators")
			return
		}
		expr = fmt.Sprintf("%s %s ?", key, opr)
		args = append(args, val)

		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	if len(exprs) > 1 {
		sql = fmt.Sprintf("(%s)", sql)
	}
	return
}

func (lt Lt) ToSQL() (sql string, args []any, err error) {
	return lt.toSQL(false, false)
}

// LtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LtOrEq{"id": 1}) == "id <= 1"
type LtOrEq Lt

func (ltOrEq LtOrEq) ToSQL() (sql string, args []any, err error) {
	return Lt(ltOrEq).toSQL(false, true)
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Gt{"id": 1}) == "id > 1"
type Gt Lt

func (gt Gt) ToSQL() (sql string, args []any, err error) {
	return Lt(gt).toSQL(true, false)
}

// GtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(GtOrEq{"id": 1}) == "id >= 1"
type GtOrEq Lt

func (gtOrEq GtOrEq) ToSQL() (sql string, args []any, err error) {
	return Lt(gtOrEq).toSQL(true, true)
}

// Between is syntactic sugar for use with Where/Having methods.
// Values must be two-element arrays: [2]interface{}{lo, hi}.
//
// Ex:
//
//	.Where(Between{"age": [2]interface{}{18, 65}})  // age BETWEEN ? AND ?
type Between map[string]any

func (b Between) toSQL(opr string) (sql string, args []any, err error) {
	if len(b) == 0 {
		sql = sqlTrue
		return
	}

	var exprs []string

	sortedKeys := getSortedKeys(b)
	for _, key := range sortedKeys {
		val := b[key]
		if val == nil {
			err = fmt.Errorf("cannot use null with between operators")
			return
		}

		r := reflect.ValueOf(val)
		switch r.Kind() {
		case reflect.Array, reflect.Slice:
			if r.Len() != 2 {
				err = fmt.Errorf("between value for %q must have exactly 2 elements, got %d", key, r.Len())
				return
			}
			lo := r.Index(0).Interface()
			hi := r.Index(1).Interface()
			exprs = append(exprs, fmt.Sprintf("%s %s ? AND ?", key, opr))
			args = append(args, lo, hi)
		default:
			err = fmt.Errorf("between value for %q must be a two-element array or slice", key)
			return
		}
	}
	sql = strings.Join(exprs, " AND ")
	if len(exprs) > 1 {
		sql = fmt.Sprintf("(%s)", sql)
	}
	return
}

func (b Between) ToSQL() (sql string, args []any, err error) {
	return b.toSQL("BETWEEN")
}

// NotBetween is syntactic sugar for use with Where/Having methods.
// Values must be two-element arrays: [2]interface{}{lo, hi}.
//
// Ex:
//
//	.Where(NotBetween{"age": [2]interface{}{18, 65}})  // age NOT BETWEEN ? AND ?
type NotBetween Between

func (nb NotBetween) ToSQL() (sql string, args []any, err error) {
	return Between(nb).toSQL("NOT BETWEEN")
}

type conj []Sqlizer

func (c conj) join(sep string) (sql string, args []any, err error) {
	if len(c) == 0 {
		return "", []any{}, nil
	}
	var sqlParts []string
	for _, sqlizer := range c {
		partSQL, partArgs, err := nestedToSQL(sqlizer)
		if err != nil {
			return "", nil, err
		}
		if partSQL != "" {
			sqlParts = append(sqlParts, partSQL)
			args = append(args, partArgs...)
		}
	}
	if len(sqlParts) > 0 {
		sql = fmt.Sprintf("(%s)", strings.Join(sqlParts, sep))
	}
	return
}

// And conjunction Sqlizers
type And conj

func (a And) ToSQL() (string, []any, error) {
	return conj(a).join(" AND ")
}

// Or conjunction Sqlizers
type Or conj

func (o Or) ToSQL() (string, []any, error) {
	return conj(o).join(" OR ")
}

// Not negates the given Sqlizer condition.
//
// Ex:
//
//	sq.Not{sq.Eq{"active": true}}    → NOT (active = ?)
//	sq.Not{sq.Or{sq.Eq{"a": 1}, sq.Eq{"b": 2}}} → NOT ((a = ? OR b = ?))
type Not struct {
	Cond Sqlizer
}

func (n Not) ToSQL() (sql string, args []any, err error) {
	if n.Cond == nil {
		return sqlTrue, []any{}, nil
	}
	sql, args, err = nestedToSQL(n.Cond)
	if err != nil {
		return
	}
	if sql == "" {
		return sqlTrue, []any{}, nil
	}
	sql = fmt.Sprintf("NOT (%s)", sql)
	return
}

// existsExpr represents an EXISTS or NOT EXISTS subquery condition.
type existsExpr struct {
	sub Sqlizer
	not bool
}

func (e existsExpr) ToSQL() (sql string, args []any, err error) {
	if e.sub == nil {
		err = fmt.Errorf("exists operator requires a non-nil subquery")
		return
	}
	sql, args, err = nestedToSQL(e.sub)
	if err != nil {
		return
	}
	if e.not {
		sql = fmt.Sprintf("NOT EXISTS (%s)", sql)
	} else {
		sql = fmt.Sprintf("EXISTS (%s)", sql)
	}
	return
}

// Exists builds an EXISTS (subquery) expression for use with Where/Having methods.
//
// Ex:
//
//	sub := sq.Select("1").From("orders").Where("orders.user_id = users.id")
//	sq.Select("*").From("users").Where(sq.Exists(sub))
//	// SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
func Exists(subquery Sqlizer) Sqlizer {
	return existsExpr{sub: subquery}
}

// NotExists builds a NOT EXISTS (subquery) expression for use with Where/Having methods.
//
// Ex:
//
//	sub := sq.Select("1").From("orders").Where("orders.user_id = users.id")
//	sq.Select("*").From("users").Where(sq.NotExists(sub))
//	// SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
func NotExists(subquery Sqlizer) Sqlizer {
	return existsExpr{sub: subquery, not: true}
}

func getSortedKeys(exp map[string]any) []string {
	sortedKeys := make([]string, 0, len(exp))
	for k := range exp {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	return sortedKeys
}

func isListType(val any) bool {
	if driver.IsValue(val) {
		return false
	}
	valVal := reflect.ValueOf(val)
	return valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice
}
