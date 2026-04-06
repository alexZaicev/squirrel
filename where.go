package squirrel

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

type wherePart part

func newWherePart(pred any, args ...any) Sqlizer {
	return &wherePart{pred: pred, args: args}
}

func (p wherePart) ToSQL() (sql string, args []any, err error) {
	switch pred := p.pred.(type) {
	case nil:
		// no-op
	case rawSqlizer:
		return pred.toSQLRaw()
	case Sqlizer:
		return pred.ToSQL()
	case map[string]any:
		return Eq(pred).ToSQL()
	case string:
		sql, args = expandWhereArgs(pred, p.args)
	default:
		err = fmt.Errorf("expected string-keyed map or string, not %T", pred)
	}
	return
}

// expandWhereArgs expands slice/array arguments in raw string where parts into
// (?,?,?) placeholders (GitHub #383) and auto-parenthesizes clauses that
// contain OR or AND to prevent operator-precedence surprises when multiple
// Where() calls are combined (GitHub #380).
func expandWhereArgs(pred string, in []any) (string, []any) {
	// Fast path: no slice args → no expansion needed.
	needsExpansion := false
	for _, a := range in {
		if isExpandableSlice(a) {
			needsExpansion = true
			break
		}
	}

	var sql string
	var args []any

	if !needsExpansion {
		sql = pred
		args = in
	} else {
		// Walk the SQL string placeholder-by-placeholder, expanding slices.
		var buf strings.Builder
		sp := pred
		ap := in
		for len(ap) > 0 && len(sp) > 0 {
			i := strings.Index(sp, "?")
			if i < 0 {
				break
			}
			// Handle escaped "??"
			if len(sp) > i+1 && sp[i+1] == '?' {
				buf.WriteString(sp[:i+2])
				sp = sp[i+2:]
				continue
			}
			buf.WriteString(sp[:i])
			if isExpandableSlice(ap[0]) {
				v := reflect.ValueOf(ap[0])
				buf.WriteString("(")
				buf.WriteString(Placeholders(v.Len()))
				buf.WriteString(")")
				for j := 0; j < v.Len(); j++ {
					args = append(args, v.Index(j).Interface())
				}
			} else {
				buf.WriteString("?")
				args = append(args, ap[0])
			}
			ap = ap[1:]
			sp = sp[i+1:]
		}
		buf.WriteString(sp)
		// Append any remaining args (more args than placeholders).
		args = append(args, ap...)
		sql = buf.String()
	}

	// Auto-parenthesize raw expressions that contain OR or AND so that
	// combining multiple Where() calls with " AND " doesn't cause
	// unexpected operator precedence. GitHub #380.
	if needsParens(sql) {
		sql = "(" + sql + ")"
	}

	return sql, args
}

// isExpandableSlice reports whether v is a slice or array that should be
// expanded in a raw where-part placeholder.  []byte (== []uint8) is excluded
// because database/sql treats it as a single value.
func isExpandableSlice(v any) bool {
	if v == nil {
		return false
	}
	if driver.IsValue(v) {
		return false
	}
	rv := reflect.ValueOf(v)
	k := rv.Kind()
	return k == reflect.Slice || k == reflect.Array
}

// needsParens returns true when the raw SQL string contains a bare OR
// keyword (case-insensitive, surrounded by whitespace).  If true the caller
// should wrap the clause in parentheses to prevent precedence errors when
// multiple Where() parts are joined with " AND ".  GitHub #380.
//
// Clauses containing only AND don't need wrapping because the separator
// between Where() parts is already AND — adding parentheses would be
// harmless but unnecessary noise.
func needsParens(sql string) bool {
	upper := strings.ToUpper(sql)
	// Already parenthesized at the outermost level — skip.
	if len(upper) >= 2 && upper[0] == '(' && upper[len(upper)-1] == ')' {
		// Simple heuristic: if the entire string is wrapped, don't double-wrap.
		depth := 0
		fullyWrapped := true
		for i, ch := range upper {
			switch ch {
			case '(':
				depth++
			case ')':
				depth--
			}
			if depth == 0 && i < len(upper)-1 {
				fullyWrapped = false
				break
			}
		}
		if fullyWrapped {
			return false
		}
	}
	// Look for bare OR keyword surrounded by whitespace.
	return strings.Contains(upper, " OR ")
}
