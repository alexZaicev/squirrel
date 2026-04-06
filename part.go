package squirrel

import (
	"bytes"
	"fmt"
	"io"
)

type part struct {
	pred any
	args []any
}

func newPart(pred any, args ...any) Sqlizer {
	return &part{pred, args}
}

func (p part) ToSQL() (sql string, args []any, err error) {
	switch pred := p.pred.(type) {
	case nil:
		// no-op
	case Sqlizer:
		sql, args, err = nestedToSQL(pred)
	case string:
		sql = pred
		args = p.args
	default:
		err = fmt.Errorf("expected string or Sqlizer, not %T", pred)
	}
	return
}

func nestedToSQL(s Sqlizer) (string, []any, error) {
	if raw, ok := s.(rawSqlizer); ok {
		return raw.toSQLRaw()
	}
	return s.ToSQL()
}

func appendToSQL(parts []Sqlizer, w io.Writer, sep string, args []any) ([]any, error) {
	first := true
	for _, p := range parts {
		partSQL, partArgs, err := nestedToSQL(p)
		if err != nil {
			return nil, err
		} else if len(partSQL) == 0 {
			continue
		}

		if !first {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		first = false

		_, err = io.WriteString(w, partSQL)
		if err != nil {
			return nil, err
		}
		args = append(args, partArgs...)
	}
	return args, nil
}

// appendPrefixedToSQL writes prefix followed by the SQL parts only if any
// part produces non-empty SQL. This prevents dangling keywords like "WHERE "
// when all parts evaluate to empty SQL (e.g., nil/empty Or/And clauses).
func appendPrefixedToSQL(parts []Sqlizer, w io.Writer, prefix string, args []any) ([]any, error) {
	if len(parts) == 0 {
		return args, nil
	}
	var buf bytes.Buffer
	var err error
	args, err = appendToSQL(parts, &buf, " AND ", args)
	if err != nil {
		return args, err
	}
	if buf.Len() > 0 {
		if _, err = io.WriteString(w, prefix); err != nil {
			return args, err
		}
		if _, err = buf.WriteTo(w); err != nil {
			return args, err
		}
	}
	return args, nil
}
