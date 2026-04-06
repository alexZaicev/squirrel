package squirrel

import (
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
	for i, p := range parts {
		partSQL, partArgs, err := nestedToSQL(p)
		if err != nil {
			return nil, err
		} else if len(partSQL) == 0 {
			continue
		}

		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}

		_, err = io.WriteString(w, partSQL)
		if err != nil {
			return nil, err
		}
		args = append(args, partArgs...)
	}
	return args, nil
}
