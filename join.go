package squirrel

import (
	"bytes"
	"fmt"
	"strings"
)

// JoinType represents the type of SQL JOIN.
type JoinType string

const (
	// JoinInner represents a JOIN / INNER JOIN.
	JoinInner JoinType = "JOIN"
	// JoinLeft represents a LEFT JOIN.
	JoinLeft JoinType = "LEFT JOIN"
	// JoinRight represents a RIGHT JOIN.
	JoinRight JoinType = "RIGHT JOIN"
	// JoinFull represents a FULL OUTER JOIN.
	JoinFull JoinType = "FULL OUTER JOIN"
	// JoinCross represents a CROSS JOIN.
	JoinCross JoinType = "CROSS JOIN"
)

// joinExpr is a structured join clause that implements Sqlizer.
// It is produced by the JoinExpr builder and consumed by SelectBuilder.JoinClause.
type joinExpr struct {
	joinType JoinType
	table    string
	alias    string
	subQuery Sqlizer

	// Exactly one of onParts or usingCols must be set (or neither for CROSS JOIN).
	onParts   []Sqlizer
	usingCols []string
}

// JoinBuilder builds a structured join clause. It implements Sqlizer so it can
// be passed directly to SelectBuilder.JoinClause. Use JoinExpr to create one.
type JoinBuilder interface {
	Sqlizer
	// Type sets the join type (JoinInner, JoinLeft, JoinRight, JoinFull, JoinCross).
	Type(JoinType) JoinBuilder
	// As sets an alias for the joined table.
	As(string) JoinBuilder
	// SubQuery sets a subquery as the join target instead of a plain table name.
	SubQuery(Sqlizer) JoinBuilder
	// On adds a raw ON condition. Multiple calls are ANDed together.
	On(pred string, args ...any) JoinBuilder
	// OnExpr adds a Sqlizer-based ON condition. Multiple calls are ANDed together.
	OnExpr(Sqlizer) JoinBuilder
	// Using sets the USING columns. Mutually exclusive with On/OnExpr.
	Using(columns ...string) JoinBuilder
}

// JoinExpr starts building a structured join clause for the given table.
//
// Use On / OnExpr to add ON conditions, or Using to add USING columns.
// Pass the result to SelectBuilder.JoinClause:
//
//	sq.Select("*").From("items").JoinClause(
//	    sq.JoinExpr("users").On("items.fk_user_key = users.key"),
//	)
//	// SELECT * FROM items JOIN users ON items.fk_user_key = users.key
func JoinExpr(table string) JoinBuilder {
	return &joinExprBuilder{data: joinExpr{joinType: JoinInner, table: table}}
}

// joinExprBuilder is a mutable builder that produces a join clause Sqlizer.
// It implements JoinBuilder. Methods return JoinBuilder to allow chaining.
type joinExprBuilder struct {
	data joinExpr
}

// Type sets the join type (JoinInner, JoinLeft, JoinRight, JoinFull, JoinCross).
func (b *joinExprBuilder) Type(t JoinType) JoinBuilder {
	b.data.joinType = t
	return b
}

// As sets an alias for the joined table.
//
//	sq.JoinExpr("users").As("u").On("items.user_id = u.id")
func (b *joinExprBuilder) As(alias string) JoinBuilder {
	b.data.alias = alias
	return b
}

// SubQuery sets a subquery as the join target instead of a plain table name.
// When set, the table field is ignored and the subquery SQL is wrapped in
// parentheses. An alias should be provided via As().
//
//	sub := sq.Select("id", "name").From("users").Where(sq.Eq{"active": true})
//	sq.Select("*").From("items").JoinClause(
//	    sq.JoinExpr("").SubQuery(sub).As("u").On("items.user_id = u.id"),
//	)
func (b *joinExprBuilder) SubQuery(sub Sqlizer) JoinBuilder {
	b.data.subQuery = sub
	return b
}

// On adds one or more raw ON conditions. Multiple calls are ANDed together.
//
//	sq.JoinExpr("users").On("items.fk_user_key = users.key")
//
//	sq.JoinExpr("users").
//	    On("items.fk_user_key = users.key").
//	    On("users.active = ?", true)
func (b *joinExprBuilder) On(pred string, args ...any) JoinBuilder {
	b.data.onParts = append(b.data.onParts, Expr(pred, args...))
	return b
}

// OnExpr adds a Sqlizer-based ON condition. Multiple calls are ANDed together.
// This allows reuse of the expression helpers (Eq, Gt, etc.) in join conditions.
//
//	sq.JoinExpr("users").
//	    OnExpr(sq.Eq{"items.fk_user_key": sq.Expr("users.key")}).
//	    OnExpr(sq.Eq{"users.active": true})
func (b *joinExprBuilder) OnExpr(pred Sqlizer) JoinBuilder {
	b.data.onParts = append(b.data.onParts, pred)
	return b
}

// Using sets the USING columns. This is mutually exclusive with On/OnExpr.
//
//	sq.JoinExpr("emails").Using("email_id")
//	sq.JoinExpr("addresses").Using("user_id", "region_id")
func (b *joinExprBuilder) Using(columns ...string) JoinBuilder {
	b.data.usingCols = append(b.data.usingCols, columns...)
	return b
}

// ToSQL renders the join clause. joinExprBuilder implements Sqlizer.
func (b *joinExprBuilder) ToSQL() (string, []any, error) {
	return b.data.toSQL()
}

func (j joinExpr) toSQL() (string, []any, error) {
	buf := &bytes.Buffer{}
	var args []any

	// Join type keyword.
	buf.WriteString(string(j.joinType))
	buf.WriteString(" ")

	// Table or subquery.
	if j.subQuery != nil {
		subSQL, subArgs, err := nestedToSQL(j.subQuery)
		if err != nil {
			return "", nil, err
		}
		fmt.Fprintf(buf, "(%s)", subSQL)
		args = append(args, subArgs...)
	} else {
		if j.table == "" {
			return "", nil, fmt.Errorf("join expression requires a table name or subquery")
		}
		buf.WriteString(j.table)
	}

	// Alias.
	if j.alias != "" {
		buf.WriteString(" ")
		buf.WriteString(j.alias)
	}

	// USING or ON clause.
	if len(j.usingCols) > 0 {
		fmt.Fprintf(buf, " USING (%s)", strings.Join(j.usingCols, ", "))
	} else if len(j.onParts) > 0 {
		buf.WriteString(" ON ")
		for i, p := range j.onParts {
			if i > 0 {
				buf.WriteString(" AND ")
			}
			pSQL, pArgs, err := p.ToSQL()
			if err != nil {
				return "", nil, err
			}
			buf.WriteString(pSQL)
			args = append(args, pArgs...)
		}
	}

	return buf.String(), args, nil
}
