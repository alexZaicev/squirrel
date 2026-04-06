package squirrel

import "github.com/lann/builder"

// StatementBuilderType is the type of StatementBuilder.
type StatementBuilderType builder.Builder

// Select returns a SelectBuilder for this StatementBuilderType.
func (b StatementBuilderType) Select(columns ...string) SelectBuilder {
	return SelectBuilder(b).Columns(columns...)
}

// Insert returns a InsertBuilder for this StatementBuilderType.
func (b StatementBuilderType) Insert(into string) InsertBuilder {
	return InsertBuilder(b).Into(into)
}

// Replace returns a InsertBuilder for this StatementBuilderType with the
// statement keyword set to "REPLACE".
func (b StatementBuilderType) Replace(into string) InsertBuilder {
	return InsertBuilder(b).statementKeyword("REPLACE").Into(into)
}

// Update returns a UpdateBuilder for this StatementBuilderType.
func (b StatementBuilderType) Update(table string) UpdateBuilder {
	return UpdateBuilder(b).Table(table)
}

// Delete returns a DeleteBuilder for this StatementBuilderType.
func (b StatementBuilderType) Delete(from string) DeleteBuilder {
	return DeleteBuilder(b).From(from)
}

// PlaceholderFormat sets the PlaceholderFormat field for any child builders.
func (b StatementBuilderType) PlaceholderFormat(f PlaceholderFormat) StatementBuilderType {
	return builder.Set(b, "PlaceholderFormat", f).(StatementBuilderType)
}

// RunWith sets the RunWith field for any child builders.
func (b StatementBuilderType) RunWith(runner BaseRunner) StatementBuilderType {
	return setRunWith(b, runner).(StatementBuilderType)
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b StatementBuilderType) Where(pred any, args ...any) StatementBuilderType {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(StatementBuilderType)
}

// StatementBuilder is a parent builder for other builders, e.g. SelectBuilder.
var StatementBuilder = StatementBuilderType(builder.EmptyBuilder).PlaceholderFormat(Question)

// Select returns a new SelectBuilder, optionally setting some result columns.
//
// See SelectBuilder.Columns.
func Select(columns ...string) SelectBuilder {
	return StatementBuilder.Select(columns...)
}

// Insert returns a new InsertBuilder with the given table name.
//
// See InsertBuilder.Into.
func Insert(into string) InsertBuilder {
	return StatementBuilder.Insert(into)
}

// Replace returns a new InsertBuilder with the statement keyword set to
// "REPLACE" and with the given table name.
//
// See InsertBuilder.Into.
func Replace(into string) InsertBuilder {
	return StatementBuilder.Replace(into)
}

// Update returns a new UpdateBuilder with the given table name.
//
// See UpdateBuilder.Table.
func Update(table string) UpdateBuilder {
	return StatementBuilder.Update(table)
}

// Delete returns a new DeleteBuilder with the given table name.
//
// See DeleteBuilder.Table.
func Delete(from string) DeleteBuilder {
	return StatementBuilder.Delete(from)
}

// Union returns a new UnionBuilder combining the given SELECTs with UNION.
func Union(selects ...SelectBuilder) UnionBuilder {
	return newUnionBuilder("UNION", selects)
}

// UnionAll returns a new UnionBuilder combining the given SELECTs with UNION ALL.
func UnionAll(selects ...SelectBuilder) UnionBuilder {
	return newUnionBuilder("UNION ALL", selects)
}

// Intersect returns a new UnionBuilder combining the given SELECTs with INTERSECT.
func Intersect(selects ...SelectBuilder) UnionBuilder {
	return newUnionBuilder("INTERSECT", selects)
}

// Except returns a new UnionBuilder combining the given SELECTs with EXCEPT.
func Except(selects ...SelectBuilder) UnionBuilder {
	return newUnionBuilder("EXCEPT", selects)
}

// With creates a new CteBuilder with a single CTE definition.
//
// Ex:
//
//	With("active_users", Select("id", "name").From("users").Where(Eq{"active": true})).
//		Statement(Select("*").From("active_users"))
//	// WITH active_users AS (SELECT id, name FROM users WHERE active = ?) SELECT * FROM active_users
func With(name string, as Sqlizer) CteBuilder {
	return CteBuilder(builder.EmptyBuilder).
		PlaceholderFormat(Question).
		With(name, as)
}

// WithRecursive creates a new CteBuilder with a single recursive CTE definition.
//
// Ex:
//
//	WithRecursive("tree",
//		Union(
//			Select("id", "parent_id").From("categories").Where(Eq{"parent_id": nil}),
//			Select("c.id", "c.parent_id").From("categories c").Join("tree t ON c.parent_id = t.id"),
//		),
//	).Statement(Select("*").From("tree"))
//	// WITH RECURSIVE tree AS (...) SELECT * FROM tree
func WithRecursive(name string, as Sqlizer) CteBuilder {
	return CteBuilder(builder.EmptyBuilder).
		PlaceholderFormat(Question).
		WithRecursive(name, as)
}

// WithColumns creates a new CteBuilder with a single CTE definition that has
// explicit column names.
//
// Ex:
//
//	WithColumns("cte", []string{"x", "y"}, Select("a", "b").From("t1")).
//		Statement(Select("x", "y").From("cte"))
//	// WITH cte (x, y) AS (SELECT a, b FROM t1) SELECT x, y FROM cte
func WithColumns(name string, columns []string, as Sqlizer) CteBuilder {
	return CteBuilder(builder.EmptyBuilder).
		PlaceholderFormat(Question).
		WithColumns(name, columns, as)
}

// WithRecursiveColumns creates a new CteBuilder with a single recursive CTE
// definition that has explicit column names.
//
// Ex:
//
//	WithRecursiveColumns("cnt", []string{"x"},
//		Union(Select("1"), Select("x + 1").From("cnt").Where("x < ?", 100)),
//	).Statement(Select("x").From("cnt"))
//	// WITH RECURSIVE cnt (x) AS (...) SELECT x FROM cnt
func WithRecursiveColumns(name string, columns []string, as Sqlizer) CteBuilder {
	return CteBuilder(builder.EmptyBuilder).
		PlaceholderFormat(Question).
		WithRecursiveColumns(name, columns, as)
}

// Case returns a new CaseBuilder
// "what" represents case value
func Case(what ...any) CaseBuilder {
	b := CaseBuilder(builder.EmptyBuilder)

	switch len(what) {
	case 0:
	case 1:
		b = b.what(what[0])
	default:
		b = b.what(newPart(what[0], what[1:]...))
	}
	return b
}
