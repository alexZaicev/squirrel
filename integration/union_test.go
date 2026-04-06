package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// helpers — query helpers for UnionBuilder
// ---------------------------------------------------------------------------

func unionQueryStrings(t *testing.T, u sqrl.UnionBuilder) []string {
	t.Helper()
	rows, err := u.Query()
	require.NoError(t, err)
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		vals = append(vals, v)
	}
	require.NoError(t, rows.Err())
	return vals
}

func unionQueryInts(t *testing.T, u sqrl.UnionBuilder) []int {
	t.Helper()
	rows, err := u.Query()
	require.NoError(t, err)
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var v int
		require.NoError(t, rows.Scan(&v))
		vals = append(vals, v)
	}
	require.NoError(t, rows.Err())
	return vals
}

// ---------------------------------------------------------------------------
// Basic UNION
// ---------------------------------------------------------------------------

func TestUnionBasic(t *testing.T) {
	t.Run("TwoSelects", func(t *testing.T) {
		// Arrange — fruit and vegetable items
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})

		// Act
		u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert — UNION deduplicates, but there are no dupes here
		assert.Equal(t, []string{"apple", "banana", "carrot", "eggplant"}, names)
	})

	t.Run("DeduplicatesRows", func(t *testing.T) {
		// Arrange — both queries will return the same rows
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})

		// Act
		u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert — UNION removes duplicates
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("ThreeSelects", func(t *testing.T) {
		// Arrange
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 3})
		q3 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 5})

		// Act
		u := sqrl.Union(q1, q2, q3).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert
		assert.Equal(t, []string{"apple", "carrot", "eggplant"}, names)
	})

	t.Run("SingleSelect", func(t *testing.T) {
		// Arrange — a union with only one query is valid
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act
		u := sqrl.Union(q1).RunWith(db).PlaceholderFormat(phf())
		names := unionQueryStrings(t, u)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("EmptyResult", func(t *testing.T) {
		// Arrange — both sides return nothing
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 999})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 998})

		// Act
		u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf())
		names := unionQueryStrings(t, u)

		// Assert
		assert.Empty(t, names)
	})
}

// ---------------------------------------------------------------------------
// UNION ALL
// ---------------------------------------------------------------------------

func TestUnionAll(t *testing.T) {
	t.Run("PreservesDuplicates", func(t *testing.T) {
		// Arrange — same query twice; UNION ALL preserves duplicates
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act
		u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf())
		names := unionQueryStrings(t, u)

		// Assert — apple appears twice
		assert.Equal(t, []string{"apple", "apple"}, names)
	})

	t.Run("ThreeSelects", func(t *testing.T) {
		// Arrange
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
		q3 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})

		// Act
		u := sqrl.UnionAll(q1, q2, q3).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert
		assert.Equal(t, []string{"apple", "apple", "banana"}, names)
	})

	t.Run("EmptyResult", func(t *testing.T) {
		// Arrange
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 999})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 998})

		// Act
		u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf())
		names := unionQueryStrings(t, u)

		// Assert
		assert.Empty(t, names)
	})
}

// ---------------------------------------------------------------------------
// INTERSECT
// ---------------------------------------------------------------------------

func TestIntersect(t *testing.T) {
	t.Run("CommonRows", func(t *testing.T) {
		// Arrange — fruit items that are also cheap (price <= 100)
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.LtOrEq{"price": 100})

		// Act
		u := sqrl.Intersect(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert — apple (100) and banana (50) are fruits AND price <= 100
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("NoCommonRows", func(t *testing.T) {
		// Arrange — fruit vs pastry: disjoint sets
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "pastry"})

		// Act
		u := sqrl.Intersect(q1, q2).RunWith(db).PlaceholderFormat(phf())
		names := unionQueryStrings(t, u)

		// Assert
		assert.Empty(t, names)
	})

	t.Run("IdenticalQueries", func(t *testing.T) {
		// Arrange — same query on both sides
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})

		// Act
		u := sqrl.Intersect(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert — all fruit rows returned (intersection of identical sets)
		assert.Equal(t, []string{"apple", "banana"}, names)
	})
}

// ---------------------------------------------------------------------------
// EXCEPT
// ---------------------------------------------------------------------------

func TestExcept(t *testing.T) {
	t.Run("SubtractRows", func(t *testing.T) {
		// Arrange — all items except vegetables
		q1 := sb.Select("name").From("sq_items").Where(sqrl.NotEq{"category": nil})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})

		// Act
		u := sqrl.Except(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert — all categorised items minus vegetables
		assert.Equal(t, []string{"apple", "banana", "donut"}, names)
	})

	t.Run("SubtractNothing", func(t *testing.T) {
		// Arrange — subtract empty set
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 999})

		// Act
		u := sqrl.Except(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert — original set unchanged
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("SubtractAll", func(t *testing.T) {
		// Arrange — subtract the entire set from itself
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})

		// Act
		u := sqrl.Except(q1, q2).RunWith(db).PlaceholderFormat(phf())
		names := unionQueryStrings(t, u)

		// Assert
		assert.Empty(t, names)
	})
}

// ---------------------------------------------------------------------------
// Chaining multiple set operations
// ---------------------------------------------------------------------------

func TestUnionChaining(t *testing.T) {
	t.Run("UnionThenUnionAll", func(t *testing.T) {
		// Arrange
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})
		q3 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act — UNION q1,q2 then UNION ALL q3 (q3 duplicates q1)
		u := sqrl.Union(q1, q2).UnionAll(q3).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
		names := unionQueryStrings(t, u)

		// Assert — apple from q1 + banana from q2 + apple from q3
		assert.Equal(t, []string{"apple", "apple", "banana"}, names)
	})

	t.Run("UnionThenIntersect", func(t *testing.T) {
		// Arrange — create helper tables to avoid ambiguity
		createTable(t, "sq_un_chain_a", "(val TEXT)")
		createTable(t, "sq_un_chain_b", "(val TEXT)")
		createTable(t, "sq_un_chain_c", "(val TEXT)")
		seedTable(t, "INSERT INTO sq_un_chain_a VALUES ('x'), ('y')")
		seedTable(t, "INSERT INTO sq_un_chain_b VALUES ('y'), ('z')")
		seedTable(t, "INSERT INTO sq_un_chain_c VALUES ('y')")

		qA := sb.Select("val").From("sq_un_chain_a")
		qB := sb.Select("val").From("sq_un_chain_b")
		qC := sb.Select("val").From("sq_un_chain_c")

		// Act — (A UNION B) INTERSECT C
		u := sqrl.Union(qA, qB).Intersect(qC).RunWith(db).PlaceholderFormat(phf())
		vals := unionQueryStrings(t, u)

		// Assert — depends on database-specific precedence, but 'y' should be in result
		assert.Contains(t, vals, "y")
	})

	t.Run("UnionThenExcept", func(t *testing.T) {
		// Arrange
		createTable(t, "sq_un_chain_d", "(val TEXT)")
		createTable(t, "sq_un_chain_e", "(val TEXT)")
		createTable(t, "sq_un_chain_f", "(val TEXT)")
		seedTable(t, "INSERT INTO sq_un_chain_d VALUES ('a'), ('b')")
		seedTable(t, "INSERT INTO sq_un_chain_e VALUES ('c')")
		seedTable(t, "INSERT INTO sq_un_chain_f VALUES ('a')")

		qD := sb.Select("val").From("sq_un_chain_d")
		qE := sb.Select("val").From("sq_un_chain_e")
		qF := sb.Select("val").From("sq_un_chain_f")

		// Act — (D UNION E) EXCEPT F => {a,b,c} - {a} = {b,c}
		u := sqrl.Union(qD, qE).Except(qF).RunWith(db).PlaceholderFormat(phf()).OrderBy("val")
		vals := unionQueryStrings(t, u)

		// Assert
		assert.Equal(t, []string{"b", "c"}, vals)
	})
}

// ---------------------------------------------------------------------------
// ORDER BY on combined result
// ---------------------------------------------------------------------------

func TestUnionOrderBy(t *testing.T) {
	t.Run("Ascending", func(t *testing.T) {
		// Arrange
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 3})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act
		u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name ASC")
		names := unionQueryStrings(t, u)

		// Assert
		assert.Equal(t, []string{"apple", "carrot"}, names)
	})

	t.Run("Descending", func(t *testing.T) {
		// Arrange
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 3})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act
		u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name DESC")
		names := unionQueryStrings(t, u)

		// Assert
		assert.Equal(t, []string{"carrot", "apple"}, names)
	})

	t.Run("MultipleOrderBy", func(t *testing.T) {
		// Arrange — price ordering among categories
		q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})

		// Act — order by name
		u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name ASC")
		names := unionQueryStrings(t, u)

		// Assert
		assert.Equal(t, []string{"apple", "banana", "carrot", "eggplant"}, names)
	})
}

// ---------------------------------------------------------------------------
// LIMIT and OFFSET on combined result
// ---------------------------------------------------------------------------

func TestUnionLimit(t *testing.T) {
	// Arrange
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})

	// Act
	u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		OrderBy("name ASC").
		Limit(2)
	names := unionQueryStrings(t, u)

	// Assert — only first 2 of {apple, banana, carrot, eggplant}
	assert.Equal(t, []string{"apple", "banana"}, names)
}

func TestUnionOffset(t *testing.T) {
	// Arrange
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})

	// Act
	u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		OrderBy("name ASC").
		Limit(2).
		Offset(1)
	names := unionQueryStrings(t, u)

	// Assert — skip 1, take 2 from {apple, banana, carrot, eggplant}
	assert.Equal(t, []string{"banana", "carrot"}, names)
}

func TestUnionRemoveLimitOffset(t *testing.T) {
	// Arrange
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})

	// Act — set then remove
	u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		OrderBy("name ASC").
		Limit(1).
		Offset(1).
		RemoveLimit().
		RemoveOffset()
	names := unionQueryStrings(t, u)

	// Assert — all rows returned (limit and offset removed)
	assert.Equal(t, []string{"apple", "banana", "carrot", "eggplant"}, names)
}

// ---------------------------------------------------------------------------
// Prefix / Suffix
// ---------------------------------------------------------------------------

func TestUnionPrefix(t *testing.T) {
	// Arrange — use a CTE prefix (standard SQL with clause)
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})

	// Act — prefix should not break the query
	u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		Prefix("/* union query */").
		OrderBy("name")
	names := unionQueryStrings(t, u)

	// Assert
	assert.Equal(t, []string{"apple", "banana"}, names)
}

func TestUnionSuffix(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support a trailing comment after UNION easily")
	}

	// Arrange
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})

	// Act — add a harmless suffix
	u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		OrderBy("name").
		Suffix("/* end */")
	names := unionQueryStrings(t, u)

	// Assert
	assert.Equal(t, []string{"apple", "banana"}, names)
}

// ---------------------------------------------------------------------------
// Placeholder formats
// ---------------------------------------------------------------------------

func TestUnionPlaceholderFormat(t *testing.T) {
	t.Run("Question", func(t *testing.T) {
		// Arrange
		q1 := sqrl.Select("name").From("sq_items").Where("id = ?", 1)
		q2 := sqrl.Select("name").From("sq_items").Where("id = ?", 2)

		// Act
		sqlStr, args, err := sqrl.Union(q1, q2).PlaceholderFormat(sqrl.Question).ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "id = ?")
		assert.Equal(t, []interface{}{1, 2}, args)
	})

	t.Run("Dollar", func(t *testing.T) {
		// Arrange
		q1 := sqrl.Select("name").From("sq_items").Where("id = ?", 1)
		q2 := sqrl.Select("name").From("sq_items").Where("id = ?", 2)

		// Act
		sqlStr, args, err := sqrl.Union(q1, q2).PlaceholderFormat(sqrl.Dollar).ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "id = $1")
		assert.Contains(t, sqlStr, "id = $2")
		assert.Equal(t, []interface{}{1, 2}, args)
	})

	t.Run("Colon", func(t *testing.T) {
		// Arrange
		q1 := sqrl.Select("name").From("sq_items").Where("id = ?", 1)
		q2 := sqrl.Select("name").From("sq_items").Where("id = ?", 2)

		// Act
		sqlStr, args, err := sqrl.Union(q1, q2).PlaceholderFormat(sqrl.Colon).ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "id = :1")
		assert.Contains(t, sqlStr, "id = :2")
		assert.Equal(t, []interface{}{1, 2}, args)
	})

	t.Run("AtP", func(t *testing.T) {
		// Arrange
		q1 := sqrl.Select("name").From("sq_items").Where("id = ?", 1)
		q2 := sqrl.Select("name").From("sq_items").Where("id = ?", 2)

		// Act
		sqlStr, args, err := sqrl.Union(q1, q2).PlaceholderFormat(sqrl.AtP).ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "id = @p1")
		assert.Contains(t, sqlStr, "id = @p2")
		assert.Equal(t, []interface{}{1, 2}, args)
	})

	t.Run("NestedDollarPlaceholdersNumberSequentially", func(t *testing.T) {
		// Arrange — inner selects set Dollar; outer also Dollar; placeholders renumber correctly
		q1 := sqrl.Select("name").From("sq_items").Where("id = ?", 1).PlaceholderFormat(sqrl.Dollar)
		q2 := sqrl.Select("name").From("sq_items").Where("id = ?", 2).PlaceholderFormat(sqrl.Dollar)
		q3 := sqrl.Select("name").From("sq_items").Where("id = ?", 3).PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := sqrl.UnionAll(q1, q2, q3).PlaceholderFormat(sqrl.Dollar).ToSQL()

		// Assert — placeholders should be $1, $2, $3 (not $1, $1, $1)
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "$1")
		assert.Contains(t, sqlStr, "$2")
		assert.Contains(t, sqlStr, "$3")
		assert.Equal(t, []interface{}{1, 2, 3}, args)
	})
}

// ---------------------------------------------------------------------------
// Exec method
// ---------------------------------------------------------------------------

func TestUnionExec(t *testing.T) {
	// Arrange — Exec on a union (fires query but discards result set)
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})

	// Act
	_, err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).Exec()

	// Assert
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// QueryRow / Scan
// ---------------------------------------------------------------------------

func TestUnionQueryRow(t *testing.T) {
	// Arrange — query that returns exactly one row
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act — UNION deduplicates, so only one row
	var name string
	err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).Scan(&name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

func TestUnionScanMultipleColumns(t *testing.T) {
	// Arrange
	q1 := sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	var id int
	var name string
	err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).Scan(&id, &name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "apple", name)
}

// ---------------------------------------------------------------------------
// Error paths — ToSQL
// ---------------------------------------------------------------------------

func TestUnionToSQLError(t *testing.T) {
	t.Run("NoParts", func(t *testing.T) {
		// Arrange
		u := sqrl.UnionBuilder{}.PlaceholderFormat(sqrl.Question)

		// Act
		_, _, err := u.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one part")
	})

	t.Run("MustSQLPanicsOnError", func(t *testing.T) {
		// Arrange
		u := sqrl.UnionBuilder{}.PlaceholderFormat(sqrl.Question)

		// Act & Assert
		assert.Panics(t, func() {
			u.MustSQL()
		})
	})
}

// ---------------------------------------------------------------------------
// Error paths — no runner
// ---------------------------------------------------------------------------

func TestUnionNoRunner(t *testing.T) {
	q1 := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})

	u := sqrl.Union(q1, q2) // no RunWith

	t.Run("Exec", func(t *testing.T) {
		_, err := u.Exec()
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("Query", func(t *testing.T) {
		_, err := u.Query()
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("QueryRow", func(t *testing.T) {
		row := u.QueryRow()
		err := row.Scan()
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("Scan", func(t *testing.T) {
		var v string
		err := u.Scan(&v)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}

// ---------------------------------------------------------------------------
// Error paths — context, no runner
// ---------------------------------------------------------------------------

func TestUnionContextNoRunner(t *testing.T) {
	ctx := context.Background()
	q1 := sqrl.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	u := sqrl.Union(q1) // no RunWith

	t.Run("ExecContext", func(t *testing.T) {
		_, err := u.ExecContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("QueryContext", func(t *testing.T) {
		_, err := u.QueryContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("ScanContext", func(t *testing.T) {
		var v string
		err := u.ScanContext(ctx, &v)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}

// ---------------------------------------------------------------------------
// Context methods — happy path
// ---------------------------------------------------------------------------

func TestUnionQueryContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})

	// Act
	rows, err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		OrderBy("name").QueryContext(ctx)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	require.NoError(t, rows.Err())

	// Assert
	assert.Equal(t, []string{"apple", "banana"}, names)
}

func TestUnionExecContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})

	// Act
	_, err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).ExecContext(ctx)

	// Assert
	assert.NoError(t, err)
}

func TestUnionScanContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	var name string
	err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).ScanContext(ctx, &name)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "apple", name)
}

func TestUnionQueryRowContext(t *testing.T) {
	// Arrange
	ctx := context.Background()
	q1 := sb.Select("COUNT(*)").From("sq_items").Where(sqrl.Eq{"category": "fruit"})

	// Act
	var count int
	err := sqrl.Union(q1).RunWith(db).PlaceholderFormat(phf()).
		QueryRowContext(ctx).Scan(&count)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// ---------------------------------------------------------------------------
// Context methods — cancelled context
// ---------------------------------------------------------------------------

func TestUnionQueryContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q1 := sb.Select("name").From("sq_items")
	q2 := sb.Select("name").From("sq_items")

	// Act
	_, err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).QueryContext(ctx)

	// Assert
	assert.Error(t, err)
}

func TestUnionExecContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q1 := sb.Select("name").From("sq_items")
	q2 := sb.Select("name").From("sq_items")

	// Act
	_, err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).ExecContext(ctx)

	// Assert
	assert.Error(t, err)
}

func TestUnionScanContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q1 := sb.Select("name").From("sq_items")

	// Act
	var name string
	err := sqrl.Union(q1).RunWith(db).PlaceholderFormat(phf()).ScanContext(ctx, &name)

	// Assert
	assert.Error(t, err)
}

func TestUnionQueryRowContextCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q1 := sb.Select("name").From("sq_items")

	// Act
	var name string
	err := sqrl.Union(q1).RunWith(db).PlaceholderFormat(phf()).
		QueryRowContext(ctx).Scan(&name)

	// Assert
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Cross-table union
// ---------------------------------------------------------------------------

func TestUnionCrossTable(t *testing.T) {
	// Arrange — union data from sq_items and sq_categories (different tables)
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
	q2 := sb.Select("name").From("sq_categories").Where(sqrl.Eq{"name": "dairy"})

	// Act
	u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
	names := unionQueryStrings(t, u)

	// Assert
	assert.Equal(t, []string{"apple", "banana", "dairy"}, names)
}

// ---------------------------------------------------------------------------
// WHERE with bound args
// ---------------------------------------------------------------------------

func TestUnionWithBoundArgs(t *testing.T) {
	// Arrange — multiple placeholders across both sides
	q1 := sb.Select("name").From("sq_items").Where("price > ? AND price < ?", 50, 200)
	q2 := sb.Select("name").From("sq_items").Where("id = ?", 6)

	// Act
	u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
	names := unionQueryStrings(t, u)

	// Assert — price between (50, 200) => carrot(75), eggplant(150), apple(100); id=6 => mystery
	assert.Equal(t, []string{"apple", "carrot", "eggplant", "mystery"}, names)
}

// ---------------------------------------------------------------------------
// Immutability — builder methods return new copies
// ---------------------------------------------------------------------------

func TestUnionImmutability(t *testing.T) {
	// Arrange
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 2})
	q3 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 3})

	base := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf())

	// Act — add q3 to a derived builder
	derived := base.UnionAll(q3).OrderBy("name")

	baseNames := unionQueryStrings(t, base.OrderBy("name"))
	derivedNames := unionQueryStrings(t, derived)

	// Assert — base should NOT include q3
	assert.Equal(t, []string{"apple", "banana"}, baseNames)
	assert.Equal(t, []string{"apple", "banana", "carrot"}, derivedNames)
}

// ---------------------------------------------------------------------------
// Multiple columns in set operations
// ---------------------------------------------------------------------------

func TestUnionMultipleColumns(t *testing.T) {
	// Arrange
	q1 := sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"id": 1})
	q2 := sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"id": 2})

	// Act
	rows, err := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		OrderBy("id").Query()
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		id   int
		name string
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Assert
	assert.Equal(t, []row{
		{1, "apple"},
		{2, "banana"},
	}, results)
}

// ---------------------------------------------------------------------------
// Edge case: UNION ALL with identical rows and ORDER BY + LIMIT
// ---------------------------------------------------------------------------

func TestUnionAllWithOrderByAndLimitDuplicates(t *testing.T) {
	// Arrange — intentionally create duplicates
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"})

	// Act — UNION ALL (preserves dupes), order, limit
	u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf()).
		OrderBy("name ASC").
		Limit(3)
	names := unionQueryStrings(t, u)

	// Assert — {apple, banana} x2 ordered = [apple, apple, banana, banana], limit 3
	assert.Equal(t, []string{"apple", "apple", "banana"}, names)
}

// ---------------------------------------------------------------------------
// Edge case: sub-selects with different WHERE conditions and same columns
// ---------------------------------------------------------------------------

func TestUnionDifferentConditions(t *testing.T) {
	// Arrange
	q1 := sb.Select("name").From("sq_items").Where(sqrl.Gt{"price": 100})
	q2 := sb.Select("name").From("sq_items").Where(sqrl.Lt{"price": 60})

	// Act
	u := sqrl.Union(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("name")
	names := unionQueryStrings(t, u)

	// Assert — price > 100: donut(200), eggplant(150); price < 60: banana(50)
	assert.Equal(t, []string{"banana", "donut", "eggplant"}, names)
}

// ---------------------------------------------------------------------------
// Edge case: UNION with aggregate functions
// ---------------------------------------------------------------------------

func TestUnionWithAggregates(t *testing.T) {
	// Arrange
	q1 := sb.Select("COUNT(*)").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
	q2 := sb.Select("COUNT(*)").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})

	// Act
	u := sqrl.UnionAll(q1, q2).RunWith(db).PlaceholderFormat(phf()).OrderBy("1")
	counts := unionQueryInts(t, u)

	// Assert — 2 fruits, 2 vegetables
	assert.Equal(t, []int{2, 2}, counts)
}

// ---------------------------------------------------------------------------
// Edge case: UNION with literal values (no real table)
// ---------------------------------------------------------------------------

func TestUnionLiteralValues(t *testing.T) {
	// Arrange — synthetic values, no table scan
	q1 := sb.Select("1 as val")
	q2 := sb.Select("2 as val")
	q3 := sb.Select("3 as val")

	// Act
	u := sqrl.UnionAll(q1, q2, q3).RunWith(db).PlaceholderFormat(phf()).OrderBy("1")
	vals := unionQueryInts(t, u)

	// Assert
	assert.Equal(t, []int{1, 2, 3}, vals)
}
