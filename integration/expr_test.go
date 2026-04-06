package integration

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// Eq
// ---------------------------------------------------------------------------

func TestExprEq(t *testing.T) {
	t.Run("SingleValue", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("SliceIN", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": []int{1, 3, 5}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "carrot", "eggplant"}, names)
	})

	t.Run("EmptySlice", func(t *testing.T) {
		// Arrange — Eq with empty slice evaluates to (1=0) → no rows
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": []int{}})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Empty(t, names)
	})

	t.Run("NilIsNull", func(t *testing.T) {
		// Arrange — row 6 has NULL category
		q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": nil})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"mystery"}, names)
	})

	t.Run("EmptyEqIsTrue", func(t *testing.T) {
		// Arrange — empty Eq{} evaluates to (1=1), matching all rows
		q := sb.Select("name").From("sq_items").Where(sqrl.Eq{}).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Len(t, names, 6)
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		// Arrange — Eq with multiple keys produces AND
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": "fruit", "price": 50})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"banana"}, names)
	})
}

// ---------------------------------------------------------------------------
// NotEq
// ---------------------------------------------------------------------------

func TestExprNotEq(t *testing.T) {
	t.Run("SingleValue", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"id": 1}).
			Where(sqrl.NotEq{"category": nil}). // exclude NULL category
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"banana", "carrot", "donut", "eggplant"}, names)
	})

	t.Run("SliceNotIN", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"id": []int{1, 2, 3}}).
			Where(sqrl.NotEq{"category": nil}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"donut", "eggplant"}, names)
	})

	t.Run("EmptySlice", func(t *testing.T) {
		// Arrange — NotEq with empty slice evaluates to (1=1) → all rows
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"id": []int{}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Len(t, names, 6)
	})

	t.Run("NilIsNotNull", func(t *testing.T) {
		// Arrange — NotEq nil → IS NOT NULL
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — all except 'mystery'
		assert.Len(t, names, 5)
	})
}

// ---------------------------------------------------------------------------
// Lt / Gt / LtOrEq / GtOrEq
// ---------------------------------------------------------------------------

func TestExprComparisons(t *testing.T) {
	t.Run("Lt", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Lt{"price": 75}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50)
		assert.Equal(t, []string{"banana"}, names)
	})

	t.Run("LtOrEq", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.LtOrEq{"price": 75}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50), carrot(75)
		assert.Equal(t, []string{"banana", "carrot"}, names)
	})

	t.Run("Gt", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Gt{"price": 150}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — donut(200)
		assert.Equal(t, []string{"donut"}, names)
	})

	t.Run("GtOrEq", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.GtOrEq{"price": 150}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — donut(200), eggplant(150)
		assert.Equal(t, []string{"donut", "eggplant"}, names)
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		// Arrange — Lt with multiple keys: price < 200 AND id < 4
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Lt{"id": 4, "price": 200}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple(1,100), banana(2,50), carrot(3,75)
		assert.Equal(t, []string{"apple", "banana", "carrot"}, names)
	})
}

// ---------------------------------------------------------------------------
// Lt/Gt error paths
// ---------------------------------------------------------------------------

func TestExprComparisonErrors(t *testing.T) {
	t.Run("LtNil", func(t *testing.T) {
		// Arrange
		expr := sqrl.Lt{"id": nil}

		// Act
		_, _, err := expr.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use null")
	})

	t.Run("GtSlice", func(t *testing.T) {
		// Arrange
		expr := sqrl.Gt{"id": []int{1, 2}}

		// Act
		_, _, err := expr.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use array or slice")
	})
}

// ---------------------------------------------------------------------------
// Like / NotLike
// ---------------------------------------------------------------------------

func TestExprLike(t *testing.T) {
	t.Run("Like", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Like{"name": "a%"}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("LikeWildcard", func(t *testing.T) {
		// Arrange — names starting with 'ba'
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Like{"name": "ba%"}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"banana"}, names)
	})

	t.Run("NotLike", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotLike{"name": "a%"}).
			Where(sqrl.NotEq{"category": nil}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — everything except apple (and mystery filtered by NotEq)
		assert.Equal(t, []string{"banana", "carrot", "donut", "eggplant"}, names)
	})
}

func TestExprLikeErrors(t *testing.T) {
	t.Run("NilValue", func(t *testing.T) {
		// Arrange
		expr := sqrl.Like{"name": nil}

		// Act
		_, _, err := expr.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use null with like")
	})

	t.Run("SliceValue", func(t *testing.T) {
		// Arrange
		expr := sqrl.Like{"name": []string{"a", "b"}}

		// Act
		_, _, err := expr.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use array or slice with like")
	})
}

// ---------------------------------------------------------------------------
// And / Or
// ---------------------------------------------------------------------------

func TestExprAndOr(t *testing.T) {
	t.Run("And", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{
				sqrl.Eq{"category": "fruit"},
				sqrl.Gt{"price": 60},
			})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("Or", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.Eq{"category": "pastry"},
				sqrl.Lt{"price": 60},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50, fruit), donut(200, pastry)
		assert.Equal(t, []string{"banana", "donut"}, names)
	})

	t.Run("NestedAndOr", func(t *testing.T) {
		// Arrange — (category='fruit' OR category='pastry') AND price > 60
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{
				sqrl.Or{
					sqrl.Eq{"category": "fruit"},
					sqrl.Eq{"category": "pastry"},
				},
				sqrl.Gt{"price": 60},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple(100, fruit), donut(200, pastry)
		assert.Equal(t, []string{"apple", "donut"}, names)
	})

	t.Run("EmptyAnd", func(t *testing.T) {
		// Arrange — empty And{} produces no WHERE clause, matching all rows
		q := sb.Select("name").From("sq_items").Where(sqrl.And{}).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Len(t, names, 6)
	})

	t.Run("EmptyOr", func(t *testing.T) {
		// Arrange — empty Or{} produces no WHERE clause, matching all rows (GitHub #382 fix)
		q := sb.Select("name").From("sq_items").Where(sqrl.Or{}).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — all rows returned, not zero
		assert.Len(t, names, 6)
	})

	t.Run("NilOrInWhere", func(t *testing.T) {
		// Arrange — GitHub #382: nil Or in Where should produce no WHERE clause
		var filters sqrl.Or
		q := sb.Select("name").From("sq_items").Where(filters).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — all rows returned
		assert.Len(t, names, 6)
	})

	t.Run("NilAndInWhere", func(t *testing.T) {
		// Arrange — GitHub #382: nil And in Where should produce no WHERE clause
		var filters sqrl.And
		q := sb.Select("name").From("sq_items").Where(filters).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — all rows returned
		assert.Len(t, names, 6)
	})

	t.Run("NilOrFollowedByCondition", func(t *testing.T) {
		// Arrange — GitHub #382: nil Or followed by a real condition
		var filters sqrl.Or
		q := sb.Select("name").From("sq_items").
			Where(filters).
			Where(sqrl.Eq{"category": "fruit"}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — only fruit items
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("ConditionFollowedByNilOr", func(t *testing.T) {
		// Arrange — real condition followed by nil Or
		var filters sqrl.Or
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": "fruit"}).
			Where(filters).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — only fruit items
		assert.Equal(t, []string{"apple", "banana"}, names)
	})
}

// ---------------------------------------------------------------------------
// Expr
// ---------------------------------------------------------------------------

func TestExprFunction(t *testing.T) {
	t.Run("SimpleExpression", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Expr("price BETWEEN ? AND ?", 70, 110)).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — carrot(75), apple(100), mystery(99)
		assert.Equal(t, []string{"apple", "carrot", "mystery"}, names)
	})

	t.Run("ExprWithNestedSqlizer", func(t *testing.T) {
		// Arrange — use a subquery as an Expr argument, wrapped in parentheses
		sub := sqrl.Select("MAX(price)").From("sq_items")
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Expr("price = (?)", sub))

		// Act
		names := queryStrings(t, q)

		// Assert — donut has max price 200
		assert.Equal(t, []string{"donut"}, names)
	})

	t.Run("ExprNoArgs", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Expr("price > 100")).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"donut", "eggplant"}, names)
	})
}

// ---------------------------------------------------------------------------
// ConcatExpr
// ---------------------------------------------------------------------------

func TestConcatExpr(t *testing.T) {
	// Arrange — build a WHERE clause by concatenating parts
	q := sb.Select("name").From("sq_items").
		Where(sqrl.ConcatExpr("price > ", sqrl.Expr("?", 100))).
		OrderBy("id")

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"donut", "eggplant"}, names)
}

// ---------------------------------------------------------------------------
// Alias
// ---------------------------------------------------------------------------

func TestAlias(t *testing.T) {
	// Arrange — alias a subquery in FROM clause
	inner := sqrl.Select("name", "price").From("sq_items").Where(sqrl.Gt{"price": 100})
	q := sb.Select("name").FromSelect(inner, "expensive").OrderBy("name")

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"donut", "eggplant"}, names)
}

// ---------------------------------------------------------------------------
// Eq with pointer
// ---------------------------------------------------------------------------

func TestExprEqPointer(t *testing.T) {
	t.Run("NonNilPointer", func(t *testing.T) {
		// Arrange
		val := 1
		q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": &val})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("NilPointer", func(t *testing.T) {
		// Arrange — nil pointer treated as IS NULL
		var val *string
		q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": val})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"mystery"}, names)
	})
}

// ---------------------------------------------------------------------------
// Eq with sql.NullString (driver.Valuer)
// ---------------------------------------------------------------------------

func TestExprEqDriverValuer(t *testing.T) {
	t.Run("ValidValue", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": sql.NullString{String: "fruit", Valid: true}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("NullValue", func(t *testing.T) {
		// Arrange — invalid NullString is treated as NULL
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": sql.NullString{}})

		// Act
		names := queryStrings(t, q)

		// Assert — row with NULL category
		assert.Equal(t, []string{"mystery"}, names)
	})
}

// ---------------------------------------------------------------------------
// ILike / NotILike — ToSQL only (PostgreSQL-specific syntax)
// ---------------------------------------------------------------------------

func TestExprILikeToSQL(t *testing.T) {
	t.Run("ILike", func(t *testing.T) {
		// Arrange
		expr := sqrl.ILike{"name": "sq%"}

		// Act
		sqlStr, args, err := expr.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "name ILIKE ?", sqlStr)
		assert.Equal(t, []interface{}{"sq%"}, args)
	})

	t.Run("NotILike", func(t *testing.T) {
		// Arrange
		expr := sqrl.NotILike{"name": "sq%"}

		// Act
		sqlStr, args, err := expr.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "name NOT ILIKE ?", sqlStr)
		assert.Equal(t, []interface{}{"sq%"}, args)
	})
}

// ---------------------------------------------------------------------------
// ConcatExpr — additional
// ---------------------------------------------------------------------------

func TestConcatExprAdditional(t *testing.T) {
	t.Run("InvalidTypeError", func(t *testing.T) {
		// Arrange — 42 is neither string nor Sqlizer
		expr := sqrl.ConcatExpr("prefix ", 42, " suffix")

		// Act
		_, _, err := expr.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a string or Sqlizer")
	})

	t.Run("MultipleStringParts", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.ConcatExpr("price", " > ", sqrl.Expr("?", 100))).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"donut", "eggplant"}, names)
	})

	t.Run("AllStrings", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.ConcatExpr("id", " = ", "1"))

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})
}

// ---------------------------------------------------------------------------
// Expr with escaped ??
// ---------------------------------------------------------------------------

func TestExprEscapedPlaceholder(t *testing.T) {
	// Arrange — ?? is escaped to a literal ? in the SQL
	expr := sqrl.Expr("data->>?? = ?", "active")

	// Act
	sqlStr, args, err := expr.ToSQL()

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "data->>?? = ?", sqlStr)
	assert.Equal(t, []interface{}{"active"}, args)
}

// ---------------------------------------------------------------------------
// And / Or — additional
// ---------------------------------------------------------------------------

func TestExprAndOrAdditional(t *testing.T) {
	t.Run("AndSingleElement", func(t *testing.T) {
		// Arrange — And with one element just wraps it in parentheses
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{sqrl.Eq{"id": 1}})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("OrSingleElement", func(t *testing.T) {
		// Arrange — Or with one element
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{sqrl.Eq{"id": 1}})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("DeeplyNested", func(t *testing.T) {
		// Arrange — (id=1 OR id=2) AND (category='fruit')
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{
				sqrl.Or{
					sqrl.Eq{"id": 1},
					sqrl.Eq{"id": 2},
				},
				sqrl.Eq{"category": "fruit"},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana"}, names)
	})
}

// ---------------------------------------------------------------------------
// Eq — additional edge cases
// ---------------------------------------------------------------------------

func TestExprEqAdditional(t *testing.T) {
	t.Run("StringSliceIN", func(t *testing.T) {
		// Arrange — Eq with string slice
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": []string{"fruit", "pastry"}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana", "donut"}, names)
	})

	t.Run("SingleElementSlice", func(t *testing.T) {
		// Arrange — slice with one element
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": []int{3}})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"carrot"}, names)
	})
}

// ---------------------------------------------------------------------------
// NotEq — additional edge cases
// ---------------------------------------------------------------------------

func TestExprNotEqAdditional(t *testing.T) {
	t.Run("MultiKeyWithNil", func(t *testing.T) {
		// Arrange — NotEq with multiple keys, one being nil
		sqlStr, _, err := sqrl.NotEq{"a": 1, "b": nil}.ToSQL()

		// Assert — should contain both conditions
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "a <> ?")
		assert.Contains(t, sqlStr, "b IS NOT NULL")
	})
}

// ---------------------------------------------------------------------------
// Like — additional
// ---------------------------------------------------------------------------

func TestExprLikeAdditional(t *testing.T) {
	t.Run("LikeEndsWithPattern", func(t *testing.T) {
		// Arrange — LIKE '%nut'
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Like{"name": "%nut"}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"donut"}, names)
	})

	t.Run("LikeExactMatch", func(t *testing.T) {
		// Arrange — LIKE 'apple' (no wildcards)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Like{"name": "apple"})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})
}

// ---------------------------------------------------------------------------
// Subqueries in expression position (Eq / NotEq)
// ---------------------------------------------------------------------------

func TestExprEqSubquery(t *testing.T) {
	t.Run("EqSubqueryIN", func(t *testing.T) {
		// Arrange — WHERE id IN (SELECT id FROM sq_items WHERE category = 'fruit')
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple(1,fruit), banana(2,fruit)
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("NotEqSubqueryNOTIN", func(t *testing.T) {
		// Arrange — WHERE id NOT IN (SELECT id FROM sq_items WHERE category = 'fruit')
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"id": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — carrot, donut, eggplant, mystery (all non-fruit)
		assert.Equal(t, []string{"carrot", "donut", "eggplant", "mystery"}, names)
	})

	t.Run("EqSubqueryNoRows", func(t *testing.T) {
		// Arrange — subquery returns no rows → IN (empty) → no matches
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "nonexistent"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": sub})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Empty(t, names)
	})

	t.Run("EqSubqueryAllRows", func(t *testing.T) {
		// Arrange — subquery returns all IDs → all rows match
		sub := sqrl.Select("id").From("sq_items")
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Len(t, names, 6)
	})

	t.Run("EqSubqueryWithMultipleConditions", func(t *testing.T) {
		// Arrange — WHERE category = 'vegetable' AND id IN (SELECT id FROM sq_items WHERE price > 100)
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Gt{"price": 100})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": "vegetable"}).
			Where(sqrl.Eq{"id": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — eggplant (vegetable, price 150 > 100)
		assert.Equal(t, []string{"eggplant"}, names)
	})

	t.Run("EqSubqueryMixedLiteralAndSubquery", func(t *testing.T) {
		// Arrange — Eq with both a literal value and a subquery value
		// WHERE category = 'vegetable' AND id IN (SELECT id FROM sq_items WHERE price >= 100)
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.GtOrEq{"price": 100})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": "vegetable", "id": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — eggplant (vegetable, price 150)
		assert.Equal(t, []string{"eggplant"}, names)
	})

	t.Run("EqSubqueryFromDifferentTable", func(t *testing.T) {
		// Arrange — cross-table: WHERE category IN (SELECT name FROM sq_categories WHERE name <> 'dairy')
		sub := sqrl.Select("name").From("sq_categories").Where(sqrl.NotEq{"name": "dairy"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple, banana (fruit), carrot, eggplant (vegetable), donut (pastry)
		assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant"}, names)
	})

	t.Run("EqSubqueryNestedSubqueries", func(t *testing.T) {
		// Arrange — nested: WHERE id IN (SELECT id FROM sq_items WHERE category IN (SELECT name FROM sq_categories WHERE description LIKE '%Fresh%'))
		innerSub := sqrl.Select("name").From("sq_categories").Where(sqrl.Like{"description": "%Fresh%"})
		outerSub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": innerSub})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": outerSub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — fruit and vegetable categories have 'Fresh' in description
		assert.Equal(t, []string{"apple", "banana", "carrot", "eggplant"}, names)
	})

	t.Run("NotEqSubqueryFromDifferentTable", func(t *testing.T) {
		// Arrange — WHERE category NOT IN (SELECT name FROM sq_categories WHERE name = 'fruit')
		sub := sqrl.Select("name").From("sq_categories").Where(sqrl.Eq{"name": "fruit"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"category": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — carrot (vegetable), donut (pastry), eggplant (vegetable)
		// mystery has NULL category so NOT IN doesn't include it (NULL comparison)
		assert.Equal(t, []string{"carrot", "donut", "eggplant"}, names)
	})

	t.Run("EqSubqueryWithAndOr", func(t *testing.T) {
		// Arrange — combine subquery in Eq with And/Or
		// WHERE (id IN (subquery) OR category = 'pastry') AND price > 60
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{
				sqrl.Or{
					sqrl.Eq{"id": sub},
					sqrl.Eq{"category": "pastry"},
				},
				sqrl.Gt{"price": 60},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple (fruit, 100), donut (pastry, 200)
		assert.Equal(t, []string{"apple", "donut"}, names)
	})

	t.Run("EqSubqueryInExprFunction", func(t *testing.T) {
		// Arrange — use Expr with subquery for manual IN syntax
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Expr("id IN (?)", sub)).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"carrot", "eggplant"}, names)
	})
}

// ---------------------------------------------------------------------------
// Subqueries in expression position (Lt / Gt / LtOrEq / GtOrEq)
// ---------------------------------------------------------------------------

func TestExprComparisonSubquery(t *testing.T) {
	t.Run("LtScalarSubquery", func(t *testing.T) {
		// Arrange — WHERE price < (SELECT AVG(price) FROM sq_items WHERE category IS NOT NULL)
		// AVG of non-null: (100+50+75+200+150)/5 = 115
		sub := sqrl.Select("AVG(price)").From("sq_items").Where(sqrl.NotEq{"category": nil})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Lt{"price": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50), carrot(75), mystery(99), apple(100) — all below AVG of 115
		assert.Equal(t, []string{"apple", "banana", "carrot", "mystery"}, names)
	})

	t.Run("GtScalarSubquery", func(t *testing.T) {
		// Arrange — WHERE price > (SELECT AVG(price) FROM sq_items WHERE category IS NOT NULL)
		// AVG = 115
		sub := sqrl.Select("AVG(price)").From("sq_items").Where(sqrl.NotEq{"category": nil})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Gt{"price": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — eggplant(150), donut(200)
		assert.Equal(t, []string{"donut", "eggplant"}, names)
	})

	t.Run("LtOrEqScalarSubquery", func(t *testing.T) {
		// Arrange — WHERE price <= (SELECT MIN(price) FROM sq_items WHERE category = 'fruit')
		// MIN of fruit = 50 (banana)
		sub := sqrl.Select("MIN(price)").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.LtOrEq{"price": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50)
		assert.Equal(t, []string{"banana"}, names)
	})

	t.Run("GtOrEqScalarSubquery", func(t *testing.T) {
		// Arrange — WHERE price >= (SELECT MAX(price) FROM sq_items WHERE category = 'vegetable')
		// MAX of vegetable = 150 (eggplant)
		sub := sqrl.Select("MAX(price)").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.GtOrEq{"price": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — eggplant(150), donut(200)
		assert.Equal(t, []string{"donut", "eggplant"}, names)
	})

	t.Run("GtSubqueryWithArgs", func(t *testing.T) {
		// Arrange — WHERE price > (SELECT AVG(price) FROM sq_items WHERE category = 'fruit')
		// AVG of fruit = (100+50)/2 = 75
		sub := sqrl.Select("AVG(price)").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Gt{"price": sub}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple(100), donut(200), eggplant(150), mystery(99) — all above AVG of fruit (75), ordered by id
		assert.Equal(t, []string{"apple", "donut", "eggplant", "mystery"}, names)
	})

	t.Run("LtSubqueryMixedWithLiteralWhere", func(t *testing.T) {
		// Arrange — WHERE price < (scalar subquery) AND category = 'fruit'
		sub := sqrl.Select("AVG(price)").From("sq_items")
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Lt{"price": sub}).
			Where(sqrl.Eq{"category": "fruit"}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — overall AVG ≈ 112; fruit items below that: banana(50), apple(100)
		assert.Equal(t, []string{"apple", "banana"}, names)
	})
}

// ---------------------------------------------------------------------------
// Subqueries in expression position — placeholder correctness
// ---------------------------------------------------------------------------

func TestExprSubqueryPlaceholders(t *testing.T) {
	t.Run("SubqueryToSQLPlaceholderQuestion", func(t *testing.T) {
		// Arrange — verify SQL generation with Question format
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": sub}).
			PlaceholderFormat(sqrl.Question)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE id IN (SELECT id FROM sq_items WHERE category = ?)", sqlStr)
		assert.Equal(t, []interface{}{"fruit"}, args)
	})

	t.Run("SubqueryToSQLPlaceholderDollar", func(t *testing.T) {
		// Arrange — verify correct Dollar placeholder numbering across outer + inner
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Eq{"price": 100}).
			Where(sqrl.Eq{"id": sub}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE price = $1 AND id IN (SELECT id FROM sq_items WHERE category = $2)", sqlStr)
		assert.Equal(t, []interface{}{100, "fruit"}, args)
	})

	t.Run("MultipleSubqueriesDollarNumbering", func(t *testing.T) {
		// Arrange — two subqueries with Dollar placeholders
		sub1 := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		sub2 := sqrl.Select("MAX(price)").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": sub1}).
			Where(sqrl.Lt{"price": sub2}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert — placeholders should be $1 and $2
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE id IN (SELECT id FROM sq_items WHERE category = $1) AND price < (SELECT MAX(price) FROM sq_items WHERE category = $2)", sqlStr)
		assert.Equal(t, []interface{}{"fruit", "vegetable"}, args)
	})

	t.Run("NestedSubqueriesDollarNumbering", func(t *testing.T) {
		// Arrange — nested subqueries with Dollar placeholders
		innerSub := sqrl.Select("name").From("sq_categories").Where(sqrl.Eq{"description": "Fresh fruits"})
		outerSub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": innerSub})
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Eq{"price": 100}).
			Where(sqrl.Eq{"id": outerSub}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert — $1 for outer literal, $2 for inner subquery arg
		require.NoError(t, err)
		assert.Equal(t,
			"SELECT name FROM sq_items WHERE price = $1 AND id IN (SELECT id FROM sq_items WHERE category IN (SELECT name FROM sq_categories WHERE description = $2))",
			sqlStr)
		assert.Equal(t, []interface{}{100, "Fresh fruits"}, args)
	})

	t.Run("ComparisonSubqueryToSQLColon", func(t *testing.T) {
		// Arrange — Colon placeholder format
		sub := sqrl.Select("MAX(price)").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Eq{"price": 100}).
			Where(sqrl.Gt{"price": sub}).
			PlaceholderFormat(sqrl.Colon)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE price = :1 AND price > (SELECT MAX(price) FROM sq_items WHERE category = :2)", sqlStr)
		assert.Equal(t, []interface{}{100, "fruit"}, args)
	})

	t.Run("ComparisonSubqueryToSQLAtP", func(t *testing.T) {
		// Arrange — AtP placeholder format
		sub := sqrl.Select("MIN(price)").From("sq_items").Where(sqrl.Eq{"category": "vegetable"})
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.LtOrEq{"price": sub}).
			PlaceholderFormat(sqrl.AtP)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE price <= (SELECT MIN(price) FROM sq_items WHERE category = @p1)", sqlStr)
		assert.Equal(t, []interface{}{"vegetable"}, args)
	})
}

// ---------------------------------------------------------------------------
// Not
// ---------------------------------------------------------------------------

func TestExprNot(t *testing.T) {
	t.Run("NotEq", func(t *testing.T) {
		// Arrange — NOT (category = 'fruit') should exclude fruit items
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Eq{"category": "fruit"}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — carrot, donut, eggplant (mystery has NULL category, excluded by NOT (category = 'fruit'))
		assert.Equal(t, []string{"carrot", "donut", "eggplant"}, names)
	})

	t.Run("NotLike", func(t *testing.T) {
		// Arrange — NOT (name LIKE 'a%') should exclude apple
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Like{"name": "a%"}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"banana", "carrot", "donut", "eggplant", "mystery"}, names)
	})

	t.Run("NotOr", func(t *testing.T) {
		// Arrange — NOT (category = 'fruit' OR category = 'pastry')
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Or{
				sqrl.Eq{"category": "fruit"},
				sqrl.Eq{"category": "pastry"},
			}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — only vegetables remain (mystery has NULL, excluded by NOT)
		assert.Equal(t, []string{"carrot", "eggplant"}, names)
	})

	t.Run("NotCombinedWithAnd", func(t *testing.T) {
		// Arrange — category = 'fruit' AND NOT (price > 75)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{
				sqrl.Eq{"category": "fruit"},
				sqrl.Not{Cond: sqrl.Gt{"price": 75}},
			})

		// Act
		names := queryStrings(t, q)

		// Assert — banana (50) is the only fruit with price <= 75
		assert.Equal(t, []string{"banana"}, names)
	})

	t.Run("DoubleNot", func(t *testing.T) {
		// Arrange — NOT (NOT (category = 'pastry')) is equivalent to category = 'pastry'
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Not{Cond: sqrl.Eq{"category": "pastry"}}})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"donut"}, names)
	})

	t.Run("NotWithSubquery", func(t *testing.T) {
		// Arrange — NOT (id IN (SELECT id FROM sq_items WHERE category = 'fruit'))
		sub := sqrl.Select("id").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Eq{"id": sub}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — all non-fruit items
		assert.Equal(t, []string{"carrot", "donut", "eggplant", "mystery"}, names)
	})

	t.Run("NotNilCondProducesTrue", func(t *testing.T) {
		// Arrange — Not with nil condition should produce (1=1), returning all rows
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: nil}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — all 6 items
		assert.Len(t, names, 6)
	})

	t.Run("NotToSQLDollar", func(t *testing.T) {
		// Arrange — verify Not produces correct Dollar placeholders
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Eq{"price": 100}).
			Where(sqrl.Not{Cond: sqrl.Eq{"category": "pastry"}}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE price = $1 AND NOT (category = $2)", sqlStr)
		assert.Equal(t, []interface{}{100, "pastry"}, args)
	})

	t.Run("NotWithBetween", func(t *testing.T) {
		// Arrange — NOT (price BETWEEN 75 AND 150)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Between{"price": [2]interface{}{75, 150}}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50), donut(200) — same result as NotBetween
		assert.Equal(t, []string{"banana", "donut"}, names)
	})

	t.Run("NotWithGt", func(t *testing.T) {
		// Arrange — NOT (price > 100) keeps items with price <= 100
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Gt{"price": 100}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple(100), banana(50), carrot(75), mystery(99) — NOT(price > 100) means price <= 100
		assert.Equal(t, []string{"apple", "banana", "carrot", "mystery"}, names)
	})

	t.Run("NotWithExists", func(t *testing.T) {
		// Arrange — Not{Exists(sub)} should produce same results as NotExists(sub)
		sub := sqrl.Select("1").From("sq_items").Where("sq_items.category = sq_categories.name")
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Not{Cond: sqrl.Exists(sub)}).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — only dairy has no items (same as NotExists)
		assert.Equal(t, []string{"dairy"}, names)
	})

	t.Run("NotWithNotBetween", func(t *testing.T) {
		// Arrange — NOT (price NOT BETWEEN 75 AND 150) → price BETWEEN 75 AND 150
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.NotBetween{"price": [2]interface{}{75, 150}}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple(100,id=1), carrot(75,id=3), eggplant(150,id=5), mystery(99,id=6) — same as Between{75,150}
		assert.Equal(t, []string{"apple", "carrot", "eggplant", "mystery"}, names)
	})
}

// ---------------------------------------------------------------------------
// Between / NotBetween
// ---------------------------------------------------------------------------

func TestExprBetween(t *testing.T) {
	t.Run("SingleColumn", func(t *testing.T) {
		// Arrange — price BETWEEN 75 AND 150
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Between{"price": [2]interface{}{75, 150}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple(100,id=1), carrot(75,id=3), eggplant(150,id=5), mystery(99,id=6)
		assert.Equal(t, []string{"apple", "carrot", "eggplant", "mystery"}, names)
	})

	t.Run("ExcludesBoundaryCorrectly", func(t *testing.T) {
		// Arrange — price BETWEEN 50 AND 99 (BETWEEN is inclusive on both ends)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Between{"price": [2]interface{}{50, 99}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50), carrot(75), mystery(99) — all inclusive
		assert.Equal(t, []string{"banana", "carrot", "mystery"}, names)
	})

	t.Run("NotBetween", func(t *testing.T) {
		// Arrange — price NOT BETWEEN 75 AND 150
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotBetween{"price": [2]interface{}{75, 150}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50), donut(200)
		assert.Equal(t, []string{"banana", "donut"}, names)
	})

	t.Run("CombinedWithEq", func(t *testing.T) {
		// Arrange — category = 'fruit' AND price BETWEEN 50 AND 80
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{
				sqrl.Eq{"category": "fruit"},
				sqrl.Between{"price": [2]interface{}{50, 80}},
			}).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana (price 50, fruit)
		assert.Equal(t, []string{"banana"}, names)
	})

	t.Run("BetweenWithStringValues", func(t *testing.T) {
		// Arrange — name BETWEEN 'b' AND 'd' (lexicographic)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Between{"name": [2]interface{}{"b", "d"}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana, carrot (lexicographically between 'b' and 'd')
		assert.Equal(t, []string{"banana", "carrot"}, names)
	})

	t.Run("BetweenNoMatch", func(t *testing.T) {
		// Arrange — price BETWEEN 300 AND 400 (no items in range)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Between{"price": [2]interface{}{300, 400}})

		// Act
		names := queryStrings(t, q)

		// Assert — empty
		assert.Empty(t, names)
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		// Arrange — id BETWEEN 2 AND 5 AND price BETWEEN 50 AND 100
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Between{"id": [2]interface{}{2, 5}, "price": [2]interface{}{50, 100}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(id=2,price=50), carrot(id=3,price=75)
		assert.Equal(t, []string{"banana", "carrot"}, names)
	})

	t.Run("ToSQLDollar", func(t *testing.T) {
		// Arrange — verify Between produces correct Dollar placeholders
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": "fruit"}).
			Where(sqrl.Between{"price": [2]interface{}{50, 100}}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE category = $1 AND price BETWEEN $2 AND $3", sqlStr)
		assert.Equal(t, []interface{}{"fruit", 50, 100}, args)
	})

	t.Run("NotBetweenToSQLDollar", func(t *testing.T) {
		// Arrange — verify NotBetween produces correct Dollar placeholders
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.NotBetween{"price": [2]interface{}{50, 100}}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_items WHERE price NOT BETWEEN $1 AND $2", sqlStr)
		assert.Equal(t, []interface{}{50, 100}, args)
	})

	t.Run("BetweenCombinedWithOr", func(t *testing.T) {
		// Arrange — price BETWEEN 50 AND 60 OR price BETWEEN 190 AND 210
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.Between{"price": [2]interface{}{50, 60}},
				sqrl.Between{"price": [2]interface{}{190, 210}},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50), donut(200)
		assert.Equal(t, []string{"banana", "donut"}, names)
	})

	t.Run("BetweenCombinedWithNot", func(t *testing.T) {
		// Arrange — NOT (price BETWEEN 75 AND 150) — should match NotBetween
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Not{Cond: sqrl.Between{"price": [2]interface{}{75, 150}}}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana(50), donut(200)
		assert.Equal(t, []string{"banana", "donut"}, names)
	})

	t.Run("BetweenSameLoAndHi", func(t *testing.T) {
		// Arrange — price BETWEEN 100 AND 100 → exact match for price = 100
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Between{"price": [2]interface{}{100, 100}})

		// Act
		names := queryStrings(t, q)

		// Assert — only apple (price = 100)
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("NotBetweenAllInRange", func(t *testing.T) {
		// Arrange — price NOT BETWEEN 1 AND 1000 — all items have price in [1, 1000]
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotBetween{"price": [2]interface{}{1, 1000}})

		// Act
		names := queryStrings(t, q)

		// Assert — empty: all items have prices in [50, 200]
		assert.Empty(t, names)
	})
}

// ---------------------------------------------------------------------------
// Exists / NotExists
// ---------------------------------------------------------------------------

func TestExprExists(t *testing.T) {
	t.Run("ExistsCorrelated", func(t *testing.T) {
		// Arrange — select categories that have at least one item
		sub := sqrl.Select("1").From("sq_items").Where("sq_items.category = sq_categories.name")
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Exists(sub)).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — fruit, pastry, vegetable have items; dairy does not
		assert.Equal(t, []string{"fruit", "pastry", "vegetable"}, names)
	})

	t.Run("NotExistsCorrelated", func(t *testing.T) {
		// Arrange — select categories that have NO items
		sub := sqrl.Select("1").From("sq_items").Where("sq_items.category = sq_categories.name")
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.NotExists(sub)).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — only dairy has no items
		assert.Equal(t, []string{"dairy"}, names)
	})

	t.Run("ExistsWithArgs", func(t *testing.T) {
		// Arrange — categories that have items with price > 100
		sub := sqrl.Select("1").From("sq_items").
			Where("sq_items.category = sq_categories.name").
			Where(sqrl.Gt{"price": 100})
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Exists(sub)).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — pastry (200) and vegetable (150)
		assert.Equal(t, []string{"pastry", "vegetable"}, names)
	})

	t.Run("ExistsCombinedWithEq", func(t *testing.T) {
		// Arrange — items in 'fruit' category that also exist in categories table
		sub := sqrl.Select("1").From("sq_categories").Where("sq_categories.name = sq_items.category")
		q := sb.Select("name").From("sq_items").
			Where(sqrl.And{
				sqrl.Eq{"category": "fruit"},
				sqrl.Exists(sub),
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple and banana
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("NotExistsCombinedWithCondition", func(t *testing.T) {
		// Arrange — items whose category does NOT exist in categories table
		sub := sqrl.Select("1").From("sq_categories").Where("sq_categories.name = sq_items.category")
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotExists(sub)).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — mystery has NULL category, so no matching row in sq_categories
		assert.Equal(t, []string{"mystery"}, names)
	})

	t.Run("ExistsToSQLDollar", func(t *testing.T) {
		// Arrange — verify Exists produces correct Dollar placeholders
		sub := sqrl.Select("1").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sqrl.Select("name").From("sq_categories").
			Where(sqrl.Eq{"description": "Fresh fruits"}).
			Where(sqrl.Exists(sub)).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_categories WHERE description = $1 AND EXISTS (SELECT 1 FROM sq_items WHERE category = $2)", sqlStr)
		assert.Equal(t, []interface{}{"Fresh fruits", "fruit"}, args)
	})

	t.Run("NotExistsToSQLDollar", func(t *testing.T) {
		// Arrange — verify NotExists produces correct Dollar placeholders
		sub := sqrl.Select("1").From("sq_items").Where(sqrl.Eq{"category": "dairy"})
		q := sqrl.Select("name").From("sq_categories").
			Where(sqrl.NotExists(sub)).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM sq_categories WHERE NOT EXISTS (SELECT 1 FROM sq_items WHERE category = $1)", sqlStr)
		assert.Equal(t, []interface{}{"dairy"}, args)
	})

	t.Run("ExistsCombinedWithOr", func(t *testing.T) {
		// Arrange — category = 'dairy' OR EXISTS (items with price > 100 in this category)
		sub := sqrl.Select("1").From("sq_items").
			Where("sq_items.category = sq_categories.name").
			Where(sqrl.Gt{"price": 100})
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Or{
				sqrl.Eq{"name": "dairy"},
				sqrl.Exists(sub),
			}).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — dairy (from the Eq) + pastry(200) + vegetable(150) from EXISTS
		assert.Equal(t, []string{"dairy", "pastry", "vegetable"}, names)
	})

	t.Run("NotExistsEquivalentToNotExists", func(t *testing.T) {
		// Arrange — Not{Exists(sub)} should produce the same rows as NotExists(sub)
		sub := sqrl.Select("1").From("sq_items").Where("sq_items.category = sq_categories.name")

		q1 := sb.Select("name").From("sq_categories").
			Where(sqrl.Not{Cond: sqrl.Exists(sub)}).
			OrderBy("name")
		q2 := sb.Select("name").From("sq_categories").
			Where(sqrl.NotExists(sub)).
			OrderBy("name")

		// Act
		names1 := queryStrings(t, q1)
		names2 := queryStrings(t, q2)

		// Assert — both should return the same results
		assert.Equal(t, names1, names2)
		assert.Equal(t, []string{"dairy"}, names1)
	})

	t.Run("ExistsNoMatchNonCorrelated", func(t *testing.T) {
		// Arrange — EXISTS with a subquery that returns no rows (non-correlated)
		sub := sqrl.Select("1").From("sq_items").Where(sqrl.Eq{"category": "nonexistent"})
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Exists(sub)).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — no category matches because subquery returns empty
		assert.Empty(t, names)
	})

	t.Run("ExistsAllMatchNonCorrelated", func(t *testing.T) {
		// Arrange — EXISTS with a subquery that always returns rows (non-correlated)
		sub := sqrl.Select("1").From("sq_items").Where(sqrl.Eq{"category": "fruit"})
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Exists(sub)).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — all categories returned because subquery always has rows
		assert.Equal(t, []string{"dairy", "fruit", "pastry", "vegetable"}, names)
	})

	t.Run("ExistsWithBetweenInSubquery", func(t *testing.T) {
		// Arrange — cross-feature: categories that have items with price BETWEEN 50 AND 80
		sub := sqrl.Select("1").From("sq_items").
			Where("sq_items.category = sq_categories.name").
			Where(sqrl.Between{"price": [2]interface{}{50, 80}})
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Exists(sub)).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — fruit(banana=50) and vegetable(carrot=75)
		assert.Equal(t, []string{"fruit", "vegetable"}, names)
	})

	t.Run("NotExistsCombinedWithOr", func(t *testing.T) {
		// Arrange — name = 'fruit' OR NOT EXISTS(items with price > 100 in category)
		sub := sqrl.Select("1").From("sq_items").
			Where("sq_items.category = sq_categories.name").
			Where(sqrl.Gt{"price": 100})
		q := sb.Select("name").From("sq_categories").
			Where(sqrl.Or{
				sqrl.Eq{"name": "fruit"},
				sqrl.NotExists(sub),
			}).
			OrderBy("name")

		// Act
		names := queryStrings(t, q)

		// Assert — fruit (from Eq) + dairy (no items) + fruit (no items > 100)
		//          fruit already counted; dairy has no items so NOT EXISTS is true;
		//          fruit has items but none > 100 so NOT EXISTS is true too
		assert.Equal(t, []string{"dairy", "fruit"}, names)
	})
}

// ---------------------------------------------------------------------------
// Multi-key expressions inside Or — GitHub #269
// ---------------------------------------------------------------------------

func TestExprOrWithMultiKeyEq(t *testing.T) {
	t.Run("BasicMultiKeyEqInsideOr", func(t *testing.T) {
		// Arrange — (category = 'fruit' AND price = 50) OR (category = 'vegetable' AND price = 75)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.Eq{"category": "fruit", "price": 50},
				sqrl.Eq{"category": "vegetable", "price": 75},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana (fruit, 50), carrot (vegetable, 75)
		assert.Equal(t, []string{"banana", "carrot"}, names)
	})

	t.Run("MultiKeyEqInsideOrNoMatch", func(t *testing.T) {
		// Arrange — combinations that don't match any row
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.Eq{"category": "fruit", "price": 999},
				sqrl.Eq{"category": "vegetable", "price": 999},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — no rows
		assert.Empty(t, names)
	})

	t.Run("MixedMultiKeyAndSingleKeyInsideOr", func(t *testing.T) {
		// Arrange — (category = 'fruit' AND price = 100) OR (id = 4)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.Eq{"category": "fruit", "price": 100},
				sqrl.Eq{"id": 4},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple (fruit, 100), donut (id 4)
		assert.Equal(t, []string{"apple", "donut"}, names)
	})

	t.Run("MultiKeyLtInsideOr", func(t *testing.T) {
		// Arrange — (id < 2 AND price < 200) OR (id > 5)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.Lt{"id": 2, "price": 200},
				sqrl.Gt{"id": 5},
			}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — apple (id 1, price 100 < 200), mystery (id 6)
		assert.Equal(t, []string{"apple", "mystery"}, names)
	})

	t.Run("MultiKeyEqWithDollarPlaceholders", func(t *testing.T) {
		// Arrange — verify Dollar placeholders work correctly with parenthesized multi-key Eq
		q := sqrl.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.Eq{"category": "fruit", "price": 50},
				sqrl.Eq{"category": "vegetable", "price": 75},
			}).
			PlaceholderFormat(sqrl.Dollar)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)

		// Assert — correct SQL with dollar placeholders and parenthesized groups
		assert.Equal(t, "SELECT name FROM sq_items WHERE ((category = $1 AND price = $2) OR (category = $3 AND price = $4))", sqlStr)
		assert.Equal(t, []interface{}{"fruit", 50, "vegetable", 75}, args)
	})

	t.Run("MultiKeyNotEqInsideOr", func(t *testing.T) {
		// Arrange — (category <> 'fruit' AND price <> 200) OR (id = 2)
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Or{
				sqrl.NotEq{"category": "fruit", "price": 200},
				sqrl.Eq{"id": 2},
			}).
			Where(sqrl.NotEq{"category": nil}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — banana (matched by Or/Eq id=2); carrot (vegetable, 75); eggplant (vegetable, 150)
		// donut: category=pastry <> fruit ✓, price=200 <> 200 ✗ → first branch fails, id=2? no → excluded
		// Wait: apple is fruit, so category<>'fruit' is false → excluded from first branch; id=2? no
		// banana: category='fruit' so category<>'fruit'=false → first branch fails; id=2? yes → included
		// carrot: category='vegetable'<>'fruit' ✓, price=75<>200 ✓ → included
		// donut: category='pastry'<>'fruit' ✓, price=200<>200 ✗ → first branch fails; id=2? no → excluded
		// eggplant: category='vegetable'<>'fruit' ✓, price=150<>200 ✓ → included
		assert.Equal(t, []string{"banana", "carrot", "eggplant"}, names)
	})
}
