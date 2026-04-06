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
		// Arrange — empty And{} evaluates to (1=1), matching all
		q := sb.Select("name").From("sq_items").Where(sqrl.And{}).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Len(t, names, 6)
	})

	t.Run("EmptyOr", func(t *testing.T) {
		// Arrange — empty Or{} evaluates to (1=0), matching none
		q := sb.Select("name").From("sq_items").Where(sqrl.Or{})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Empty(t, names)
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
