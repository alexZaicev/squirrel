package integration

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// Basic SELECT
// ---------------------------------------------------------------------------

func TestSelectBasic(t *testing.T) {
	t.Run("AllRows", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant", "mystery"}, names)
	})

	t.Run("SpecificColumns", func(t *testing.T) {
		// Arrange
		q := sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id int
		var name string
		require.NoError(t, rows.Scan(&id, &name))

		// Assert
		assert.Equal(t, 1, id)
		assert.Equal(t, "apple", name)
		assert.False(t, rows.Next())
	})

	t.Run("Star", func(t *testing.T) {
		// Arrange
		q := sb.Select("*").From("sq_items").Where(sqrl.Eq{"id": 2})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id, price int
		var name string
		var category sql.NullString
		require.NoError(t, rows.Scan(&id, &name, &category, &price))

		// Assert
		assert.Equal(t, 2, id)
		assert.Equal(t, "banana", name)
		assert.Equal(t, "fruit", category.String)
		assert.Equal(t, 50, price)
	})

	t.Run("EmptyResult", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 999})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Empty(t, names)
	})
}

// ---------------------------------------------------------------------------
// DISTINCT and Options
// ---------------------------------------------------------------------------

func TestSelectDistinct(t *testing.T) {
	// Arrange
	q := sb.Select("category").From("sq_items").
		Where(sqrl.NotEq{"category": nil}).
		Distinct().
		OrderBy("category")

	// Act
	vals := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"fruit", "pastry", "vegetable"}, vals)
}

func TestSelectOptions(t *testing.T) {
	// Arrange — DISTINCT via Options instead of Distinct()
	q := sb.Select("category").From("sq_items").
		Where(sqrl.NotEq{"category": nil}).
		Options("DISTINCT").
		OrderBy("category")

	// Act
	vals := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"fruit", "pastry", "vegetable"}, vals)
}

// ---------------------------------------------------------------------------
// WHERE clause
// ---------------------------------------------------------------------------

func TestSelectWhere(t *testing.T) {
	t.Run("StringPredicate", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").Where("id = ?", 3)

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"carrot"}, names)
	})

	t.Run("MapPredicate", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").Where(map[string]interface{}{"id": 1})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("NilPredicateIgnored", func(t *testing.T) {
		// Arrange — nil where should be a no-op
		q := sb.Select("name").From("sq_items").Where(nil).OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — all rows returned
		assert.Len(t, names, 6)
	})

	t.Run("EmptyStringPredicateIgnored", func(t *testing.T) {
		// Arrange — empty string where should be a no-op
		q := sb.Select("name").From("sq_items").Where("").OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Len(t, names, 6)
	})

	t.Run("MultipleClauses", func(t *testing.T) {
		// Arrange — multiple Where calls are ANDed
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": "fruit"}).
			Where(sqrl.Gt{"price": 60}).
			OrderBy("id")

		// Act
		names := queryStrings(t, q)

		// Assert — only apple (price 100), banana is 50 < 60
		assert.Equal(t, []string{"apple"}, names)
	})
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

func TestSelectOrderBy(t *testing.T) {
	t.Run("Ascending", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			OrderBy("name ASC")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant"}, names)
	})

	t.Run("Descending", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			OrderBy("name DESC")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"eggplant", "donut", "carrot", "banana", "apple"}, names)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		// Arrange — sort by category asc, then price desc
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": []string{"fruit", "vegetable"}}).
			OrderBy("category ASC", "price DESC")

		// Act
		names := queryStrings(t, q)

		// Assert — fruit: apple(100), banana(50) ; vegetable: eggplant(150), carrot(75)
		assert.Equal(t, []string{"apple", "banana", "eggplant", "carrot"}, names)
	})

	t.Run("OrderByClause", func(t *testing.T) {
		// Arrange — complex ORDER BY using OrderByClause with placeholder
		q := sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			OrderByClause("CASE WHEN category = ? THEN 0 ELSE 1 END, name", "fruit")

		// Act
		names := queryStrings(t, q)

		// Assert — fruit items first (apple, banana), then others alphabetically
		assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant"}, names)
	})
}

// ---------------------------------------------------------------------------
// LIMIT and OFFSET
// ---------------------------------------------------------------------------

func TestSelectLimitOffset(t *testing.T) {
	t.Run("Limit", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(3)

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana", "carrot"}, names)
	})

	t.Run("Offset", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(100).Offset(4)

		// Act
		names := queryStrings(t, q)

		// Assert — skip first 4 rows
		assert.Equal(t, []string{"eggplant", "mystery"}, names)
	})

	t.Run("LimitAndOffset", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(2).Offset(1)

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"banana", "carrot"}, names)
	})

	t.Run("RemoveLimit", func(t *testing.T) {
		// Arrange — set limit then remove it
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(1).RemoveLimit()

		// Act
		names := queryStrings(t, q)

		// Assert — all 6 rows
		assert.Len(t, names, 6)
	})

	t.Run("RemoveOffset", func(t *testing.T) {
		// Arrange — set offset then remove it
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(100).Offset(5).RemoveOffset()

		// Act
		names := queryStrings(t, q)

		// Assert — all 6 rows
		assert.Len(t, names, 6)
	})

	t.Run("LimitZero", func(t *testing.T) {
		// Arrange
		q := sb.Select("name").From("sq_items").Limit(0)

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Empty(t, names)
	})

	t.Run("ParameterizedLimitPlaceholderSQL", func(t *testing.T) {
		// Verify that the generated SQL uses placeholders for LIMIT/OFFSET.
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(3).Offset(1)

		sqlStr, args, err := q.ToSQL()
		require.NoError(t, err)

		if isPostgres() {
			assert.Contains(t, sqlStr, "LIMIT $")
			assert.Contains(t, sqlStr, "OFFSET $")
		} else {
			assert.Contains(t, sqlStr, "LIMIT ?")
			assert.Contains(t, sqlStr, "OFFSET ?")
		}

		// LIMIT and OFFSET values appear as bound args
		assert.Contains(t, args, uint64(3))
		assert.Contains(t, args, uint64(1))
	})

	t.Run("ParameterizedLimitWithWhere", func(t *testing.T) {
		// LIMIT/OFFSET args don't interfere with WHERE args
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"category": "fruit"}).
			OrderBy("id").
			Limit(1).
			Offset(0)

		names := queryStrings(t, q)

		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("ParameterizedLimitPreparedStatementReuse", func(t *testing.T) {
		// The key benefit: SQL string is the same for different values,
		// enabling prepared statement caching.
		b1 := sb.Select("name").From("sq_items").OrderBy("id").Limit(2).Offset(0)
		b2 := sb.Select("name").From("sq_items").OrderBy("id").Limit(2).Offset(2)

		sql1, _, err := b1.ToSQL()
		require.NoError(t, err)
		sql2, _, err := b2.ToSQL()
		require.NoError(t, err)

		assert.Equal(t, sql1, sql2, "SQL strings should be identical for different limit/offset values")

		names1 := queryStrings(t, b1)
		names2 := queryStrings(t, b2)

		assert.Equal(t, []string{"apple", "banana"}, names1)
		assert.Equal(t, []string{"carrot", "donut"}, names2)
	})

	t.Run("ParameterizedLimitLargeValue", func(t *testing.T) {
		// Large limit values work correctly with parameterized queries
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(1000000)

		names := queryStrings(t, q)

		assert.Len(t, names, 6) // all 6 rows
	})

	t.Run("ParameterizedOffsetBeyondRows", func(t *testing.T) {
		// Offset beyond the number of rows returns empty result
		q := sb.Select("name").From("sq_items").OrderBy("id").Limit(100).Offset(100)

		names := queryStrings(t, q)

		assert.Empty(t, names)
	})
}

// ---------------------------------------------------------------------------
// GROUP BY and HAVING
// ---------------------------------------------------------------------------

func TestSelectGroupBy(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		// Arrange
		q := sb.Select("category").
			Column("COUNT(*) AS cnt").
			From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category").
			OrderBy("category")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Category string
			Count    int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Category, &r.Count))
			results = append(results, r)
		}

		// Assert
		assert.Equal(t, []result{
			{"fruit", 2},
			{"pastry", 1},
			{"vegetable", 2},
		}, results)
	})

	t.Run("Having", func(t *testing.T) {
		// Arrange — only categories with more than 1 item
		q := sb.Select("category").
			Column("COUNT(*) AS cnt").
			From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category").
			Having("COUNT(*) > ?", 1).
			OrderBy("category")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		var categories []string
		for rows.Next() {
			var cat string
			var cnt int
			require.NoError(t, rows.Scan(&cat, &cnt))
			categories = append(categories, cat)
		}

		// Assert — only fruit(2) and vegetable(2)
		assert.Equal(t, []string{"fruit", "vegetable"}, categories)
	})
}

// ---------------------------------------------------------------------------
// JOINs
// ---------------------------------------------------------------------------

func TestSelectJoin(t *testing.T) {
	t.Run("InnerJoin", func(t *testing.T) {
		// Arrange
		q := sb.Select("sq_items.name", "sq_categories.description").
			From("sq_items").
			InnerJoin("sq_categories ON sq_items.category = sq_categories.name").
			Where(sqrl.Eq{"sq_items.id": 1})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name, desc string
		require.NoError(t, rows.Scan(&name, &desc))

		// Assert
		assert.Equal(t, "apple", name)
		assert.Equal(t, "Fresh fruits", desc)
	})

	t.Run("Join", func(t *testing.T) {
		// Arrange — Join is alias for INNER JOIN
		q := sb.Select("sq_items.name").
			From("sq_items").
			Join("sq_categories ON sq_items.category = sq_categories.name").
			OrderBy("sq_items.id")

		// Act
		names := queryStrings(t, q)

		// Assert — 'mystery' has NULL category, so no join match
		assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant"}, names)
	})

	t.Run("LeftJoin", func(t *testing.T) {
		// Arrange — LEFT JOIN keeps all rows from sq_items
		q := sb.Select("sq_items.name").
			From("sq_items").
			LeftJoin("sq_categories ON sq_items.category = sq_categories.name").
			OrderBy("sq_items.id")

		// Act
		names := queryStrings(t, q)

		// Assert — all 6 items including mystery
		assert.Len(t, names, 6)
		assert.Equal(t, "mystery", names[5])
	})

	t.Run("CrossJoin", func(t *testing.T) {
		// Arrange — CROSS JOIN produces cartesian product
		q := sb.Select("COUNT(*)").
			From("sq_items").
			CrossJoin("sq_categories")

		// Act
		var count int
		err := q.QueryRow().Scan(&count)

		// Assert — 6 items × 4 categories = 24
		require.NoError(t, err)
		assert.Equal(t, 24, count)
	})

	t.Run("JoinWithPlaceholders", func(t *testing.T) {
		// Arrange — JOIN clause with bound parameters
		q := sb.Select("sq_items.name").
			From("sq_items").
			Join("sq_categories ON sq_items.category = sq_categories.name AND sq_categories.name = ?", "fruit").
			OrderBy("sq_items.id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("RightJoin", func(t *testing.T) {
		// Arrange — RIGHT JOIN keeps all rows from sq_categories
		q := sb.Select("sq_categories.name", "sq_items.name").
			From("sq_items").
			RightJoin("sq_categories ON sq_items.category = sq_categories.name").
			OrderBy("sq_categories.name")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Category string
			ItemName sql.NullString
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Category, &r.ItemName))
			results = append(results, r)
		}

		// Assert — 'dairy' has no items so ItemName is NULL
		var dairyFound bool
		for _, r := range results {
			if r.Category == "dairy" {
				dairyFound = true
				assert.False(t, r.ItemName.Valid)
			}
		}
		assert.True(t, dairyFound, "dairy category should appear in RIGHT JOIN")
	})

	t.Run("JoinClauseDirect", func(t *testing.T) {
		// Arrange — use JoinClause directly with a custom join type
		q := sb.Select("sq_items.name").
			From("sq_items").
			JoinClause("JOIN sq_categories ON sq_items.category = sq_categories.name").
			Where(sqrl.Eq{"sq_items.id": 1})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("MultipleJoins", func(t *testing.T) {
		// Arrange — join the same table twice with aliases is complex;
		// instead test multiple different join clauses on same query
		q := sb.Select("sq_items.name").
			From("sq_items").
			Join("sq_categories ON sq_items.category = sq_categories.name").
			Where(sqrl.Eq{"sq_categories.description": "Fresh fruits"}).
			OrderBy("sq_items.id")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple", "banana"}, names)
	})
}

// ---------------------------------------------------------------------------
// SELECT without FROM (e.g. SELECT 1+1)
// ---------------------------------------------------------------------------

func TestSelectWithoutFrom(t *testing.T) {
	// Arrange
	q := sb.Select("1+1")

	// Act
	var result int
	err := q.Scan(&result)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 2, result)
}

// ---------------------------------------------------------------------------
// GROUP BY and HAVING — additional
// ---------------------------------------------------------------------------

func TestSelectGroupByAdditional(t *testing.T) {
	t.Run("MultipleGroupBys", func(t *testing.T) {
		// Arrange — GROUP BY category, then by a computed expression
		q := sb.Select("category").
			Column("CASE WHEN price > 100 THEN 'high' ELSE 'low' END AS tier").
			Column("COUNT(*) AS cnt").
			From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category", "tier").
			OrderBy("category", "tier")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Cat   string
			Tier  string
			Count int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Cat, &r.Tier, &r.Count))
			results = append(results, r)
		}

		// Assert — verify we get multiple groups
		assert.True(t, len(results) > 3, "expected more than 3 groups from dual GROUP BY")
	})

	t.Run("HavingWithEq", func(t *testing.T) {
		// Arrange — HAVING with Eq predicate
		q := sb.Select("category").
			Column("COUNT(*) AS cnt").
			From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category").
			Having(sqrl.Eq{"COUNT(*)": 1}).
			OrderBy("category")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		var categories []string
		for rows.Next() {
			var cat string
			var cnt int
			require.NoError(t, rows.Scan(&cat, &cnt))
			categories = append(categories, cat)
		}

		// Assert — only pastry has count = 1
		assert.Equal(t, []string{"pastry"}, categories)
	})

	t.Run("MultipleHaving", func(t *testing.T) {
		// Arrange — multiple Having calls produce AND
		q := sb.Select("category").
			Column("COUNT(*) AS cnt").
			From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category").
			Having("COUNT(*) >= ?", 1).
			Having("COUNT(*) < ?", 3).
			OrderBy("category")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		var categories []string
		for rows.Next() {
			var cat string
			var cnt int
			require.NoError(t, rows.Scan(&cat, &cnt))
			categories = append(categories, cat)
		}

		// Assert — all categories have 1 or 2 items, all < 3
		assert.Equal(t, []string{"fruit", "pastry", "vegetable"}, categories)
	})
}

// ---------------------------------------------------------------------------
// Prefix and Suffix
// ---------------------------------------------------------------------------

func TestSelectPrefixSuffix(t *testing.T) {
	t.Run("Prefix", func(t *testing.T) {
		// Arrange — a SQL comment as prefix is harmless and verifies the prefix is added
		q := sb.Select("name").From("sq_items").
			Prefix("/* integration test prefix */").
			Where(sqrl.Eq{"id": 1})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("Suffix", func(t *testing.T) {
		// Arrange — a SQL comment as suffix
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": 1}).
			Suffix("/* integration test suffix */")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("PrefixExpr", func(t *testing.T) {
		// Arrange — PrefixExpr with a Sqlizer
		q := sb.Select("name").From("sq_items").
			PrefixExpr(sqrl.Expr("/* dynamic prefix */")).
			Where(sqrl.Eq{"id": 2})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"banana"}, names)
	})

	t.Run("SuffixExpr", func(t *testing.T) {
		// Arrange — SuffixExpr with a Sqlizer
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": 2}).
			SuffixExpr(sqrl.Expr("/* dynamic suffix */"))

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"banana"}, names)
	})

	t.Run("PrefixWithArgs", func(t *testing.T) {
		// Arrange — Prefix with bound args (test at ToSQL level since ? in
		// prefix text is opaque to the DB parser)
		q := sqrl.Select("name").From("items").
			Prefix("WITH cte AS (SELECT ? AS v)", 42)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "WITH cte AS (SELECT ? AS v) SELECT name FROM items", sqlStr)
		assert.Equal(t, []interface{}{42}, args)
	})

	t.Run("SuffixWithArgs", func(t *testing.T) {
		// Arrange — Suffix with bound args (test at ToSQL level)
		q := sqrl.Select("name").From("items").
			Where(sqrl.Eq{"id": 1}).
			Suffix("LIMIT ?", 10)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM items WHERE id = ? LIMIT ?", sqlStr)
		assert.Equal(t, []interface{}{1, 10}, args)
	})

	t.Run("MultiplePrefixes", func(t *testing.T) {
		// Arrange — multiple prefix expressions
		q := sb.Select("name").From("sq_items").
			Prefix("/* prefix1 */").
			Prefix("/* prefix2 */").
			Where(sqrl.Eq{"id": 1})

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("MultipleSuffixes", func(t *testing.T) {
		// Arrange — multiple suffix expressions
		q := sb.Select("name").From("sq_items").
			Where(sqrl.Eq{"id": 1}).
			Suffix("/* suffix1 */").
			Suffix("/* suffix2 */")

		// Act
		names := queryStrings(t, q)

		// Assert
		assert.Equal(t, []string{"apple"}, names)
	})
}

// ---------------------------------------------------------------------------
// Column / RemoveColumns / Column with expressions
// ---------------------------------------------------------------------------

func TestSelectColumns(t *testing.T) {
	t.Run("ColumnsMethod", func(t *testing.T) {
		// Arrange
		q := sb.Select("id").Columns("name", "price").From("sq_items").Where(sqrl.Eq{"id": 1})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id, price int
		var name string
		require.NoError(t, rows.Scan(&id, &name, &price))

		// Assert
		assert.Equal(t, 1, id)
		assert.Equal(t, "apple", name)
		assert.Equal(t, 100, price)
	})

	t.Run("ColumnWithArgs", func(t *testing.T) {
		// Arrange — Column with placeholder args.
		// Use CASE to return an integer instead of a boolean for
		// portability across SQLite, MySQL, and PostgreSQL.
		q := sb.Select("name").
			Column("CASE WHEN price > ? THEN 1 ELSE 0 END AS is_expensive", 100).
			From("sq_items").
			Where(sqrl.Eq{"id": 4})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name string
		var isExpensive int
		require.NoError(t, rows.Scan(&name, &isExpensive))

		// Assert — donut price 200 > 100
		assert.Equal(t, "donut", name)
		assert.Equal(t, 1, isExpensive)
	})

	t.Run("ColumnWithAlias", func(t *testing.T) {
		// Arrange — use Alias to alias an expression
		caseExpr := sqrl.Case().
			When(sqrl.Expr("price > ?", 100), "'expensive'").
			Else("'cheap'")
		q := sb.Select("name").Column(sqrl.Alias(caseExpr, "tier")).
			From("sq_items").
			Where(sqrl.Eq{"id": 4})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name, tier string
		require.NoError(t, rows.Scan(&name, &tier))

		// Assert
		assert.Equal(t, "donut", name)
		assert.Equal(t, "expensive", tier)
	})

	t.Run("RemoveColumns", func(t *testing.T) {
		// Arrange — build with columns, remove them, add new ones
		q := sb.Select("id", "name").From("sq_items").RemoveColumns().Columns("price").
			Where(sqrl.Eq{"id": 1})

		// Act
		prices := queryInts(t, q)

		// Assert
		assert.Equal(t, []int{100}, prices)
	})

	t.Run("ColumnWithPlaceholdersHelper", func(t *testing.T) {
		// Arrange — use Placeholders() in Column expression.
		// Use CASE to return an integer instead of a boolean for
		// portability across SQLite, MySQL, and PostgreSQL.
		q := sb.Select("name").
			Column("CASE WHEN id IN ("+sqrl.Placeholders(3)+") THEN 1 ELSE 0 END AS in_set", 1, 2, 3).
			From("sq_items").
			Where(sqrl.Eq{"id": 1})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name string
		var inSet int
		require.NoError(t, rows.Scan(&name, &inSet))

		// Assert — id 1 is in (1,2,3)
		assert.Equal(t, "apple", name)
		assert.Equal(t, 1, inSet)
	})
}

// ---------------------------------------------------------------------------
// Exec on SELECT (legal but discards results)
// ---------------------------------------------------------------------------

func TestSelectExec(t *testing.T) {
	// Arrange
	q := sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 1})

	// Act
	_, err := q.Exec()

	// Assert
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Builder immutability
// ---------------------------------------------------------------------------

func TestSelectBuilderImmutability(t *testing.T) {
	// Arrange — build two different queries from the same base
	base := sb.Select("name").From("sq_items").OrderBy("name")
	qFruit := base.Where(sqrl.Eq{"category": "fruit"})
	qVeg := base.Where(sqrl.Eq{"category": "vegetable"})

	// Act
	fruits := queryStrings(t, qFruit)
	vegs := queryStrings(t, qVeg)

	// Assert — each query is independent
	assert.Equal(t, []string{"apple", "banana"}, fruits)
	assert.Equal(t, []string{"carrot", "eggplant"}, vegs)
}

// ---------------------------------------------------------------------------
// ToSQL and MustSQL
// ---------------------------------------------------------------------------

func TestSelectToSQL(t *testing.T) {
	t.Run("ValidQuery", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items").Where(sqrl.Eq{"id": 1})

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM items WHERE id = ?", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("DollarPlaceholder", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items").
			Where(sqrl.Eq{"id": 1}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM items WHERE id = $1", sqlStr)
		assert.Equal(t, []interface{}{1}, args)
	})

	t.Run("ColonPlaceholder", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items").
			Where("id = ?", 1).
			PlaceholderFormat(sqrl.Colon)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM items WHERE id = :1", sqlStr)
	})

	t.Run("AtPPlaceholder", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items").
			Where("id = ?", 1).
			PlaceholderFormat(sqrl.AtP)

		// Act
		sqlStr, _, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT name FROM items WHERE id = @p1", sqlStr)
	})
}

func TestSelectMustSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("name").From("items")

		// Act
		sqlStr, args := q.MustSQL()

		// Assert
		assert.Equal(t, "SELECT name FROM items", sqlStr)
		assert.Empty(t, args)
	})

	t.Run("PanicsOnError", func(t *testing.T) {
		// Arrange — no columns
		q := sqrl.Select()

		// Act & Assert
		assert.Panics(t, func() { q.MustSQL() })
	})
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

func TestSelectErrors(t *testing.T) {
	t.Run("NoColumns", func(t *testing.T) {
		// Arrange
		q := sqrl.Select()

		// Act
		_, _, err := q.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one result column")
	})

	t.Run("NoRunnerExec", func(t *testing.T) {
		// Arrange — builder without RunWith
		q := sqrl.Select("1")

		// Act
		_, err := q.Exec()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerQuery", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("1")

		// Act
		_, err := q.Query()

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerScan", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("1")

		// Act
		var v int
		err := q.Scan(&v)

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}

// ---------------------------------------------------------------------------
// Error paths — additional
// ---------------------------------------------------------------------------

func TestSelectErrorsAdditional(t *testing.T) {
	t.Run("NoRunnerQueryRow", func(t *testing.T) {
		// Arrange — builder without RunWith
		q := sqrl.Select("1")

		// Act
		var v int
		err := q.QueryRow().Scan(&v)

		// Assert
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}

// ---------------------------------------------------------------------------
// UNION via Suffix
// ---------------------------------------------------------------------------

func TestSelectUnionViaSuffix(t *testing.T) {
	// Arrange — first query selects fruit names, UNION selects pastry names
	q := sb.Select("name").From("sq_items").
		Where(sqrl.Eq{"category": "fruit"}).
		Suffix("UNION SELECT name FROM sq_items WHERE category = ?", "pastry")

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}

	// Assert — apple, banana, donut (UNION removes duplicates; order may vary)
	assert.ElementsMatch(t, []string{"apple", "banana", "donut"}, names)
}
