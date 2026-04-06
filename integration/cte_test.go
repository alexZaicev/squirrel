package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// helpers — query helpers for CteBuilder
// ---------------------------------------------------------------------------

func cteQueryStrings(t *testing.T, c sqrl.CteBuilder) []string {
	t.Helper()
	rows, err := c.Query()
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

func cteQueryInts(t *testing.T, c sqrl.CteBuilder) []int {
	t.Helper()
	rows, err := c.Query()
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
// Basic CTE — WITH ... AS (...) SELECT
// ---------------------------------------------------------------------------

func TestCteBasicSelect(t *testing.T) {
	t.Run("SingleCTE", func(t *testing.T) {
		// Arrange — CTE selects all fruit, main query reads from CTE
		cte := sqrl.With("fruits",
			sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"}),
		).Statement(
			sqrl.Select("name").From("fruits").OrderBy("name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("CTEWithMultipleColumns", func(t *testing.T) {
		// Arrange — CTE selects id + name, main query reads both
		cte := sqrl.With("items_cte",
			sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"}),
		).Statement(
			sqrl.Select("id", "name").From("items_cte").OrderBy("id"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		rows, err := cte.Query()
		require.NoError(t, err)
		defer rows.Close()

		type row struct {
			ID   int
			Name string
		}
		var results []row
		for rows.Next() {
			var r row
			require.NoError(t, rows.Scan(&r.ID, &r.Name))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert
		assert.Equal(t, []row{
			{3, "carrot"},
			{5, "eggplant"},
		}, results)
	})

	t.Run("CTEReturnsNoRows", func(t *testing.T) {
		// Arrange — CTE matches nothing
		cte := sqrl.With("empty_cte",
			sb.Select("name").From("sq_items").Where(sqrl.Eq{"id": 999}),
		).Statement(
			sqrl.Select("name").From("empty_cte"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert
		assert.Empty(t, names)
	})
}

// ---------------------------------------------------------------------------
// Multiple CTEs — WITH a AS (...), b AS (...) SELECT
// ---------------------------------------------------------------------------

func TestCteMultipleCTEs(t *testing.T) {
	t.Run("TwoCTEsJoined", func(t *testing.T) {
		// Arrange — CTE1: items with their category,
		//           CTE2: category descriptions,
		//           Main: JOIN the two.
		cte := sqrl.With("item_cats",
			sb.Select("name", "category").From("sq_items").
				Where(sqrl.NotEq{"category": nil}),
		).With("cat_desc",
			sb.Select("name", "description").From("sq_categories"),
		).Statement(
			sqrl.Select("item_cats.name", "cat_desc.description").
				From("item_cats").
				Join("cat_desc ON item_cats.category = cat_desc.name").
				Where(sqrl.Eq{"item_cats.name": "apple"}),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		rows, err := cte.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name, desc string
		require.NoError(t, rows.Scan(&name, &desc))

		// Assert — apple is a fruit → "Fresh fruits"
		assert.Equal(t, "apple", name)
		assert.Equal(t, "Fresh fruits", desc)
		assert.False(t, rows.Next())
	})

	t.Run("CTEReferencingAnotherCTE", func(t *testing.T) {
		// Arrange — CTE1 selects fruit items; CTE2 filters CTE1 by price;
		//           Main reads from CTE2.
		cte := sqrl.With("all_fruit",
			sb.Select("id", "name", "price").From("sq_items").
				Where(sqrl.Eq{"category": "fruit"}),
		).With("expensive_fruit",
			sqrl.Select("name").From("all_fruit").Where("price > ?", 60),
		).Statement(
			sqrl.Select("name").From("expensive_fruit").OrderBy("name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — apple (100 > 60), banana (50 <= 60) excluded
		assert.Equal(t, []string{"apple"}, names)
	})

	t.Run("ThreeCTEs", func(t *testing.T) {
		// Arrange — three disjoint CTEs, main query unions them
		cte := sqrl.With("fruits_cte",
			sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"}),
		).With("vegs_cte",
			sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"}),
		).With("pastries_cte",
			sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "pastry"}),
		).Statement(
			sqrl.Union(
				sqrl.Select("name").From("fruits_cte"),
				sqrl.Select("name").From("vegs_cte"),
				sqrl.Select("name").From("pastries_cte"),
			).OrderBy("name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — all non-NULL category items sorted
		assert.Equal(t, []string{"apple", "banana", "carrot", "donut", "eggplant"}, names)
	})
}

// ---------------------------------------------------------------------------
// WITH ... AS (...) SELECT with WHERE placeholders
// ---------------------------------------------------------------------------

func TestCteWithPlaceholders(t *testing.T) {
	t.Run("SinglePlaceholder", func(t *testing.T) {
		// Arrange
		cte := sqrl.With("cheap",
			sb.Select("name").From("sq_items").Where("price < ?", 80),
		).Statement(
			sqrl.Select("name").From("cheap").OrderBy("name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — banana 50, carrot 75 < 80
		assert.Equal(t, []string{"banana", "carrot"}, names)
	})

	t.Run("PlaceholdersInCTEAndMainQuery", func(t *testing.T) {
		// Arrange — placeholder in CTE and in main query
		cte := sqrl.With("priced_items",
			sb.Select("name", "price").From("sq_items").Where("price >= ?", 50),
		).Statement(
			sqrl.Select("name").From("priced_items").Where("price <= ?", 100).OrderBy("name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — items with 50 <= price <= 100: banana(50), carrot(75), apple(100), mystery(99)
		assert.Equal(t, []string{"apple", "banana", "carrot", "mystery"}, names)
	})

	t.Run("MultiplePlaceholdersAcrossMultipleCTEs", func(t *testing.T) {
		// Arrange — two CTEs each with placeholders, main query also with placeholder
		cte := sqrl.With("cte_a",
			sb.Select("name", "price").From("sq_items").Where("price > ?", 90),
		).With("cte_b",
			sb.Select("name").From("sq_categories").Where("description LIKE ?", "%Fresh%"),
		).Statement(
			sqrl.Select("cte_a.name").From("cte_a").
				Join("sq_items ON sq_items.name = cte_a.name").
				Where("sq_items.category IN (SELECT name FROM cte_b)").
				OrderBy("cte_a.name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — items with price > 90 in "Fresh" categories (fruit, vegetable):
		//   apple(100, fruit), eggplant(150, vegetable)
		//   mystery(99, NULL) excluded (category is NULL)
		assert.Equal(t, []string{"apple", "eggplant"}, names)
	})
}

// ---------------------------------------------------------------------------
// Recursive CTE — WITH RECURSIVE
// ---------------------------------------------------------------------------

func TestCteRecursive(t *testing.T) {
	t.Run("GenerateNumberSequence", func(t *testing.T) {
		// Arrange — generate numbers 1..5 using recursive CTE
		cte := sqrl.WithRecursiveColumns("cnt", []string{"n"},
			sqrl.UnionAll(
				sqrl.Select("1"),
				sqrl.Select("n + 1").From("cnt").Where("n < ?", 5),
			),
		).Statement(
			sqrl.Select("n").From("cnt").OrderBy("n"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		vals := cteQueryInts(t, cte)

		// Assert
		assert.Equal(t, []int{1, 2, 3, 4, 5}, vals)
	})

	t.Run("RecursiveWithLargerLimit", func(t *testing.T) {
		// Arrange — generate 1..10
		cte := sqrl.WithRecursiveColumns("seq", []string{"val"},
			sqrl.UnionAll(
				sqrl.Select("1"),
				sqrl.Select("val + 1").From("seq").Where("val < ?", 10),
			),
		).Statement(
			sqrl.Select("val").From("seq").OrderBy("val"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		vals := cteQueryInts(t, cte)

		// Assert
		assert.Len(t, vals, 10)
		assert.Equal(t, 1, vals[0])
		assert.Equal(t, 10, vals[9])
	})

	t.Run("RecursiveWithSumAggregation", func(t *testing.T) {
		// Arrange — generate 1..5 then SUM them in main query
		cte := sqrl.WithRecursiveColumns("nums", []string{"n"},
			sqrl.UnionAll(
				sqrl.Select("1"),
				sqrl.Select("n + 1").From("nums").Where("n < ?", 5),
			),
		).Statement(
			sqrl.Select("SUM(n)").From("nums"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		var total int
		err := cte.Scan(&total)

		// Assert — 1+2+3+4+5 = 15
		require.NoError(t, err)
		assert.Equal(t, 15, total)
	})

	t.Run("RecursiveFibonacci", func(t *testing.T) {
		// Arrange — generate Fibonacci numbers using two columns
		// WITH RECURSIVE fib(a, b) AS (
		//   SELECT 0, 1
		//   UNION ALL
		//   SELECT b, a + b FROM fib WHERE b < 100
		// )
		// SELECT a FROM fib ORDER BY a
		cte := sqrl.WithRecursiveColumns("fib", []string{"a", "b"},
			sqrl.UnionAll(
				sqrl.Select("0, 1"),
				sqrl.Select("b, a + b").From("fib").Where("b < ?", 100),
			),
		).Statement(
			sqrl.Select("a").From("fib").OrderBy("a"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		vals := cteQueryInts(t, cte)

		// Assert — Fibonacci sequence: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89
		assert.Equal(t, []int{0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89}, vals)
	})
}

// ---------------------------------------------------------------------------
// Recursive CTE — hierarchical tree traversal
// ---------------------------------------------------------------------------

func TestCteRecursiveTreeTraversal(t *testing.T) {
	// Arrange — create a table with a parent-child relationship
	createTable(t, "sq_cte_tree", "(id INTEGER, name TEXT, parent_id INTEGER)")
	seedTable(t, `INSERT INTO sq_cte_tree (id, name, parent_id) VALUES
		(1, 'root', NULL),
		(2, 'child1', 1),
		(3, 'child2', 1),
		(4, 'grandchild1', 2),
		(5, 'grandchild2', 2),
		(6, 'grandchild3', 3)`)

	t.Run("TraverseFromRoot", func(t *testing.T) {
		// All descendants of root (id=1)
		cte := sqrl.WithRecursive("tree",
			sqrl.UnionAll(
				sb.Select("id", "name", "parent_id").From("sq_cte_tree").
					Where(sqrl.Eq{"id": 1}),
				sqrl.Select("t.id", "t.name", "t.parent_id").
					From("sq_cte_tree t").
					Join("tree ON t.parent_id = tree.id"),
			),
		).Statement(
			sqrl.Select("name").From("tree").OrderBy("id"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — entire tree rooted at 1
		assert.Equal(t, []string{"root", "child1", "child2", "grandchild1", "grandchild2", "grandchild3"}, names)
	})

	t.Run("TraverseFromSubtree", func(t *testing.T) {
		// Only descendants of child1 (id=2)
		cte := sqrl.WithRecursive("subtree",
			sqrl.UnionAll(
				sb.Select("id", "name", "parent_id").From("sq_cte_tree").
					Where(sqrl.Eq{"id": 2}),
				sqrl.Select("t.id", "t.name", "t.parent_id").
					From("sq_cte_tree t").
					Join("subtree ON t.parent_id = subtree.id"),
			),
		).Statement(
			sqrl.Select("name").From("subtree").OrderBy("id"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — child1 and its children
		assert.Equal(t, []string{"child1", "grandchild1", "grandchild2"}, names)
	})

	t.Run("TraverseLeafNode", func(t *testing.T) {
		// Leaf node has no children — recursive step returns nothing
		cte := sqrl.WithRecursive("leaf",
			sqrl.UnionAll(
				sb.Select("id", "name", "parent_id").From("sq_cte_tree").
					Where(sqrl.Eq{"id": 6}),
				sqrl.Select("t.id", "t.name", "t.parent_id").
					From("sq_cte_tree t").
					Join("leaf ON t.parent_id = leaf.id"),
			),
		).Statement(
			sqrl.Select("name").From("leaf"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		names := cteQueryStrings(t, cte)

		// Assert — only the leaf itself
		assert.Equal(t, []string{"grandchild3"}, names)
	})

	t.Run("CountDescendantsPerNode", func(t *testing.T) {
		// Use recursive CTE to count all descendants of root
		cte := sqrl.WithRecursive("descendants",
			sqrl.UnionAll(
				sb.Select("id", "name").From("sq_cte_tree").
					Where(sqrl.Eq{"parent_id": 1}),
				sqrl.Select("t.id", "t.name").
					From("sq_cte_tree t").
					Join("descendants d ON t.parent_id = d.id"),
			),
		).Statement(
			sqrl.Select("COUNT(*)").From("descendants"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		var count int
		err := cte.Scan(&count)

		// Assert — 5 descendants of root (child1, child2, grandchild1, grandchild2, grandchild3)
		require.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}

// ---------------------------------------------------------------------------
// CTE with explicit column aliases — WITH name(col1, col2) AS (...)
// ---------------------------------------------------------------------------

func TestCteWithColumns(t *testing.T) {
	t.Run("RenameColumns", func(t *testing.T) {
		// Arrange — CTE renames id→item_id, name→item_name
		cte := sqrl.WithColumns("renamed", []string{"item_id", "item_name"},
			sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"category": "fruit"}),
		).Statement(
			sqrl.Select("item_id", "item_name").From("renamed").OrderBy("item_id"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		rows, err := cte.Query()
		require.NoError(t, err)
		defer rows.Close()

		type row struct {
			ID   int
			Name string
		}
		var results []row
		for rows.Next() {
			var r row
			require.NoError(t, rows.Scan(&r.ID, &r.Name))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert
		assert.Equal(t, []row{
			{1, "apple"},
			{2, "banana"},
		}, results)
	})
}

// ---------------------------------------------------------------------------
// Mixed recursive and non-recursive CTEs
// ---------------------------------------------------------------------------

func TestCteMixedRecursiveAndNonRecursive(t *testing.T) {
	// Arrange — one non-recursive CTE (categories) and one recursive CTE (number sequence)
	// WITH RECURSIVE cats AS (...), nums(n) AS (...)
	// SELECT c.name, n FROM cats, nums WHERE n <= 2 ORDER BY c.name, n
	cte := sqrl.With("cats",
		sb.Select("name").From("sq_categories").Where(sqrl.Eq{"name": "fruit"}),
	).WithRecursiveColumns("nums", []string{"n"},
		sqrl.UnionAll(
			sqrl.Select("1"),
			sqrl.Select("n + 1").From("nums").Where("n < ?", 3),
		),
	).Statement(
		sqrl.Select("cats.name", "nums.n").From("cats").
			CrossJoin("nums").
			Where("nums.n <= ?", 2).
			OrderBy("cats.name", "nums.n"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	rows, err := cte.Query()
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		Name string
		N    int
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.Name, &r.N))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Assert — fruit × {1, 2}
	assert.Equal(t, []row{
		{"fruit", 1},
		{"fruit", 2},
	}, results)
}

// ---------------------------------------------------------------------------
// CTE with INSERT as main statement
// ---------------------------------------------------------------------------

func TestCteWithInsertStatement(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support WITH ... INSERT syntax (CTE must be inside INSERT ... SELECT)")
	}

	// Arrange — CTE selects fruit items, INSERT inserts them into a new table
	createTable(t, "sq_cte_ins", "(id INTEGER, name TEXT)")

	cte := sqrl.With("fruit_data",
		sb.Select("id", "name").From("sq_items").Where(sqrl.Eq{"category": "fruit"}),
	).Statement(
		sqrl.Insert("sq_cte_ins").Columns("id", "name").
			Select(sqrl.Select("id", "name").From("fruit_data")),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	_, err := cte.Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_cte_ins").OrderBy("id"))
	assert.Equal(t, []string{"apple", "banana"}, names)
}

func TestCteWithInsertFromRecursive(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support WITH ... INSERT syntax (CTE must be inside INSERT ... SELECT)")
	}

	// Arrange — recursive CTE generates 1..5, INSERT puts them into a table
	createTable(t, "sq_cte_ins_r", "(n INTEGER)")

	cte := sqrl.WithRecursiveColumns("seq", []string{"n"},
		sqrl.UnionAll(
			sqrl.Select("1"),
			sqrl.Select("n + 1").From("seq").Where("n < ?", 5),
		),
	).Statement(
		sqrl.Insert("sq_cte_ins_r").Columns("n").
			Select(sqrl.Select("n").From("seq")),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	_, err := cte.Exec()

	// Assert
	require.NoError(t, err)

	vals := queryInts(t, sb.Select("n").From("sq_cte_ins_r").OrderBy("n"))
	assert.Equal(t, []int{1, 2, 3, 4, 5}, vals)
}

// ---------------------------------------------------------------------------
// CTE with UPDATE as main statement (PostgreSQL & SQLite)
// ---------------------------------------------------------------------------

func TestCteWithUpdateStatement(t *testing.T) {
	if isMySQL() {
		t.Skip("CTE with UPDATE not supported on this MySQL version or syntax differs")
	}

	// Arrange
	createTable(t, "sq_cte_upd", "(id INTEGER, name TEXT, price INTEGER)")
	seedTable(t, `INSERT INTO sq_cte_upd VALUES (1, 'item_a', 100), (2, 'item_b', 200), (3, 'item_c', 300)`)

	// CTE selects items with price > 150, then UPDATE sets price = 0 for those
	cte := sqrl.With("expensive",
		sqrl.Select("id").From("sq_cte_upd").Where("price > ?", 150).PlaceholderFormat(phf()),
	).Statement(
		sqrl.Update("sq_cte_upd").Set("price", 0).
			Where("id IN (SELECT id FROM expensive)"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	_, err := cte.Exec()

	// Assert
	require.NoError(t, err)

	prices := queryInts(t, sb.Select("price").From("sq_cte_upd").OrderBy("id"))
	assert.Equal(t, []int{100, 0, 0}, prices)
}

// ---------------------------------------------------------------------------
// CTE with DELETE as main statement (PostgreSQL & SQLite)
// ---------------------------------------------------------------------------

func TestCteWithDeleteStatement(t *testing.T) {
	if isMySQL() {
		t.Skip("CTE with DELETE not supported on this MySQL version or syntax differs")
	}

	// Arrange
	createTable(t, "sq_cte_del", "(id INTEGER, name TEXT, price INTEGER)")
	seedTable(t, `INSERT INTO sq_cte_del VALUES (1, 'keep', 50), (2, 'remove', 200), (3, 'remove2', 300)`)

	// CTE finds expensive items, DELETE removes them
	cte := sqrl.With("to_delete",
		sqrl.Select("id").From("sq_cte_del").Where("price > ?", 100).PlaceholderFormat(phf()),
	).Statement(
		sqrl.Delete("sq_cte_del").Where("id IN (SELECT id FROM to_delete)"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	_, err := cte.Exec()

	// Assert
	require.NoError(t, err)

	names := queryStrings(t, sb.Select("name").From("sq_cte_del"))
	assert.Equal(t, []string{"keep"}, names)
}

// ---------------------------------------------------------------------------
// CTE with UNION as main statement
// ---------------------------------------------------------------------------

func TestCteWithUnionStatement(t *testing.T) {
	// Arrange — CTE selects fruit items, main statement UNIONs CTE with vegetable items
	cte := sqrl.With("fruit_cte",
		sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"}),
	).Statement(
		sqrl.Union(
			sqrl.Select("name").From("fruit_cte"),
			sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "vegetable"}),
		).OrderBy("name"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	names := cteQueryStrings(t, cte)

	// Assert
	assert.Equal(t, []string{"apple", "banana", "carrot", "eggplant"}, names)
}

// ---------------------------------------------------------------------------
// CTE with aggregation in CTE body
// ---------------------------------------------------------------------------

func TestCteWithAggregation(t *testing.T) {
	t.Run("SumPerCategory", func(t *testing.T) {
		// Arrange — CTE computes sum per category, main query orders
		cte := sqrl.With("cat_totals",
			sb.Select("category", "SUM(price) as total").From("sq_items").
				Where(sqrl.NotEq{"category": nil}).
				GroupBy("category"),
		).Statement(
			sqrl.Select("category", "total").From("cat_totals").OrderBy("category"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		rows, err := cte.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Category string
			Total    int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Category, &r.Total))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert
		assert.Equal(t, []result{
			{"fruit", 150},     // apple(100) + banana(50)
			{"pastry", 200},    // donut(200)
			{"vegetable", 225}, // carrot(75) + eggplant(150)
		}, results)
	})

	t.Run("MaxPricePerCategory", func(t *testing.T) {
		// Arrange — CTE finds max price per category, main query finds
		// which item has that max price
		cte := sqrl.With("max_prices",
			sb.Select("category", "MAX(price) as max_price").From("sq_items").
				Where(sqrl.NotEq{"category": nil}).
				GroupBy("category"),
		).Statement(
			sqrl.Select("sq_items.name", "sq_items.price").
				From("sq_items").
				Join("max_prices ON sq_items.category = max_prices.category AND sq_items.price = max_prices.max_price").
				OrderBy("sq_items.name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		rows, err := cte.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Name  string
			Price int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Name, &r.Price))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — most expensive per category
		assert.Equal(t, []result{
			{"apple", 100},
			{"donut", 200},
			{"eggplant", 150},
		}, results)
	})
}

// ---------------------------------------------------------------------------
// CTE with subquery in WHERE — realistic filtering pattern
// ---------------------------------------------------------------------------

func TestCteSubqueryFiltering(t *testing.T) {
	// Arrange — find items whose category has more than 1 item
	// CTE computes category counts, main query filters
	cte := sqrl.With("cat_counts",
		sb.Select("category", "COUNT(*) as cnt").From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category"),
	).Statement(
		sqrl.Select("sq_items.name").From("sq_items").
			Where("sq_items.category IN (SELECT category FROM cat_counts WHERE cnt > ?)", 1).
			OrderBy("sq_items.name"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	names := cteQueryStrings(t, cte)

	// Assert — fruit has 2 items, vegetable has 2 items; pastry only has 1
	assert.Equal(t, []string{"apple", "banana", "carrot", "eggplant"}, names)
}

// ---------------------------------------------------------------------------
// CTE used for data deduplication — a common real-world pattern
// ---------------------------------------------------------------------------

func TestCteDeduplicate(t *testing.T) {
	// Arrange — create a table with duplicate rows
	createTable(t, "sq_cte_dup", "(id INTEGER, name TEXT, category TEXT)")
	seedTable(t, `INSERT INTO sq_cte_dup VALUES
		(1, 'apple', 'fruit'),
		(2, 'apple', 'fruit'),
		(3, 'banana', 'fruit'),
		(4, 'banana', 'fruit'),
		(5, 'carrot', 'vegetable')`)

	// CTE selects distinct names, main query reads unique names
	cte := sqrl.With("uniq",
		sqrl.Select("DISTINCT name").From("sq_cte_dup"),
	).Statement(
		sqrl.Select("name").From("uniq").OrderBy("name"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	names := cteQueryStrings(t, cte)

	// Assert
	assert.Equal(t, []string{"apple", "banana", "carrot"}, names)
}

// ---------------------------------------------------------------------------
// CTE with CASE expression inside — complex real-world query
// ---------------------------------------------------------------------------

func TestCteWithCaseExpression(t *testing.T) {
	// Arrange — CTE classifies items by price tier, main query groups
	caseExpr := sqrl.Case().
		When("price >= 150", "'premium'").
		When("price >= 75", "'standard'").
		Else("'budget'")

	cte := sqrl.With("tiered_items",
		sb.Select("name").Column(sqrl.Alias(caseExpr, "tier")).From("sq_items").
			Where(sqrl.NotEq{"category": nil}),
	).Statement(
		sqrl.Select("tier", "COUNT(*) as cnt").From("tiered_items").
			GroupBy("tier").
			OrderBy("tier"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	rows, err := cte.Query()
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		Tier  string
		Count int
	}
	var results []result
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.Tier, &r.Count))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Assert
	// budget: banana(50)
	// premium: donut(200), eggplant(150)
	// standard: apple(100), carrot(75)
	assert.Equal(t, []result{
		{"budget", 1},
		{"premium", 2},
		{"standard", 2},
	}, results)
}

// ---------------------------------------------------------------------------
// CTE Scan and QueryRow
// ---------------------------------------------------------------------------

func TestCteScan(t *testing.T) {
	// Arrange — scalar CTE result
	cte := sqrl.With("total_cte",
		sb.Select("SUM(price) as total").From("sq_items").
			Where(sqrl.Eq{"category": "fruit"}),
	).Statement(
		sqrl.Select("total").From("total_cte"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	var total int
	err := cte.Scan(&total)

	// Assert — apple(100) + banana(50) = 150
	require.NoError(t, err)
	assert.Equal(t, 150, total)
}

func TestCteQueryRow(t *testing.T) {
	// Arrange
	cte := sqrl.With("count_cte",
		sb.Select("COUNT(*) as cnt").From("sq_items"),
	).Statement(
		sqrl.Select("cnt").From("count_cte"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	var count int
	err := cte.QueryRow().Scan(&count)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 6, count)
}

// ---------------------------------------------------------------------------
// CTE Suffix
// ---------------------------------------------------------------------------

func TestCteSuffix(t *testing.T) {
	// Arrange — use Suffix for LIMIT (portable across all databases)
	cte := sqrl.With("all_items",
		sb.Select("name").From("sq_items").Where(sqrl.NotEq{"category": nil}).OrderBy("name"),
	).Statement(
		sqrl.Select("name").From("all_items").OrderBy("name"),
	).Suffix("LIMIT 2").
		RunWith(db).PlaceholderFormat(phf())

	// Act
	names := cteQueryStrings(t, cte)

	// Assert — first 2 alphabetically
	assert.Equal(t, []string{"apple", "banana"}, names)
}

// ---------------------------------------------------------------------------
// CTE with Dollar placeholder format (PostgreSQL-style)
// ---------------------------------------------------------------------------

func TestCteDollarPlaceholders(t *testing.T) {
	// Arrange — force Dollar format regardless of driver
	// We just verify the SQL is generated correctly; we can execute only on PG
	cte := sqrl.With("cte_d",
		sqrl.Select("id", "name").From("sq_items").Where(sqrl.Eq{"category": "fruit"}),
	).Statement(
		sqrl.Select("name").From("cte_d").Where(sqrl.Eq{"id": 1}),
	).PlaceholderFormat(sqrl.Dollar)

	// Act
	sqlStr, args, err := cte.ToSQL()

	// Assert — verify Dollar placeholders are correctly numbered
	require.NoError(t, err)
	assert.Contains(t, sqlStr, "$1")
	assert.Contains(t, sqlStr, "$2")
	assert.Equal(t, []any{"fruit", 1}, args)
}

// ---------------------------------------------------------------------------
// CTE — error handling
// ---------------------------------------------------------------------------

func TestCteErrorCases(t *testing.T) {
	t.Run("NoCTEDefinitions", func(t *testing.T) {
		_, _, err := sqrl.CteBuilder{}.
			Statement(sqrl.Select("1")).
			PlaceholderFormat(sqrl.Question).
			ToSQL()
		// CteBuilder{} has no CTEs registered via init — need to use the builder properly.
		// The zero-value CteBuilder won't have any data.
		assert.Error(t, err)
	})

	t.Run("NoMainStatement", func(t *testing.T) {
		_, _, err := sqrl.With("cte", sqrl.Select("1")).ToSQL()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "main statement")
	})

	t.Run("CTEWithInvalidInnerQuery", func(t *testing.T) {
		// Select() without columns → error
		_, _, err := sqrl.With("bad_cte", sqrl.Select()).
			Statement(sqrl.Select("1")).
			ToSQL()
		assert.Error(t, err)
	})

	t.Run("MainStatementWithError", func(t *testing.T) {
		// Main statement has no columns → error
		_, _, err := sqrl.With("good_cte", sqrl.Select("1")).
			Statement(sqrl.Select()).
			ToSQL()
		assert.Error(t, err)
	})

	t.Run("NoRunnerExec", func(t *testing.T) {
		cte := sqrl.With("c", sqrl.Select("1")).
			Statement(sqrl.Select("*").From("c"))
		_, err := cte.Exec()
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerQuery", func(t *testing.T) {
		cte := sqrl.With("c", sqrl.Select("1")).
			Statement(sqrl.Select("*").From("c"))
		_, err := cte.Query()
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("NoRunnerScan", func(t *testing.T) {
		cte := sqrl.With("c", sqrl.Select("1")).
			Statement(sqrl.Select("*").From("c"))
		err := cte.Scan()
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})

	t.Run("MustSQLPanics", func(t *testing.T) {
		assert.Panics(t, func() {
			sqrl.With("cte", sqrl.Select("1")).MustSQL() // no Statement
		})
	})
}

// ---------------------------------------------------------------------------
// CTE — context-aware methods
// ---------------------------------------------------------------------------

func TestCteContextMethods(t *testing.T) {
	ctx := context.Background()

	t.Run("ExecContext", func(t *testing.T) {
		if isMySQL() {
			t.Skip("MySQL does not support WITH ... INSERT syntax")
		}
		createTable(t, "sq_cte_ctx_e", "(n INTEGER)")

		cte := sqrl.WithRecursiveColumns("seq", []string{"n"},
			sqrl.UnionAll(
				sqrl.Select("1"),
				sqrl.Select("n + 1").From("seq").Where("n < ?", 3),
			),
		).Statement(
			sqrl.Insert("sq_cte_ctx_e").Columns("n").
				Select(sqrl.Select("n").From("seq")),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		_, err := cte.ExecContext(ctx)

		// Assert
		require.NoError(t, err)

		vals := queryInts(t, sb.Select("n").From("sq_cte_ctx_e").OrderBy("n"))
		assert.Equal(t, []int{1, 2, 3}, vals)
	})

	t.Run("QueryContext", func(t *testing.T) {
		cte := sqrl.With("ctx_cte",
			sb.Select("name").From("sq_items").Where(sqrl.Eq{"category": "fruit"}),
		).Statement(
			sqrl.Select("name").From("ctx_cte").OrderBy("name"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		rows, err := cte.QueryContext(ctx)
		require.NoError(t, err)
		defer rows.Close()

		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Err())

		// Assert
		assert.Equal(t, []string{"apple", "banana"}, names)
	})

	t.Run("QueryRowContext", func(t *testing.T) {
		cte := sqrl.With("ctx_cnt",
			sb.Select("COUNT(*) as c").From("sq_items"),
		).Statement(
			sqrl.Select("c").From("ctx_cnt"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		var count int
		err := cte.QueryRowContext(ctx).Scan(&count)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, 6, count)
	})

	t.Run("ScanContext", func(t *testing.T) {
		cte := sqrl.With("ctx_sum",
			sb.Select("SUM(price) as total").From("sq_items").
				Where(sqrl.Eq{"category": "fruit"}),
		).Statement(
			sqrl.Select("total").From("ctx_sum"),
		).RunWith(db).PlaceholderFormat(phf())

		// Act
		var total int
		err := cte.ScanContext(ctx, &total)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, 150, total)
	})

	t.Run("CancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		cte := sqrl.With("cte_c",
			sb.Select("name").From("sq_items"),
		).Statement(
			sqrl.Select("name").From("cte_c"),
		).RunWith(db).PlaceholderFormat(phf())

		_, err := cte.QueryContext(ctx)
		assert.Error(t, err)
	})

	t.Run("ContextNoRunner", func(t *testing.T) {
		cte := sqrl.With("c", sqrl.Select("1")).
			Statement(sqrl.Select("*").From("c"))

		_, err := cte.ExecContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)

		_, err = cte.QueryContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)

		err = cte.ScanContext(ctx)
		assert.Equal(t, sqrl.ErrRunnerNotSet, err)
	})
}

// ---------------------------------------------------------------------------
// CTE — real-world pagination pattern
// ---------------------------------------------------------------------------

func TestCtePagination(t *testing.T) {
	// A common pattern: CTE computes the total count, main query paginates.
	// WITH total AS (SELECT COUNT(*) as cnt FROM items WHERE category = ?),
	//      page AS (SELECT name FROM items WHERE category = ? ORDER BY name LIMIT 2 OFFSET 0)
	// SELECT page.name, total.cnt FROM page, total

	cte := sqrl.With("total",
		sb.Select("COUNT(*) as cnt").From("sq_items").
			Where(sqrl.NotEq{"category": nil}),
	).With("page",
		sb.Select("name").From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			OrderBy("name").Limit(2).Offset(0),
	).Statement(
		sqrl.Select("page.name", "total.cnt").From("page").CrossJoin("total"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	rows, err := cte.Query()
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		Name  string
		Total int
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.Name, &r.Total))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Assert — 2 items from page (first 2 alpha), each with total 5
	require.Len(t, results, 2)
	assert.Equal(t, "apple", results[0].Name)
	assert.Equal(t, "banana", results[1].Name)
	assert.Equal(t, 5, results[0].Total)
	assert.Equal(t, 5, results[1].Total)
}

// ---------------------------------------------------------------------------
// CTE — running totals / window-function-like via recursive CTE
// ---------------------------------------------------------------------------

func TestCteRunningTotal(t *testing.T) {
	// Use a recursive CTE to simulate a running total over fruit items.
	// This tests complex recursive CTE with multiple columns and arithmetic.
	//
	// Approach: CTE computes row numbers for fruits ordered by id,
	// then a recursive CTE accumulates prices.
	//
	// Simpler approach for portability: just use a non-recursive CTE with
	// a self-join to compute running totals.

	cte := sqrl.With("fruit_prices",
		sb.Select("id", "name", "price").From("sq_items").
			Where(sqrl.Eq{"category": "fruit"}),
	).Statement(
		sqrl.Select("f1.name", "SUM(f2.price) as running_total").
			From("fruit_prices f1").
			Join("fruit_prices f2 ON f2.id <= f1.id").
			GroupBy("f1.id", "f1.name").
			OrderBy("f1.id"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	rows, err := cte.Query()
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		Name         string
		RunningTotal int
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.Name, &r.RunningTotal))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Assert — apple=100 (running:100), banana=50 (running:150)
	assert.Equal(t, []row{
		{"apple", 100},
		{"banana", 150},
	}, results)
}

// ---------------------------------------------------------------------------
// CTE ToSQL — verify generated SQL structure
// ---------------------------------------------------------------------------

func TestCteToSQL(t *testing.T) {
	t.Run("SimpleWithQuestion", func(t *testing.T) {
		sqlStr, args, err := sqrl.With("c",
			sqrl.Select("id").From("t").Where(sqrl.Eq{"x": 1}),
		).Statement(
			sqrl.Select("id").From("c"),
		).ToSQL()

		require.NoError(t, err)
		assert.Equal(t, "WITH c AS (SELECT id FROM t WHERE x = ?) SELECT id FROM c", sqlStr)
		assert.Equal(t, []any{1}, args)
	})

	t.Run("RecursiveWithDollar", func(t *testing.T) {
		sqlStr, args, err := sqrl.WithRecursiveColumns("cnt", []string{"n"},
			sqrl.UnionAll(
				sqrl.Select("1"),
				sqrl.Select("n + 1").From("cnt").Where("n < ?", 10),
			),
		).Statement(
			sqrl.Select("n").From("cnt").Where("n > ?", 5),
		).PlaceholderFormat(sqrl.Dollar).ToSQL()

		require.NoError(t, err)
		assert.Contains(t, sqlStr, "WITH RECURSIVE")
		assert.Contains(t, sqlStr, "cnt (n)")
		assert.Contains(t, sqlStr, "$1")
		assert.Contains(t, sqlStr, "$2")
		assert.Equal(t, []any{10, 5}, args)
	})

	t.Run("MultipleCTEsWithSuffix", func(t *testing.T) {
		sqlStr, _, err := sqrl.With("a",
			sqrl.Select("1 as x"),
		).With("b",
			sqrl.Select("2 as y"),
		).Statement(
			sqrl.Select("x", "y").From("a").Join("b"),
		).Suffix("LIMIT 1").ToSQL()

		require.NoError(t, err)
		assert.Contains(t, sqlStr, "WITH a AS")
		assert.Contains(t, sqlStr, ", b AS")
		assert.Contains(t, sqlStr, "LIMIT 1")
	})
}

// ---------------------------------------------------------------------------
// CTE — complex real-world scenario: category summary report
// ---------------------------------------------------------------------------

func TestCteCategorySummaryReport(t *testing.T) {
	// A realistic business query: produce a category summary with
	// item count, total price, average price, and the name of the most
	// expensive item per category.
	//
	// WITH
	//   cat_stats AS (SELECT category, COUNT(*) as cnt, SUM(price) as total FROM ... GROUP BY category),
	//   max_item AS (SELECT i.category, i.name as top_item, i.price
	//                FROM sq_items i JOIN (SELECT category, MAX(price) as mp FROM sq_items GROUP BY category) m
	//                ON i.category = m.category AND i.price = m.mp)
	// SELECT cat_stats.category, cnt, total, top_item
	// FROM cat_stats JOIN max_item ON cat_stats.category = max_item.category
	// ORDER BY cat_stats.category

	cte := sqrl.With("cat_stats",
		sb.Select("category", "COUNT(*) as cnt", "SUM(price) as total").
			From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category"),
	).With("max_prices",
		sb.Select("category", "MAX(price) as mp").
			From("sq_items").
			Where(sqrl.NotEq{"category": nil}).
			GroupBy("category"),
	).With("max_item",
		sqrl.Select("i.category", "i.name as top_item").
			From("sq_items i").
			Join("max_prices mp ON i.category = mp.category AND i.price = mp.mp"),
	).Statement(
		sqrl.Select("cat_stats.category", "cnt", "total", "top_item").
			From("cat_stats").
			Join("max_item ON cat_stats.category = max_item.category").
			OrderBy("cat_stats.category"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	rows, err := cte.Query()
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		Category string
		Count    int
		Total    int
		TopItem  string
	}
	var results []result
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.Category, &r.Count, &r.Total, &r.TopItem))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Assert
	assert.Equal(t, []result{
		{"fruit", 2, 150, "apple"},
		{"pastry", 1, 200, "donut"},
		{"vegetable", 2, 225, "eggplant"},
	}, results)
}

// ---------------------------------------------------------------------------
// CTE — immutability: builder calls return new instances
// ---------------------------------------------------------------------------

func TestCteImmutability(t *testing.T) {
	base := sqrl.With("base_cte", sqrl.Select("1 as n"))

	// Adding a second CTE should not affect the original
	extended := base.With("extra", sqrl.Select("2 as m"))

	// Setting the statement on extended should not affect base
	final := extended.Statement(sqrl.Select("n", "m").From("base_cte").Join("extra"))

	// base should still error (no Statement set)
	_, _, err := base.Statement(sqrl.Select("n").From("base_cte")).ToSQL()
	require.NoError(t, err) // this one is fine — we add Statement here

	// Verify the final query uses both CTEs
	sqlStr, _, err := final.ToSQL()
	require.NoError(t, err)
	assert.Contains(t, sqlStr, "base_cte")
	assert.Contains(t, sqlStr, "extra")

	// Verify that adding Statement to base doesn't affect 'final'
	baseFinal := base.Statement(sqrl.Select("n").From("base_cte"))
	bsql, _, err := baseFinal.ToSQL()
	require.NoError(t, err)
	assert.NotContains(t, bsql, "extra") // base should NOT have the extra CTE
}

// ---------------------------------------------------------------------------
// CTE — Exec returns correct RowsAffected
// ---------------------------------------------------------------------------

func TestCteExecRowsAffected(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support WITH ... INSERT syntax")
	}

	// Arrange
	createTable(t, "sq_cte_ra", "(n INTEGER)")

	cte := sqrl.WithRecursiveColumns("seq", []string{"n"},
		sqrl.UnionAll(
			sqrl.Select("1"),
			sqrl.Select("n + 1").From("seq").Where("n < ?", 3),
		),
	).Statement(
		sqrl.Insert("sq_cte_ra").Columns("n").
			Select(sqrl.Select("n").From("seq")),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	res, err := cte.Exec()

	// Assert
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(3), affected)
}

// ---------------------------------------------------------------------------
// CTE — NULL handling in CTE results
// ---------------------------------------------------------------------------

func TestCteNullHandling(t *testing.T) {
	// Item #6 "mystery" has category=NULL. Verify it can be selected
	// through a CTE and NULL is preserved.
	cte := sqrl.With("nullable",
		sb.Select("id", "name", "category").From("sq_items").Where(sqrl.Eq{"id": 6}),
	).Statement(
		sqrl.Select("name", "category").From("nullable"),
	).RunWith(db).PlaceholderFormat(phf())

	// Act
	rows, err := cte.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	var category sql.NullString
	require.NoError(t, rows.Scan(&name, &category))

	// Assert
	assert.Equal(t, "mystery", name)
	assert.False(t, category.Valid) // NULL
}
