package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// CASE WHEN ... THEN ... ELSE ... END
// ---------------------------------------------------------------------------

func TestCaseSimple(t *testing.T) {
	// Arrange — CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END
	caseExpr := sqrl.Case().
		When("price > 100", "'expensive'").
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

	// Assert — donut price 200 > 100 → expensive
	assert.Equal(t, "donut", name)
	assert.Equal(t, "expensive", tier)
}

func TestCaseWithValue(t *testing.T) {
	// Arrange — CASE category WHEN 'fruit' THEN 'yes' ELSE 'no' END
	caseExpr := sqrl.Case("category").
		When("'fruit'", "'yes'").
		When("'vegetable'", "'maybe'").
		Else("'no'")

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "is_fruit")).
		From("sq_items").
		Where(sqrl.NotEq{"category": nil}).
		OrderBy("id")

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		Name    string
		IsFruit string
	}
	var results []result
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.Name, &r.IsFruit))
		results = append(results, r)
	}

	// Assert
	assert.Equal(t, []result{
		{"apple", "yes"},
		{"banana", "yes"},
		{"carrot", "maybe"},
		{"donut", "no"},
		{"eggplant", "maybe"},
	}, results)
}

func TestCaseWithPlaceholders(t *testing.T) {
	// Arrange — CASE WHEN price > ? THEN 'high' ELSE 'low' END
	caseExpr := sqrl.Case().
		When(sqrl.Expr("price > ?", 100), "'high'").
		Else("'low'")

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "level")).
		From("sq_items").
		Where(sqrl.Eq{"id": 4})

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name, level string
	require.NoError(t, rows.Scan(&name, &level))

	// Assert
	assert.Equal(t, "donut", name)
	assert.Equal(t, "high", level)
}

func TestCaseNoElse(t *testing.T) {
	// Arrange — CASE without ELSE → NULL when no match
	caseExpr := sqrl.Case().
		When("category = 'fruit'", "'yes'")

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "is_fruit")).
		From("sq_items").
		Where(sqrl.Eq{"id": 4}) // donut is pastry

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	var isFruit *string
	require.NoError(t, rows.Scan(&name, &isFruit))

	// Assert — donut is not fruit, no ELSE → NULL
	assert.Equal(t, "donut", name)
	assert.Nil(t, isFruit)
}

func TestCaseMultipleWhens(t *testing.T) {
	// Arrange — multiple WHEN clauses
	caseExpr := sqrl.Case().
		When("price < 60", "'low'").
		When("price < 120", "'mid'").
		When("price < 200", "'high'").
		Else("'premium'")

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "bracket")).
		From("sq_items").
		OrderBy("id")

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		Name    string
		Bracket string
	}
	var results []result
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.Name, &r.Bracket))
		results = append(results, r)
	}

	// Assert
	assert.Equal(t, []result{
		{"apple", "mid"},     // 100
		{"banana", "low"},    // 50
		{"carrot", "mid"},    // 75
		{"donut", "premium"}, // 200 (not < 200)
		{"eggplant", "high"}, // 150
		{"mystery", "mid"},   // 99
	}, results)
}

// ---------------------------------------------------------------------------
// ToSQL and MustSQL
// ---------------------------------------------------------------------------

func TestCaseToSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		c := sqrl.Case().When("a = 1", "x").Else("y")

		// Act
		sqlStr, _, err := c.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "CASE WHEN a = 1 THEN x ELSE y END", sqlStr)
	})

	t.Run("WithValue", func(t *testing.T) {
		// Arrange
		c := sqrl.Case("status").When("1", "'active'").Else("'inactive'")

		// Act
		sqlStr, _, err := c.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "CASE status WHEN 1 THEN 'active' ELSE 'inactive' END", sqlStr)
	})
}

func TestCaseMustSQL(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		// Arrange
		c := sqrl.Case().When("1=1", "1")

		// Act
		sqlStr, _ := c.MustSQL()

		// Assert
		assert.Contains(t, sqlStr, "CASE")
	})

	t.Run("PanicsOnError", func(t *testing.T) {
		// Arrange — no WHEN clauses
		c := sqrl.Case()

		// Act & Assert
		assert.Panics(t, func() { c.MustSQL() })
	})
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

func TestCaseErrors(t *testing.T) {
	t.Run("NoWhenClauses", func(t *testing.T) {
		// Arrange
		c := sqrl.Case()

		// Act
		_, _, err := c.ToSQL()

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at lease one WHEN clause")
	})
}

// ---------------------------------------------------------------------------
// CASE in WHERE clause
// ---------------------------------------------------------------------------

func TestCaseInWhere(t *testing.T) {
	// Arrange — use CASE in WHERE to filter rows
	caseExpr := sqrl.Case().
		When("category = 'fruit'", "1").
		Else("0")

	q := sb.Select("name").From("sq_items").
		Where(sqrl.Expr("? = 1", caseExpr)).
		OrderBy("id")

	// Act
	names := queryStrings(t, q)

	// Assert
	assert.Equal(t, []string{"apple", "banana"}, names)
}

// ---------------------------------------------------------------------------
// CASE builder immutability
// ---------------------------------------------------------------------------

func TestCaseBuilderImmutability(t *testing.T) {
	// Arrange
	base := sqrl.Case().When("a = 1", "'x'")
	c1 := base.Else("'y'")
	c2 := base.Else("'z'")

	// Act
	sql1, _, err1 := c1.ToSQL()
	sql2, _, err2 := c2.ToSQL()

	// Assert — each branch is independent
	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Contains(t, sql1, "'y'")
	assert.NotContains(t, sql1, "'z'")
	assert.Contains(t, sql2, "'z'")
	assert.NotContains(t, sql2, "'y'")
}

// ---------------------------------------------------------------------------
// Non-string values in CASE WHEN/THEN/ELSE (GitHub #388)
// ---------------------------------------------------------------------------

func TestCaseWithIntThenValue(t *testing.T) {
	// Arrange — CASE WHEN category = 'fruit' THEN 1 ELSE 0 END
	caseExpr := sqrl.Case().
		When("category = 'fruit'", 1).
		Else(0)

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "is_fruit")).
		From("sq_items").
		Where(sqrl.Eq{"id": 1})

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	var isFruit int
	require.NoError(t, rows.Scan(&name, &isFruit))

	// Assert — apple is fruit → 1
	assert.Equal(t, "apple", name)
	assert.Equal(t, 1, isFruit)
}

func TestCaseWithIntThenElseValues(t *testing.T) {
	// Arrange — non-fruit should get ELSE value 0
	caseExpr := sqrl.Case().
		When("category = 'fruit'", 1).
		Else(0)

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "is_fruit")).
		From("sq_items").
		Where(sqrl.Eq{"id": 4}) // donut is pastry

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name string
	var isFruit int
	require.NoError(t, rows.Scan(&name, &isFruit))

	// Assert — donut is not fruit → 0
	assert.Equal(t, "donut", name)
	assert.Equal(t, 0, isFruit)
}

func TestCaseWithIntWhenValue(t *testing.T) {
	// Arrange — CASE id WHEN ? THEN 'match' ELSE 'no match' END
	// Using int in the WHEN position.
	caseExpr := sqrl.Case("id").
		When(1, "'found'").
		Else("'not found'")

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "status")).
		From("sq_items").
		Where(sqrl.Eq{"id": 1})

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var name, status string
	require.NoError(t, rows.Scan(&name, &status))

	// Assert
	assert.Equal(t, "apple", name)
	assert.Equal(t, "found", status)
}

func TestCaseWithMixedNonStringValues(t *testing.T) {
	// Arrange — mix of int THEN values across multiple WHEN clauses
	caseExpr := sqrl.Case().
		When("price < 80", 1).
		When("price < 150", 2).
		Else(3)

	q := sb.Select("name").Column(sqrl.Alias(caseExpr, "tier")).
		From("sq_items").
		OrderBy("id")

	// Act
	rows, err := q.Query()
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		Name string
		Tier int
	}
	var results []result
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.Name, &r.Tier))
		results = append(results, r)
	}

	// Assert
	assert.Equal(t, []result{
		{"apple", 2},    // 100 → tier 2
		{"banana", 1},   // 50 → tier 1
		{"carrot", 1},   // 75 → tier 1
		{"donut", 3},    // 200 → tier 3
		{"eggplant", 3}, // 150 → tier 3 (not < 150)
		{"mystery", 2},  // 99 → tier 2
	}, results)
}
