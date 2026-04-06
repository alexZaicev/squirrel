package integration

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

// ---------------------------------------------------------------------------
// Test-local schema for JOIN USING and FULL OUTER JOIN tests.
//
// We need tables with matching column names to test USING. The existing
// sq_items/sq_categories tables use different column names for the join key
// (sq_items.category vs sq_categories.name), so USING cannot be used there.
// ---------------------------------------------------------------------------

const (
	schemaOrders = `(
		id        INTEGER,
		region_id INTEGER,
		amount    INTEGER
	)`
	schemaRegions = `(
		region_id   INTEGER,
		region_name TEXT
	)`
	schemaWarehouses = `(
		id        INTEGER,
		region_id INTEGER,
		capacity  INTEGER
	)`

	seedOrders = `INSERT INTO sq_orders (id, region_id, amount) VALUES
		(1, 10, 500),
		(2, 10, 300),
		(3, 20, 700),
		(4, 30, 100),
		(5, 99, 50)`

	seedRegions = `INSERT INTO sq_regions (region_id, region_name) VALUES
		(10, 'North'),
		(20, 'South'),
		(30, 'East'),
		(40, 'West')`

	seedWarehouses = `INSERT INTO sq_warehouses (id, region_id, capacity) VALUES
		(1, 10, 1000),
		(2, 20, 2000),
		(3, 30, 500),
		(4, 10, 750)`
)

func setupJoinTables(t *testing.T) {
	t.Helper()
	createTable(t, "sq_orders", schemaOrders)
	createTable(t, "sq_regions", schemaRegions)
	createTable(t, "sq_warehouses", schemaWarehouses)
	seedTable(t, seedOrders)
	seedTable(t, seedRegions)
	seedTable(t, seedWarehouses)
}

// ---------------------------------------------------------------------------
// JOIN ... USING — basic
// ---------------------------------------------------------------------------

func TestJoinUsing(t *testing.T) {
	setupJoinTables(t)

	t.Run("BasicJoinUsing", func(t *testing.T) {
		// Arrange — JOIN orders to regions on region_id
		q := sb.Select("sq_orders.id", "sq_regions.region_name").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			OrderBy("sq_orders.id")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			ID   int
			Name string
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.ID, &r.Name))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — order 5 (region_id=99) has no match, excluded by inner join
		assert.Equal(t, []result{
			{1, "North"},
			{2, "North"},
			{3, "South"},
			{4, "East"},
		}, results)
	})

	t.Run("JoinUsingExcludesNonMatching", func(t *testing.T) {
		// Arrange — verify the non-matching row is excluded
		q := sb.Select("sq_orders.id").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			Where(sqrl.Eq{"sq_orders.id": 5})

		// Act
		ids := queryInts(t, q)

		// Assert — order 5 has region_id=99 which doesn't exist in sq_regions
		assert.Empty(t, ids)
	})

	t.Run("JoinUsingWithWhere", func(t *testing.T) {
		// Arrange — USING join combined with WHERE filter
		q := sb.Select("sq_orders.id").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			Where(sqrl.Eq{"sq_regions.region_name": "North"}).
			OrderBy("sq_orders.id")

		// Act
		ids := queryInts(t, q)

		// Assert — orders 1 and 2 are in region 10 (North)
		assert.Equal(t, []int{1, 2}, ids)
	})
}

// ---------------------------------------------------------------------------
// LEFT JOIN ... USING
// ---------------------------------------------------------------------------

func TestLeftJoinUsing(t *testing.T) {
	setupJoinTables(t)

	t.Run("PreservesAllLeftRows", func(t *testing.T) {
		// Arrange — LEFT JOIN keeps all orders, even those without a matching region
		q := sb.Select("sq_orders.id", "sq_regions.region_name").
			From("sq_orders").
			LeftJoinUsing("sq_regions", "region_id").
			OrderBy("sq_orders.id")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			ID   int
			Name sql.NullString
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.ID, &r.Name))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — all 5 orders; order 5 has NULL region_name
		assert.Len(t, results, 5)
		assert.Equal(t, 5, results[4].ID)
		assert.False(t, results[4].Name.Valid, "order 5 should have NULL region_name")
	})
}

// ---------------------------------------------------------------------------
// RIGHT JOIN ... USING
// ---------------------------------------------------------------------------

func TestRightJoinUsing(t *testing.T) {
	setupJoinTables(t)

	t.Run("PreservesAllRightRows", func(t *testing.T) {
		// Arrange — RIGHT JOIN keeps all regions, even those without orders
		q := sb.Select("sq_regions.region_name", "sq_orders.id").
			From("sq_orders").
			RightJoinUsing("sq_regions", "region_id").
			OrderBy("sq_regions.region_name")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Name   string
			ItemID sql.NullInt64
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Name, &r.ItemID))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — 'West' (region_id=40) has no orders so ItemID is NULL
		var westFound bool
		for _, r := range results {
			if r.Name == "West" {
				westFound = true
				assert.False(t, r.ItemID.Valid, "West region should have NULL order id")
			}
		}
		assert.True(t, westFound, "West region should appear in RIGHT JOIN")
	})
}

// ---------------------------------------------------------------------------
// INNER JOIN ... USING
// ---------------------------------------------------------------------------

func TestInnerJoinUsing(t *testing.T) {
	setupJoinTables(t)

	t.Run("SameAsJoinUsing", func(t *testing.T) {
		// Arrange — INNER JOIN USING should behave identically to JOIN USING
		qJoin := sb.Select("sq_orders.id").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			OrderBy("sq_orders.id")
		qInner := sb.Select("sq_orders.id").
			From("sq_orders").
			InnerJoinUsing("sq_regions", "region_id").
			OrderBy("sq_orders.id")

		// Act
		idsJoin := queryInts(t, qJoin)
		idsInner := queryInts(t, qInner)

		// Assert
		assert.Equal(t, idsJoin, idsInner)
	})
}

// ---------------------------------------------------------------------------
// CROSS JOIN ... USING (unusual but syntactically valid)
// ---------------------------------------------------------------------------

func TestCrossJoinUsing(t *testing.T) {
	setupJoinTables(t)

	t.Run("CartesianOnMatchingColumn", func(t *testing.T) {
		// Note: CROSS JOIN ... USING is unusual and may not be supported by
		// all databases. Some databases treat it like an inner join on the
		// USING column. We test SQL generation and skip execution if the DB
		// rejects it.

		q := sqrl.Select("sq_orders.id").
			From("sq_orders").
			CrossJoinUsing("sq_regions", "region_id")

		// Verify SQL generation
		sqlStr, _, err := q.PlaceholderFormat(phf()).ToSQL()
		require.NoError(t, err)
		assert.Contains(t, sqlStr, "CROSS JOIN sq_regions USING (region_id)")
	})
}

// ---------------------------------------------------------------------------
// FULL OUTER JOIN (via FullJoin)
// ---------------------------------------------------------------------------

func TestFullJoin(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support FULL OUTER JOIN")
	}

	setupJoinTables(t)

	t.Run("PreservesBothSides", func(t *testing.T) {
		// Arrange — FULL OUTER JOIN keeps all orders AND all regions
		q := sb.Select("sq_orders.id", "sq_regions.region_name").
			From("sq_orders").
			FullJoin("sq_regions ON sq_orders.region_id = sq_regions.region_id").
			OrderBy("sq_orders.id")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			OrderID    sql.NullInt64
			RegionName sql.NullString
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.OrderID, &r.RegionName))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — should have:
		// - 4 matched rows (orders 1-4 match regions)
		// - 1 unmatched order (order 5, region_id=99)
		// - 1 unmatched region (West, region_id=40)
		// Total: 6 rows
		assert.Len(t, results, 6)

		// Check that we have a NULL region (from unmatched order 5)
		var hasNullRegion bool
		for _, r := range results {
			if r.OrderID.Valid && r.OrderID.Int64 == 5 {
				assert.False(t, r.RegionName.Valid, "order 5 should have NULL region_name")
				hasNullRegion = true
			}
		}
		assert.True(t, hasNullRegion, "should have order with no matching region")
	})

	t.Run("FullJoinWithWhere", func(t *testing.T) {
		// Arrange — FULL OUTER JOIN with a WHERE filter
		q := sb.Select("sq_orders.id", "sq_regions.region_name").
			From("sq_orders").
			FullJoin("sq_regions ON sq_orders.region_id = sq_regions.region_id").
			Where(sqrl.Eq{"sq_regions.region_name": "North"}).
			OrderBy("sq_orders.id")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		// Assert — only orders 1, 2 are in region North
		assert.Equal(t, []int{1, 2}, ids)
	})

	t.Run("FullJoinWithArgs", func(t *testing.T) {
		// Arrange — FULL OUTER JOIN with placeholder in ON clause
		q := sb.Select("sq_orders.id").
			From("sq_orders").
			FullJoin("sq_regions ON sq_orders.region_id = sq_regions.region_id AND sq_regions.region_name = ?", "South").
			Where(sqrl.NotEq{"sq_orders.id": nil}).
			OrderBy("sq_orders.id")

		// Act
		ids := queryInts(t, q)

		// Assert — all 5 orders should appear (FULL JOIN preserves all left rows);
		// only order 3 gets matched to South
		assert.Len(t, ids, 5)
	})
}

// ---------------------------------------------------------------------------
// FULL OUTER JOIN ... USING
// ---------------------------------------------------------------------------

func TestFullJoinUsing(t *testing.T) {
	if isMySQL() {
		t.Skip("MySQL does not support FULL OUTER JOIN")
	}

	setupJoinTables(t)

	t.Run("PreservesBothSides", func(t *testing.T) {
		// Arrange
		q := sb.Select("sq_orders.id", "sq_regions.region_name").
			From("sq_orders").
			FullJoinUsing("sq_regions", "region_id").
			OrderBy("sq_orders.id")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			OrderID    sql.NullInt64
			RegionName sql.NullString
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.OrderID, &r.RegionName))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — 4 matched + 1 unmatched order + 1 unmatched region = 6
		assert.Len(t, results, 6)
	})

	t.Run("UnmatchedRegionHasNullOrder", func(t *testing.T) {
		// Arrange — check that West (region_id=40, no orders) appears
		q := sb.Select("sq_regions.region_name", "sq_orders.id").
			From("sq_orders").
			FullJoinUsing("sq_regions", "region_id").
			Where(sqrl.Eq{"sq_regions.region_name": "West"})

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name string
		var orderID sql.NullInt64
		require.NoError(t, rows.Scan(&name, &orderID))

		// Assert
		assert.Equal(t, "West", name)
		assert.False(t, orderID.Valid, "West region should have no matching orders")
	})
}

// ---------------------------------------------------------------------------
// Multiple USING columns
// ---------------------------------------------------------------------------

func TestJoinUsingMultipleColumns(t *testing.T) {
	setupJoinTables(t)

	// Create tables with two shared columns
	createTable(t, "sq_shipments", `(
		region_id  INTEGER,
		id         INTEGER,
		shipped_at TEXT
	)`)
	seedTable(t, `INSERT INTO sq_shipments (region_id, id, shipped_at) VALUES
		(10, 1, '2025-01-01'),
		(20, 3, '2025-02-01')`)

	t.Run("TwoColumnUsing", func(t *testing.T) {
		// Arrange — join orders to shipments on (region_id, id)
		q := sb.Select("sq_orders.id", "sq_shipments.shipped_at").
			From("sq_orders").
			JoinUsing("sq_shipments", "region_id", "id").
			OrderBy("sq_orders.id")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			ID        int
			ShippedAt string
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.ID, &r.ShippedAt))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — only orders 1 (region_id=10, id=1) and 3 (region_id=20, id=3) match
		assert.Equal(t, []result{
			{1, "2025-01-01"},
			{3, "2025-02-01"},
		}, results)
	})
}

// ---------------------------------------------------------------------------
// Mixing ON and USING joins in the same query
// ---------------------------------------------------------------------------

func TestMixedOnAndUsingJoins(t *testing.T) {
	setupJoinTables(t)

	t.Run("JoinUsingAndLeftJoinOn", func(t *testing.T) {
		// Arrange — join orders to regions via USING, then left join warehouses via ON
		q := sb.Select("sq_orders.id", "sq_regions.region_name", "sq_warehouses.capacity").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			LeftJoin("sq_warehouses ON sq_orders.region_id = sq_warehouses.region_id").
			Where(sqrl.Eq{"sq_orders.id": 3}).
			OrderBy("sq_warehouses.capacity")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			ID       int
			Region   string
			Capacity int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.ID, &r.Region, &r.Capacity))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — order 3 is in region 20 (South); warehouse 2 has region_id=20
		assert.Len(t, results, 1)
		assert.Equal(t, "South", results[0].Region)
		assert.Equal(t, 2000, results[0].Capacity)
	})

	t.Run("MultipleUsingJoins", func(t *testing.T) {
		// Arrange — join orders to regions, then join orders to warehouses,
		// both via USING on region_id
		q := sb.Select("sq_orders.id", "sq_regions.region_name", "sq_warehouses.capacity").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			JoinUsing("sq_warehouses", "region_id").
			Where(sqrl.Eq{"sq_orders.id": 4}).
			OrderBy("sq_warehouses.capacity")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			ID       int
			Region   string
			Capacity int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.ID, &r.Region, &r.Capacity))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — order 4 is in region 30 (East); warehouse 3 has region_id=30, capacity=500
		assert.Len(t, results, 1)
		assert.Equal(t, "East", results[0].Region)
		assert.Equal(t, 500, results[0].Capacity)
	})
}

// ---------------------------------------------------------------------------
// JOIN USING with placeholder formats (Dollar for PostgreSQL)
// ---------------------------------------------------------------------------

func TestJoinUsingPlaceholderFormats(t *testing.T) {
	setupJoinTables(t)

	t.Run("DollarPlaceholder", func(t *testing.T) {
		// Arrange — build with Dollar placeholders and verify SQL generation
		q := sqrl.Select("sq_orders.id").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			Where(sqrl.Eq{"sq_regions.region_name": "North"}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, "SELECT sq_orders.id FROM sq_orders JOIN sq_regions USING (region_id) WHERE sq_regions.region_name = $1", sqlStr)
		assert.Equal(t, []interface{}{"North"}, args)
	})

	t.Run("DollarPlaceholderExecution", func(t *testing.T) {
		if !isPostgres() {
			t.Skip("Dollar placeholder execution only tested on PostgreSQL")
		}

		// Arrange — execute with Dollar placeholders against real DB
		q := sb.Select("sq_orders.id").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			Where(sqrl.Eq{"sq_regions.region_name": "South"}).
			OrderBy("sq_orders.id")

		// Act
		ids := queryInts(t, q)

		// Assert — only order 3 is in South
		assert.Equal(t, []int{3}, ids)
	})
}

// ---------------------------------------------------------------------------
// FullJoin with Dollar placeholders
// ---------------------------------------------------------------------------

func TestFullJoinDollarPlaceholder(t *testing.T) {
	t.Run("SQLGeneration", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("a.id", "b.name").
			From("a").
			FullJoin("b ON a.bid = b.id AND b.active = ?", true).
			Where(sqrl.Eq{"a.status": "open"}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t,
			"SELECT a.id, b.name FROM a FULL OUTER JOIN b ON a.bid = b.id AND b.active = $1 WHERE a.status = $2",
			sqlStr)
		assert.Equal(t, []interface{}{true, "open"}, args)
	})
}

// ---------------------------------------------------------------------------
// FullJoinUsing with Dollar placeholders
// ---------------------------------------------------------------------------

func TestFullJoinUsingDollarPlaceholder(t *testing.T) {
	t.Run("SQLGeneration", func(t *testing.T) {
		// Arrange
		q := sqrl.Select("a.x", "b.y").
			From("a").
			FullJoinUsing("b", "id", "region").
			Where(sqrl.Gt{"a.x": 10}).
			PlaceholderFormat(sqrl.Dollar)

		// Act
		sqlStr, args, err := q.ToSQL()

		// Assert
		require.NoError(t, err)
		assert.Equal(t,
			"SELECT a.x, b.y FROM a FULL OUTER JOIN b USING (id, region) WHERE a.x > $1",
			sqlStr)
		assert.Equal(t, []interface{}{10}, args)
	})
}

// ---------------------------------------------------------------------------
// Edge case: empty result from JOIN USING
// ---------------------------------------------------------------------------

func TestJoinUsingEmptyResult(t *testing.T) {
	setupJoinTables(t)

	t.Run("NoMatchingRows", func(t *testing.T) {
		// Arrange — WHERE filter that matches nothing
		q := sb.Select("sq_orders.id").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			Where(sqrl.Eq{"sq_regions.region_name": "Antarctica"})

		// Act
		ids := queryInts(t, q)

		// Assert
		assert.Empty(t, ids)
	})
}

// ---------------------------------------------------------------------------
// Edge case: JOIN USING with aggregate functions
// ---------------------------------------------------------------------------

func TestJoinUsingWithAggregate(t *testing.T) {
	setupJoinTables(t)

	t.Run("SumByRegion", func(t *testing.T) {
		// Arrange — total order amounts per region
		q := sb.Select("sq_regions.region_name").
			Column("SUM(sq_orders.amount) AS total").
			From("sq_orders").
			JoinUsing("sq_regions", "region_id").
			GroupBy("sq_regions.region_name").
			OrderBy("sq_regions.region_name")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Region string
			Total  int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Region, &r.Total))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert
		// East: order 4 (100)
		// North: orders 1 (500) + 2 (300) = 800
		// South: order 3 (700)
		assert.Equal(t, []result{
			{"East", 100},
			{"North", 800},
			{"South", 700},
		}, results)
	})
}

// ---------------------------------------------------------------------------
// Edge case: LeftJoinUsing with GROUP BY and HAVING
// ---------------------------------------------------------------------------

func TestLeftJoinUsingWithGroupByHaving(t *testing.T) {
	setupJoinTables(t)

	t.Run("RegionsWithMultipleOrders", func(t *testing.T) {
		// Arrange — regions that have more than 1 order
		q := sb.Select("sq_regions.region_name").
			Column("COUNT(sq_orders.id) AS cnt").
			From("sq_regions").
			LeftJoinUsing("sq_orders", "region_id").
			GroupBy("sq_regions.region_name").
			Having("COUNT(sq_orders.id) > ?", 1).
			OrderBy("sq_regions.region_name")

		// Act
		rows, err := q.Query()
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			Region string
			Count  int
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.Region, &r.Count))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())

		// Assert — only North (2 orders) has count > 1
		assert.Equal(t, []result{{"North", 2}}, results)
	})
}
