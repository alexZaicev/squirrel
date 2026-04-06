package integration

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	sqrl "github.com/alexZaicev/squirrel"
)

var (
	db         *sql.DB
	sb         sqrl.StatementBuilderType
	driverName string
)

const (
	schemaItems = `CREATE TABLE sq_items (
		id INTEGER,
		name TEXT,
		category TEXT,
		price INTEGER
	)`
	schemaCategories = `CREATE TABLE sq_categories (
		name TEXT,
		description TEXT
	)`

	seedItems = `INSERT INTO sq_items (id, name, category, price) VALUES
		(1, 'apple', 'fruit', 100),
		(2, 'banana', 'fruit', 50),
		(3, 'carrot', 'vegetable', 75),
		(4, 'donut', 'pastry', 200),
		(5, 'eggplant', 'vegetable', 150),
		(6, 'mystery', NULL, 99)`

	seedCategories = `INSERT INTO sq_categories (name, description) VALUES
		('fruit', 'Fresh fruits'),
		('vegetable', 'Fresh vegetables'),
		('pastry', 'Baked goods'),
		('dairy', 'Dairy products')`
)

func TestMain(m *testing.M) {
	var dataSource string
	flag.StringVar(&driverName, "driver", "", "integration database driver")
	flag.StringVar(&dataSource, "dataSource", "", "integration database data source")
	flag.Parse()

	if driverName == "" {
		driverName = "sqlite3"
	}

	if driverName == "sqlite3" && dataSource == "" {
		dataSource = ":memory:"
	}

	var err error
	db, err = sql.Open(driverName, dataSource)
	if err != nil {
		fmt.Printf("error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Drop tables from any previous run (persistent databases like MySQL/PostgreSQL).
	for _, tbl := range []string{"sq_items", "sq_categories"} {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
	}

	for _, stmt := range []string{schemaItems, schemaCategories} {
		if _, err := db.Exec(stmt); err != nil {
			fmt.Printf("error creating schema: %v\n", err)
			os.Exit(2)
		}
	}

	for _, stmt := range []string{seedItems, seedCategories} {
		if _, err := db.Exec(stmt); err != nil {
			fmt.Printf("error seeding data: %v\n", err)
			os.Exit(3)
		}
	}

	sb = sqrl.StatementBuilder.RunWith(db)
	if driverName == "postgres" {
		sb = sb.PlaceholderFormat(sqrl.Dollar)
	}

	code := m.Run()

	db.Exec("DROP TABLE IF EXISTS sq_items")
	db.Exec("DROP TABLE IF EXISTS sq_categories")

	os.Exit(code)
}

// createTable creates a test table and registers cleanup to drop it.
func createTable(t *testing.T, name, schemaDDL string) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s %s", name, schemaDDL))
	require.NoError(t, err, "failed to create table %s", name)
	t.Cleanup(func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	})
}

// seedTable inserts data into a table.
func seedTable(t *testing.T, stmt string) {
	t.Helper()
	_, err := db.Exec(stmt)
	require.NoError(t, err, "failed to seed table")
}

// queryStrings runs a single-column string query and returns all values.
func queryStrings(t *testing.T, q sqrl.SelectBuilder) []string {
	t.Helper()
	rows, err := q.Query()
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

// queryInts runs a single-column int query and returns all values.
func queryInts(t *testing.T, q sqrl.SelectBuilder) []int {
	t.Helper()
	rows, err := q.Query()
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

// isPostgres returns true when running against PostgreSQL.
func isPostgres() bool { return driverName == "postgres" }

// isMySQL returns true when running against MySQL.
func isMySQL() bool { return driverName == "mysql" }

// phf returns the placeholder format appropriate for the current driver.
func phf() sqrl.PlaceholderFormat {
	if isPostgres() {
		return sqrl.Dollar
	}
	return sqrl.Question
}
