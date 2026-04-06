package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "WITH prefix AS ? " +
		"INSERT DELAYED IGNORE INTO a (b,c) VALUES (?,?),(?,? + 1) " +
		"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSQL()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSQL()
	assert.Error(t, err)
}

func TestInsertBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestInsertBuilderMustSql should have panicked!")
		}
	}()
	Insert("").MustSQL()
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Insert("test").Values(1).RunWith(db)

	expectedSQL := "INSERT INTO test VALUES (?)"

	_, err := b.Exec()
	assert.NoError(t, err)
	assert.Equal(t, expectedSQL, db.LastExecSQL)
}

func TestInsertBuilderNoRunner(t *testing.T) {
	b := Insert("test").Values(1)

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"field1": 1, "field2": 2, "field3": 3})

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table (field1,field2,field3) VALUES (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSelect(t *testing.T) {
	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReplace(t *testing.T) {
	b := Replace("table").Values(1)

	expectedSQL := "REPLACE INTO table VALUES (?)"

	sql, _, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
}

func TestInsertBuilderOnConflictDoNothing(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id").
		OnConflictDoNothing()

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO NOTHING"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictDoNothingNoTarget(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictDoNothing()

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT DO NOTHING"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictOnConstraintDoNothing(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictOnConstraint("users_pkey").
		OnConflictDoNothing()

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictDoUpdate(t *testing.T) {
	b := Insert("users").
		Columns("id", "name", "email").
		Values(1, "John", "john@example.com").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", "John").
		OnConflictDoUpdate("email", "john@example.com")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name,email) VALUES (?,?,?) ON CONFLICT (id) DO UPDATE SET name = ?, email = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", "john@example.com", "John", "john@example.com"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictDoUpdateExcluded(t *testing.T) {
	b := Insert("users").
		Columns("id", "name", "email").
		Values(1, "John", "john@example.com").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", Expr("EXCLUDED.name")).
		OnConflictDoUpdate("email", Expr("EXCLUDED.email"))

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name,email) VALUES (?,?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", "john@example.com"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictDoUpdateMap(t *testing.T) {
	b := Insert("users").
		Columns("id", "name", "email").
		Values(1, "John", "john@example.com").
		OnConflictColumns("id").
		OnConflictDoUpdateMap(map[string]any{
			"name":  "John",
			"email": "john@example.com",
		})

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name,email) VALUES (?,?,?) ON CONFLICT (id) DO UPDATE SET email = ?, name = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", "john@example.com", "john@example.com", "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictDoUpdateWithWhere(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", Expr("EXCLUDED.name")).
		OnConflictWhere(Eq{"users.active": true})

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.active = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", true}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictMultipleColumns(t *testing.T) {
	b := Insert("users").
		Columns("id", "org_id", "name").
		Values(1, 10, "John").
		OnConflictColumns("id", "org_id").
		OnConflictDoNothing()

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,org_id,name) VALUES (?,?,?) ON CONFLICT (id,org_id) DO NOTHING"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 10, "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictMultiRowValues(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		Values(2, "Jane").
		Values(3, "Bob").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", Expr("EXCLUDED.name"))

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?),(?,?),(?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", 2, "Jane", 3, "Bob"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictWithDollarPlaceholder(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", "John").
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name = $3"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictWithSuffix(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", Expr("EXCLUDED.name")).
		Suffix("RETURNING ?", "id")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", "id"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictDoNothingAndDoUpdateError(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id").
		OnConflictDoNothing().
		OnConflictDoUpdate("name", "John")

	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestInsertBuilderOnConflictColumnsWithoutAction(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id")

	_, _, err := b.ToSQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must use DO NOTHING or DO UPDATE")
}

func TestInsertBuilderOnDuplicateKeyUpdate(t *testing.T) {
	b := Insert("users").
		Columns("id", "name", "email").
		Values(1, "John", "john@example.com").
		OnDuplicateKeyUpdate("name", "John").
		OnDuplicateKeyUpdate("email", "john@example.com")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name,email) VALUES (?,?,?) ON DUPLICATE KEY UPDATE name = ?, email = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", "john@example.com", "John", "john@example.com"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnDuplicateKeyUpdateExpr(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnDuplicateKeyUpdate("name", Expr("VALUES(name)"))

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON DUPLICATE KEY UPDATE name = VALUES(name)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnDuplicateKeyUpdateMap(t *testing.T) {
	b := Insert("users").
		Columns("id", "name", "email").
		Values(1, "John", "john@example.com").
		OnDuplicateKeyUpdateMap(map[string]any{
			"name":  "John",
			"email": "john@example.com",
		})

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name,email) VALUES (?,?,?) ON DUPLICATE KEY UPDATE email = ?, name = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", "john@example.com", "john@example.com", "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnDuplicateKeyUpdateMultiRow(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		Values(2, "Jane").
		OnDuplicateKeyUpdate("name", Expr("VALUES(name)"))

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE name = VALUES(name)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", 2, "Jane"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderOnConflictDoUpdateSubquery(t *testing.T) {
	sub := Select("name").From("defaults").Where(Eq{"id": 1})
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id").
		OnConflictDoUpdate("name", sub)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO UPDATE SET name = (SELECT name FROM defaults WHERE id = ?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John", 1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReturning(t *testing.T) {
	b := Insert("users").
		Columns("name", "email").
		Values("John", "john@example.com").
		Returning("id", "created_at")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (name,email) VALUES (?,?) RETURNING id, created_at"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John", "john@example.com"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReturningWithPlaceholders(t *testing.T) {
	b := Insert("users").
		Columns("name").
		Values("John").
		Returning("id").
		PlaceholderFormat(Dollar)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (name) VALUES ($1) RETURNING id"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReturningStar(t *testing.T) {
	b := Insert("users").
		Columns("name").
		Values("John").
		Returning("*")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (name) VALUES (?) RETURNING *"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReturningWithSuffix(t *testing.T) {
	b := Insert("users").
		Columns("name").
		Values("John").
		Returning("id").
		Suffix("-- comment")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (name) VALUES (?) RETURNING id -- comment"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{"John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReturningWithOnConflict(t *testing.T) {
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		OnConflictColumns("id").
		OnConflictDoNothing().
		Returning("id")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?) ON CONFLICT (id) DO NOTHING RETURNING id"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, "John"}
	assert.Equal(t, expectedArgs, args)
}
