package squirrel

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuestion(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, err := Question.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, sql, s)
}

func TestDollar(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = $1 AND y = $2", s)
}

func TestColon(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = :1 AND y = :2", s)
}

func TestAtp(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = @p1 AND y = @p2", s)
}

func TestQuestionSinglePlaceholder(t *testing.T) {
	sql := "x = ?"
	s, err := Question.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = ?", s)
}

func TestDollarSinglePlaceholder(t *testing.T) {
	sql := "x = ?"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = $1", s)
}

func TestColonSinglePlaceholder(t *testing.T) {
	sql := "x = ?"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = :1", s)
}

func TestAtpSinglePlaceholder(t *testing.T) {
	sql := "x = ?"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = @p1", s)
}

func TestQuestionManyPlaceholders(t *testing.T) {
	sql := "x = ? AND y = ? AND z = ? AND w = ? AND v = ?"
	s, err := Question.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, sql, s)
}

func TestDollarManyPlaceholders(t *testing.T) {
	sql := "x = ? AND y = ? AND z = ? AND w = ? AND v = ?"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = $1 AND y = $2 AND z = $3 AND w = $4 AND v = $5", s)
}

func TestColonManyPlaceholders(t *testing.T) {
	sql := "x = ? AND y = ? AND z = ? AND w = ? AND v = ?"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = :1 AND y = :2 AND z = :3 AND w = :4 AND v = :5", s)
}

func TestAtpManyPlaceholders(t *testing.T) {
	sql := "x = ? AND y = ? AND z = ? AND w = ? AND v = ?"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = @p1 AND y = @p2 AND z = @p3 AND w = @p4 AND v = @p5", s)
}

func TestQuestionNoPlaceholders(t *testing.T) {
	sql := "SELECT 1"
	s, err := Question.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, sql, s)
}

func TestDollarNoPlaceholders(t *testing.T) {
	sql := "SELECT 1"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, sql, s)
}

func TestColonNoPlaceholders(t *testing.T) {
	sql := "SELECT 1"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, sql, s)
}

func TestAtpNoPlaceholders(t *testing.T) {
	sql := "SELECT 1"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, sql, s)
}

func TestQuestionEmptySQL(t *testing.T) {
	s, err := Question.ReplacePlaceholders("")
	assert.NoError(t, err)
	assert.Equal(t, "", s)
}

func TestDollarEmptySQL(t *testing.T) {
	s, err := Dollar.ReplacePlaceholders("")
	assert.NoError(t, err)
	assert.Equal(t, "", s)
}

func TestColonEmptySQL(t *testing.T) {
	s, err := Colon.ReplacePlaceholders("")
	assert.NoError(t, err)
	assert.Equal(t, "", s)
}

func TestAtpEmptySQL(t *testing.T) {
	s, err := AtP.ReplacePlaceholders("")
	assert.NoError(t, err)
	assert.Equal(t, "", s)
}

func TestPlaceholders(t *testing.T) {
	assert.Equal(t, "?,?", Placeholders(2))
}

func TestPlaceholdersZero(t *testing.T) {
	assert.Equal(t, "", Placeholders(0))
}

func TestPlaceholdersNegative(t *testing.T) {
	assert.Equal(t, "", Placeholders(-1))
}

func TestPlaceholdersOne(t *testing.T) {
	assert.Equal(t, "?", Placeholders(1))
}

func TestPlaceholdersMany(t *testing.T) {
	assert.Equal(t, "?,?,?,?,?", Placeholders(5))
}

func TestEscapeQuestion(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, err := Question.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	// Question format returns input unchanged, so ?? stays as ??
	assert.Equal(t, sql, s)
}

func TestEscapeDollar(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array['$1'] AND enabled = $2", s)
}

func TestEscapeColon(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array[':1'] AND enabled = :2", s)
}

func TestEscapeAtp(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array['@p1'] AND enabled = @p2", s)
}

func TestEscapeDollarOnly(t *testing.T) {
	sql := "??"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "?", s)
}

func TestEscapeColonOnly(t *testing.T) {
	sql := "??"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "?", s)
}

func TestEscapeAtpOnly(t *testing.T) {
	sql := "??"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "?", s)
}

func TestEscapeDollarTrailing(t *testing.T) {
	sql := "x = ? AND y ??"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = $1 AND y ?", s)
}

func TestEscapeColonTrailing(t *testing.T) {
	sql := "x = ? AND y ??"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = :1 AND y ?", s)
}

func TestEscapeAtpTrailing(t *testing.T) {
	sql := "x = ? AND y ??"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x = @p1 AND y ?", s)
}

func TestEscapeDollarConsecutive(t *testing.T) {
	sql := "x ?? y ?? z = ?"
	s, err := Dollar.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x ? y ? z = $1", s)
}

func TestEscapeColonConsecutive(t *testing.T) {
	sql := "x ?? y ?? z = ?"
	s, err := Colon.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x ? y ? z = :1", s)
}

func TestEscapeAtpConsecutive(t *testing.T) {
	sql := "x ?? y ?? z = ?"
	s, err := AtP.ReplacePlaceholders(sql)
	assert.NoError(t, err)
	assert.Equal(t, "x ? y ? z = @p1", s)
}

func TestDebugPlaceholderQuestion(t *testing.T) {
	assert.Equal(t, "?", Question.debugPlaceholder())
}

func TestDebugPlaceholderDollar(t *testing.T) {
	assert.Equal(t, "$", Dollar.debugPlaceholder())
}

func TestDebugPlaceholderColon(t *testing.T) {
	assert.Equal(t, ":", Colon.debugPlaceholder())
}

func TestDebugPlaceholderAtp(t *testing.T) {
	assert.Equal(t, "@p", AtP.debugPlaceholder())
}

func BenchmarkPlaceholdersArray(b *testing.B) {
	count := b.N
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	_ = strings.Join(placeholders, ",")
}

func BenchmarkPlaceholdersStrings(b *testing.B) {
	Placeholders(b.N)
}
