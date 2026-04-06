package squirrel

import (
	"fmt"
	"regexp"
	"strings"
)

// ErrInvalidIdentifier is returned when a string cannot be used as a safe SQL identifier.
var ErrInvalidIdentifier = fmt.Errorf("invalid SQL identifier")

// identPattern matches simple SQL identifiers: letters, digits, underscores,
// and optionally a single dot for schema-qualified names (e.g. "public.users").
// It intentionally rejects any characters that could be used in SQL injection
// attacks such as semicolons, quotes, dashes, spaces, and parentheses.
var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)

// Ident represents a validated SQL identifier (table name, column name, etc.)
// that is safe for interpolation into SQL strings.
//
// An Ident is created by QuoteIdent or ValidateIdent, which ensure the value
// is safe to use in SQL. This type is used by the Safe* builder methods to
// prevent SQL injection via dynamic identifiers.
//
// Ident implements the Sqlizer interface, so it can be used anywhere a Sqlizer
// is accepted.
type Ident struct {
	// raw is the original unquoted identifier string (after validation).
	raw string
	// quoted is the ANSI-SQL double-quoted form of the identifier.
	quoted string
}

// String returns the quoted form of the identifier.
func (id Ident) String() string {
	return id.quoted
}

// Raw returns the original unquoted identifier string.
func (id Ident) Raw() string {
	return id.raw
}

// ToSQL implements the Sqlizer interface. It returns the quoted identifier.
func (id Ident) ToSQL() (string, []any, error) {
	return id.quoted, nil, nil
}

// QuoteIdent produces a safely-quoted SQL identifier using ANSI SQL
// double-quoting. Any embedded double-quote characters are escaped by doubling
// them. This is the most permissive way to produce safe identifiers — it
// allows any string, including those with spaces, reserved words, and special
// characters, by wrapping the entire identifier in double quotes.
//
// For schema-qualified identifiers (e.g. "public.users"), each part is quoted
// separately: "public"."users".
//
// WARNING: Empty strings are rejected and will cause QuoteIdent to return an
// error.
//
// Ex:
//
//	id, err := sq.QuoteIdent("users")        // "users"
//	id, err := sq.QuoteIdent("my table")     // "my table"
//	id, err := sq.QuoteIdent("public.users") // "public"."users"
//	id, err := sq.QuoteIdent(`Robert"; DROP TABLE users; --`)
//	  // "Robert""; DROP TABLE users; --"
func QuoteIdent(name string) (Ident, error) {
	if name == "" {
		return Ident{}, fmt.Errorf("%w: identifier must not be empty", ErrInvalidIdentifier)
	}

	parts := strings.Split(name, ".")
	quoted := make([]string, len(parts))
	for i, p := range parts {
		if p == "" {
			return Ident{}, fmt.Errorf("%w: identifier part must not be empty", ErrInvalidIdentifier)
		}
		quoted[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}

	return Ident{raw: name, quoted: strings.Join(quoted, ".")}, nil
}

// MustQuoteIdent is like QuoteIdent but panics on error.
func MustQuoteIdent(name string) Ident {
	id, err := QuoteIdent(name)
	if err != nil {
		panic(err)
	}
	return id
}

// ValidateIdent checks that name matches a strict identifier pattern
// (letters, digits, underscores; optionally dot-separated for schema-qualified
// names) and returns an Ident WITHOUT adding double quotes. This is useful
// when double-quoting is undesirable (e.g. case-sensitive databases where
// quoting changes behaviour) but you still want to reject obviously dangerous
// input.
//
// The validation pattern is: ^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$
//
// Ex:
//
//	id, err := sq.ValidateIdent("users")         // OK → users
//	id, err := sq.ValidateIdent("public.users")  // OK → public.users
//	id, err := sq.ValidateIdent("users; DROP")   // ERROR — invalid
//	id, err := sq.ValidateIdent("")               // ERROR — empty
func ValidateIdent(name string) (Ident, error) {
	if name == "" {
		return Ident{}, fmt.Errorf("%w: identifier must not be empty", ErrInvalidIdentifier)
	}
	if !identPattern.MatchString(name) {
		return Ident{}, fmt.Errorf("%w: %q contains invalid characters", ErrInvalidIdentifier, name)
	}
	return Ident{raw: name, quoted: name}, nil
}

// MustValidateIdent is like ValidateIdent but panics on error.
func MustValidateIdent(name string) Ident {
	id, err := ValidateIdent(name)
	if err != nil {
		panic(err)
	}
	return id
}

// QuoteIdents quotes each name with QuoteIdent and returns all results.
// It returns an error on the first invalid identifier.
func QuoteIdents(names ...string) ([]Ident, error) {
	ids := make([]Ident, len(names))
	for i, n := range names {
		id, err := QuoteIdent(n)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

// ValidateIdents validates each name with ValidateIdent and returns all results.
// It returns an error on the first invalid identifier.
func ValidateIdents(names ...string) ([]Ident, error) {
	ids := make([]Ident, len(names))
	for i, n := range names {
		id, err := ValidateIdent(n)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

// identsToStrings converts a slice of Ident to their string (quoted) representations.
func identsToStrings(ids []Ident) []string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	return strs
}
