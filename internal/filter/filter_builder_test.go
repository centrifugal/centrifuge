package filter

import (
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestFilterBuilder_AllOperators(t *testing.T) {
	tags := map[string]string{
		"env":     "prod",
		"region":  "us",
		"tier":    "gold",
		"symbol":  "AAPL",
		"price":   "120.5",
		"count":   "42",
		"debug":   "true",
		"version": "v2",
	}

	cases := []struct {
		name     string
		filter   *protocol.FilterNode
		expected bool
	}{
		{"EQ true", Eq("env", "prod"), true},
		{"EQ false", Eq("env", "staging"), false},
		{"NEQ true", Neq("env", "staging"), true},
		{"NEQ false", Neq("env", "prod"), false},
		{"IN true", In("tier", "gold", "platinum"), true},
		{"IN false", In("tier", "bronze"), false},
		{"NIN true", Nin("tier", "bronze"), true},
		{"NIN false", Nin("tier", "gold"), false},
		{"GT true", Gt("count", "40"), true},
		{"GT false", Gt("count", "100"), false},
		{"GTE true", Gte("count", "42"), true},
		{"LT true", Lt("count", "50"), true},
		{"LTE true", Lte("count", "42"), true},
		{"Contains true", Contains("symbol", "AAP"), true},
		{"Contains false", Contains("symbol", "GOO"), false},
		{"Starts true", Starts("version", "v"), true},
		{"Ends true", Ends("version", "2"), true},
		{"Exists true", Exists("debug"), true},
		{"NotExists true", NotExists("missing"), true},

		// Logical combinations.
		{"AND true", And(Eq("env", "prod"), In("tier", "gold")), true},
		{"AND false", And(Eq("env", "prod"), In("tier", "silver")), false},
		{"OR true", Or(Eq("env", "staging"), In("tier", "gold")), true},
		{"OR false", Or(Eq("env", "staging"), In("tier", "silver")), false},
		{"NOT true", Not(Eq("env", "staging")), true},
		{"NOT false", Not(Eq("env", "prod")), false},

		// Nested logical combinations.
		{"Nested AND/OR true", And(Eq("env", "prod"), Or(In("tier", "silver", "gold"))), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(tc.filter, tags)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}
