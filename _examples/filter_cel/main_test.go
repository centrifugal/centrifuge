package main

import (
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/maypok86/otter"
)

func BenchmarkCELPublicationFiltererMatch(b *testing.B) {
	// Create CEL environment
	env, err := cel.NewEnv(
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
	)
	if err != nil {
		b.Fatal(err)
	}

	// Test different filter expressions.
	testCases := []struct {
		name       string
		expression string
		tags       map[string]string
		expected   bool
	}{
		{
			name:       "Simple equality match",
			expression: `tags["ticker"] == "GOOG"`,
			tags:       map[string]string{"ticker": "GOOG"},
			expected:   true,
		},
		{
			name:       "Simple equality no match",
			expression: `tags["ticker"] == "GOOG"`,
			tags:       map[string]string{"ticker": "AAPL"},
			expected:   false,
		},
		{
			name:       "Complex expression match",
			expression: `tags["ticker"] in ["GOOG", "AAPL", "MSFT"]`,
			tags:       map[string]string{"ticker": "AAPL"},
			expected:   true,
		},
		{
			name:       "Complex expression no match",
			expression: `tags["ticker"] in ["GOOG", "AAPL", "MSFT"]`,
			tags:       map[string]string{"ticker": "TSLA"},
			expected:   false,
		},
		{
			name:       "Multiple tags match",
			expression: `tags["ticker"] == "GOOG" && tags["market"] == "NASDAQ"`,
			tags:       map[string]string{"ticker": "GOOG", "market": "NASDAQ"},
			expected:   true,
		},
		{
			name:       "Multiple tags no match",
			expression: `tags["ticker"] == "GOOG" && tags["market"] == "NASDAQ"`,
			tags:       map[string]string{"ticker": "GOOG", "market": "NYSE"},
			expected:   false,
		},
		{
			name:       "String contains",
			expression: `tags["ticker"].contains("GOO")`,
			tags:       map[string]string{"ticker": "GOOG"},
			expected:   true,
		},
		{
			name:       "String starts with",
			expression: `tags["ticker"].startsWith("AA")`,
			tags:       map[string]string{"ticker": "AAPL"},
			expected:   true,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create filter once
			filter, err := NewCELPublicationFilterer(env, tc.expression)
			if err != nil {
				b.Fatal(err)
			}

			variables := map[string]any{
				"tags": tc.tags,
			}

			// Reset timer and run benchmark
			b.ResetTimer()
			for b.Loop() {
				for range 10000 {
					result := filter.FilterPublication(variables)
					if result != tc.expected {
						b.Fatalf("Expected %v, got %v", tc.expected, result)
					}
				}
			}
		})
	}
}

func BenchmarkCELPublicationFilterer_Creation(b *testing.B) {
	var opts []cel.EnvOption
	opts = append(opts, cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)))
	// Validate function declarations once during base env initialization,
	// so they don't need to be evaluated each time a CEL rule is compiled.
	// This is a relatively expensive operation.
	opts = append(opts, cel.EagerlyValidateDeclarations(true))

	env, err := cel.NewEnv(opts...)
	if err != nil {
		b.Fatal(err)
	}

	testExpressions := []string{
		`tags["ticker"] == "GOOG"`,
		`tags["ticker"] in ["GOOG", "AAPL", "MSFT"]`,
		`tags["ticker"] == "GOOG" && tags["market"] == "NASDAQ"`,
		`tags["ticker"].contains("GOO")`,
		`tags["ticker"].startsWith("AA")`,
	}

	for _, expr := range testExpressions {
		b.Run(expr, func(b *testing.B) {
			for b.Loop() {
				_, err = NewCELPublicationFilterer(env, expr)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCELPublicationFilterer_CreationWithCache(b *testing.B) {
	var opts []cel.EnvOption
	opts = append(opts, cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)))
	opts = append(opts, cel.EagerlyValidateDeclarations(true))

	env, err := cel.NewEnv(opts...)
	if err != nil {
		b.Fatal(err)
	}

	// Create cache similar to websocket handler
	cache, err := otter.MustBuilder[string, *CELPublicationFilterer](1000).
		Cost(func(key string, value *CELPublicationFilterer) uint32 {
			return uint32(len(key))
		}).
		WithTTL(time.Minute).
		Build()
	if err != nil {
		b.Fatal(err)
	}

	testExpressions := []string{
		`tags["ticker"] == "GOOG"`,
		`tags["ticker"] in ["GOOG", "AAPL", "MSFT"]`,
		`tags["ticker"] == "GOOG" && tags["market"] == "NASDAQ"`,
		`tags["ticker"].contains("GOO")`,
		`tags["ticker"].startsWith("AA")`,
	}

	// Helper function to get or create cached filter
	getOrCreateFilter := func(expression string) (*CELPublicationFilterer, error) {
		if filter, ok := cache.Get(expression); ok {
			return filter, nil
		}

		filter, err := NewCELPublicationFilterer(env, expression)
		if err != nil {
			return nil, err
		}

		cache.Set(expression, filter)
		return filter, nil
	}

	for _, expr := range testExpressions {
		b.Run(expr, func(b *testing.B) {
			for b.Loop() {
				_, err := getOrCreateFilter(expr)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCELPublicationFilterer_RealisticCacheUsage(b *testing.B) {
	var opts []cel.EnvOption
	opts = append(opts, cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)))
	opts = append(opts, cel.EagerlyValidateDeclarations(true))

	env, err := cel.NewEnv(opts...)
	if err != nil {
		b.Fatal(err)
	}

	// Create cache
	cache, err := otter.MustBuilder[string, *CELPublicationFilterer](10000).
		Cost(func(key string, value *CELPublicationFilterer) uint32 {
			return uint32(len(key))
		}).
		WithTTL(10 * time.Minute).
		Build()
	if err != nil {
		b.Fatal(err)
	}

	// Helper function to get or create cached filter
	getOrCreateFilter := func(expression string) (*CELPublicationFilterer, error) {
		if filter, ok := cache.Get(expression); ok {
			return filter, nil
		}

		filter, err := NewCELPublicationFilterer(env, expression)
		if err != nil {
			return nil, err
		}

		cache.Set(expression, filter)
		return filter, nil
	}

	// Simulate realistic scenario where many subscribers use similar filter expressions
	// In a real scenario, many users would subscribe to the same ticker
	popularExpressions := []string{
		`tags["ticker"] == "GOOG"`,
		`tags["ticker"] == "AAPL"`,
		`tags["ticker"] == "MSFT"`,
		`tags["ticker"] in ["GOOG", "AAPL"]`,         // Portfolio tracking
		`tags["ticker"] in ["TSLA", "META", "NVDA"]`, // Tech stocks
	}

	b.Run("WithCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Simulate many subscription requests using popular expressions
			for j := 0; j < 100; j++ {
				expr := popularExpressions[j%len(popularExpressions)]
				_, err := getOrCreateFilter(expr)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("WithoutCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Simulate many subscription requests WITHOUT caching
			for j := 0; j < 100; j++ {
				expr := popularExpressions[j%len(popularExpressions)]
				_, err := NewCELPublicationFilterer(env, expr)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
