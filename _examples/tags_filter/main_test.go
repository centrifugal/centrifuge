package main

import (
	"testing"
)

func TestRoundToTwoDecimals(t *testing.T) {
	// Test roundToTwoDecimals function
	testCases := []struct {
		input    float64
		expected float64
	}{
		{1.234567, 1.23},
		{1.236789, 1.24},
		{100.0, 100.0},
		{99.999, 100.0},
	}

	for _, tc := range testCases {
		result := roundToTwoDecimals(tc.input)
		if result != tc.expected {
			t.Errorf("roundToTwoDecimals(%f) = %f, expected %f", tc.input, result, tc.expected)
		}
	}
}
