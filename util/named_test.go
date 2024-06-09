package util_test

import (
	"testing"

	"github.com/wkalt/dp3/util"
)

func TestNamed(t *testing.T) {
	tests := []struct {
		name     string
		input    util.Named[string]
		expected string
	}{
		{
			name:     "Empty input",
			input:    util.NewNamed("", ""),
			expected: "(: )",
		},
		{
			name:     "Single character",
			input:    util.NewNamed("a", "b"),
			expected: "(a: b)",
		},
		{
			name:     "Multiple characters",
			input:    util.NewNamed("Hello", "World"),
			expected: "(Hello: World)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.String()
			if got != tt.expected {
				t.Errorf("Named.String() = %s, want %s", got, tt.expected)
			}
		})
	}
}
