package util_test

import (
	"bytes"
	"testing"

	"github.com/wkalt/dp3/util"
)

func TestCountingWriter(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int
	}{
		{
			name:     "Empty input",
			input:    []byte{},
			expected: 0,
		},
		{
			name:     "Single character",
			input:    []byte{'a'},
			expected: 1,
		},
		{
			name:     "Multiple characters",
			input:    []byte("Hello, World!"),
			expected: 13,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			writer := util.NewCountingWriter(&buf)

			_, err := writer.Write(tt.input)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			count := writer.Count()
			if count != tt.expected {
				t.Errorf("CountingWriter.Count() = %d, want %d", count, tt.expected)
			}
		})
	}
}
