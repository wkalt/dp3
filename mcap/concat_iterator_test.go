package mcap_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/mcap"
)

func TestConcatIterator(t *testing.T) {
	input := &bytes.Buffer{}
	stamps := make([]int64, 0, 100)
	for i := 0; i < 100; i++ {
		stamps = append(stamps, int64(i))
	}
	mcap.WriteFile(t, input, stamps)

	cases := []struct {
		assertion string
		ranges    [][]uint64
		outputs   []uint64
	}{
		{
			"empty",
			[][]uint64{},
			[]uint64{},
		},
		{
			"one",
			[][]uint64{{1, 4}},
			[]uint64{1, 2, 3},
		},
		{
			"two",
			[][]uint64{{1, 4}, {4, 7}},
			[]uint64{1, 2, 3, 4, 5, 6},
		},
		{
			"three",
			[][]uint64{{1, 4}, {4, 6}, {6, 10}},
			[]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			iter := mcap.NewConcatIterator(bytes.NewReader(input.Bytes()), c.ranges, false)
			var got []uint64
			for {
				_, _, m, err := iter.Next(nil)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
				got = append(got, m.LogTime)
			}

			if len(got) != len(c.outputs) {
				t.Fatalf("expected %v, got %v", c.outputs, got)
			}

			for i := range got {
				if got[i] != c.outputs[i] {
					t.Fatalf("expected %v, got %v", c.outputs, got)
				}
			}
		})
	}
}
