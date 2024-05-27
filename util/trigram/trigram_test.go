package trigram_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/trigram"
)

func TestExtractTrigrams(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"cat", []string{"  c", " ca", "cat", "at "}},
		{"a", []string{"  a", " a "}},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := trigram.ComputeTrigrams(c.in)
			require.Equal(t, c.want, got)
		})
	}
}

func TestSignatureComparisons(t *testing.T) {
	s1 := trigram.NewSignature(12)

	s1.AddString("The cat in the hat")

	s2 := trigram.NewSignature(12)
	s2.AddString("the hat")

	require.True(t, s1.Contains(s2))
}
