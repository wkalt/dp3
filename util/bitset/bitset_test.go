package bitset_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/util/bitset"
)

func TestBitset(t *testing.T) {
	t.Run("set bit", func(t *testing.T) {
		set := bitset.New(12)
		set.SetBit(24)
		require.True(t, set.HasBit(24))
	})

	t.Run("contains", func(t *testing.T) {
		set1 := bitset.New(12)
		set1.SetBit(24)
		set1.SetBit(25)
		set1.SetBit(26)
		set1.SetBit(27)

		set2 := bitset.New(12)
		set2.SetBit(24)
		set2.SetBit(25)

		require.True(t, set1.Contains(set2))
	})
}
