package util_test

import (
	"testing"

	"github.com/wkalt/dp3/server/util"
)

func TestPair(t *testing.T) {
	t.Run("pair", func(t *testing.T) {
		pair := util.NewPair(1, "hello")
		if pair.First != 1 {
			t.Errorf("pair.First = %v, want %v", pair.First, 1)
		}
		if pair.Second != "hello" {
			t.Errorf("pair.Second = %v, want %v", pair.Second, "hello")
		}
	})
}
