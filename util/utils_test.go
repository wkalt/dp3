package util_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wkalt/dp3/util"
)

func TestPow(t *testing.T) {
	cases := []struct {
		x        int
		y        int
		expected int
	}{
		{2, 0, 1},
		{2, 1, 2},
		{2, 2, 4},
		{2, 3, 8},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, util.Pow(c.x, c.y))
	}
}

func TestGroupBy(t *testing.T) {
	arr := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	groups := util.GroupBy(arr, func(x int) int { return x % 2 })
	assert.Equal(t, map[int][]int{0: {2, 4, 6, 8}, 1: {1, 3, 5, 7, 9}}, groups)
}

func TestParseNanos(t *testing.T) {
	tm := util.ParseNanos(1e9)
	assert.Equal(t, "1970-01-01 00:00:01 +0000 UTC", tm.UTC().String())
}

func TestDateSeconds(t *testing.T) {
	assert.Equal(t, uint64(0), util.DateSeconds("1970-01-01"))
}

func TestComputeStreamID(t *testing.T) {
	assert.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", util.ComputeStreamID("", ""))
}
