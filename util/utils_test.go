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

func TestOkeys(t *testing.T) {
	m := map[int]string{3: "c", 1: "a", 2: "b"}
	for i := 0; i < 1000; i++ {
		assert.Equal(t, []int{1, 2, 3}, util.Okeys(m))
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		assertion string
		input     uint64
		expected  string
	}{
		{"0 bytes", 0, "0 B"},
		{"1 byte", 1, "1 B"},
		{"50 bytes", 50, "50 B"},
		{"1 kilobyte", 1024, "1 KB"},
		{"1 megabyte", 1024 * 1024, "1 MB"},
		{"1 gigabyte", 1024 * 1024 * 1024, "1 GB"},
		{"50 gigabytes", 50 * 1024 * 1024 * 1024, "50 GB"},
		{"1 terabyte", 1024 * 1024 * 1024 * 1024, "1 TB"},
		{"1 petabyte", 1024 * 1024 * 1024 * 1024 * 1024, "1 PB"},
		{"1 exabyte", 1024 * 1024 * 1024 * 1024 * 1024 * 1024, "1 EB"},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, util.HumanBytes(c.input), c.assertion)
	}
}

func TestWhen(t *testing.T) {
	cases := []struct {
		assertion string
		cond      bool
		a         int
		b         int
		expected  int
	}{
		{"true", true, 1, 2, 1},
		{"false", false, 1, 2, 2},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, util.When(c.cond, c.a, c.b), c.assertion)
	}
}

func TestReduce(t *testing.T) {
	cases := []struct {
		assertion string
		input     []int
		expected  int
	}{
		{"empty", []int{}, 0},
		{"single", []int{1}, 1},
		{"multiple", []int{1, 2, 3, 4, 5}, 15},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, util.Reduce(func(a, b int) int { return a + b }, 0, c.input), c.assertion)
	}
}

func TestMax(t *testing.T) {
	cases := []struct {
		assertion string
		a         int
		b         int
		expected  int
	}{
		{"a > b", 2, 1, 2},
		{"a < b", 1, 2, 2},
		{"a = b", 1, 1, 1},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, util.Max(c.a, c.b), c.assertion)
	}
}

func TestMin(t *testing.T) {
	cases := []struct {
		assertion string
		a         int
		b         int
		expected  int
	}{
		{"a > b", 2, 1, 1},
		{"a < b", 1, 2, 1},
		{"a = b", 1, 1, 1},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, util.Min(c.a, c.b), c.assertion)
	}
}
