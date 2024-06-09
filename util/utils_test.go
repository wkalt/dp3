package util_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestOkeys(t *testing.T) {
	m := map[int]string{3: "c", 1: "a", 2: "b"}
	for i := 0; i < 1000; i++ {
		assert.Equal(t, []int{1, 2, 3}, util.Okeys(m))
	}
}

func TestMap(t *testing.T) {
	arr := []int{1, 2, 3, 4, 5}
	assert.Equal(t, []int{2, 4, 6, 8, 10}, util.Map(func(x int) int { return x * 2 }, arr))
}

func TestCryptoGraphicHash(t *testing.T) {
	assert.Equal(
		t,
		"6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
		util.CryptographicHash([]byte("1")),
	)
}

func TestPointer(t *testing.T) {
	assert.Equal(t, 1, *util.Pointer(1))
}

func TestAll(t *testing.T) {
	assert.True(t, util.All([]bool{true, true, true}, func(b bool) bool { return b }))
	assert.False(t, util.All([]bool{true, false, true}, func(b bool) bool { return b }))
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
		require.Equal(t, c.expected, util.HumanBytes(c.input), c.assertion)
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
		require.Equal(t, c.expected, util.When(c.cond, c.a, c.b), c.assertion)
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
		require.Equal(t, c.expected, util.Reduce(func(a, b int) int { return a + b }, 0, c.input), c.assertion)
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
		require.Equal(t, c.expected, util.Max(c.a, c.b), c.assertion)
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
		require.Equal(t, c.expected, util.Min(c.a, c.b), c.assertion)
	}
}

func TestEnsureDirectoryExists(t *testing.T) {
	subdir := "/subdir"
	t.Run("directory already exists", func(t *testing.T) {
		tmpdir, err := os.MkdirTemp("", "dp3")
		require.NoError(t, err)
		defer os.RemoveAll(tmpdir)
		require.NoError(t, os.Mkdir(tmpdir+subdir, 0755))
		require.NoError(t, util.EnsureDirectoryExists(tmpdir+subdir))
	})
	t.Run("directory does not exist", func(t *testing.T) {
		tmpdir, err := os.MkdirTemp("", "dp3")
		require.NoError(t, err)
		defer os.RemoveAll(tmpdir)
		require.NoError(t, util.EnsureDirectoryExists(tmpdir+subdir))
		_, err = os.Stat(tmpdir + subdir)
		require.NoError(t, err)
	})
}

func TestFilter(t *testing.T) {
	isEven := func(a int) bool { return a%2 == 0 }
	cases := []struct {
		assertion string
		input     []int
		expected  []int
	}{
		{"empty", []int{}, []int{}},
		{"single", []int{1}, []int{}},
		{"multiple", []int{1, 2, 3, 4, 5}, []int{2, 4}},
	}
	for _, c := range cases {
		require.Equal(t, c.expected, util.Filter(isEven, c.input), c.assertion)
	}
}

func TestRunPipe(t *testing.T) {
	t.Run("write and read", func(t *testing.T) {
		wfunc := func(ctx context.Context, w io.Writer) error {
			if _, err := w.Write([]byte("hello")); err != nil {
				return fmt.Errorf("failed to write: %w", err)
			}
			return nil
		}
		buf := make([]byte, 5)
		rfunc := func(ctx context.Context, r io.Reader) error {
			if _, err := io.ReadFull(r, buf); err != nil {
				return fmt.Errorf("failed to read: %w", err)
			}
			return nil
		}

		ctx := context.Background()
		require.NoError(t, util.RunPipe(ctx, wfunc, rfunc))
		assert.Equal(t, "hello", string(buf))
	})
	t.Run("write error", func(t *testing.T) {
		wfunc := func(ctx context.Context, w io.Writer) error {
			return io.ErrClosedPipe
		}
		rfunc := func(ctx context.Context, r io.Reader) error {
			return nil
		}
		ctx := context.Background()
		require.ErrorIs(t, util.RunPipe(ctx, wfunc, rfunc), io.ErrClosedPipe)
	})
	t.Run("read error", func(t *testing.T) {
		wfunc := func(ctx context.Context, w io.Writer) error {
			if _, err := w.Write([]byte("hello")); err != nil {
				return fmt.Errorf("failed to write: %w", err)
			}
			return nil
		}
		rfunc := func(ctx context.Context, r io.Reader) error {
			return io.ErrClosedPipe
		}
		ctx := context.Background()
		require.ErrorIs(t, util.RunPipe(ctx, wfunc, rfunc), io.ErrClosedPipe)
	})
}

type closer struct {
	closeFunc func() error
}

func (c *closer) Close() error {
	return c.closeFunc()
}

func TestCloseAll(t *testing.T) {
	t.Run("no errors", func(t *testing.T) {
		c1 := closer{func() error { return nil }}
		c2 := closer{func() error { return nil }}
		require.NoError(t, util.CloseAll(&c1, &c2))
	})
	t.Run("error", func(t *testing.T) {
		c1 := closer{func() error { return nil }}
		c2 := closer{func() error { return errors.New("failed to close") }}
		require.Error(t, util.CloseAll(&c1, &c2))
	})
}

type contextCloser struct {
	closeFunc func(ctx context.Context) error
}

func (c *contextCloser) Close(ctx context.Context) error {
	return c.closeFunc(ctx)
}

func TestCloseAllContext(t *testing.T) {
	t.Run("no errors", func(t *testing.T) {
		c1 := contextCloser{func(context.Context) error { return nil }}
		c2 := contextCloser{func(context.Context) error { return nil }}
		ctx := context.Background()
		require.NoError(t, util.CloseAllContext(ctx, &c1, &c2))
	})
	t.Run("error", func(t *testing.T) {
		c1 := contextCloser{func(context.Context) error { return nil }}
		c2 := contextCloser{func(context.Context) error { return errors.New("failed to close") }}
		ctx := context.Background()
		require.Error(t, util.CloseAllContext(ctx, &c1, &c2))
	})
}
