package log_test

import (
	"context"
	"io"
	glog "log"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wkalt/dp3/server/util/log"
)

func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	stdout := os.Stdout
	stderr := os.Stderr
	os.Stdout = w
	os.Stderr = w
	glog.SetOutput(w)
	defer func() {
		os.Stdout = stdout
		os.Stderr = stderr
		glog.SetOutput(stdout)
	}()
	f()
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}

func TestAddTags(t *testing.T) {
	ctx := context.Background()
	t.Run("infof", func(t *testing.T) {
		ctx := log.AddTags(ctx, "planet", "pluto")
		output := captureStdout(t, func() {
			log.Infof(ctx, "hello world")
		})
		require.Contains(t, output, "INFO hello world planet=pluto")
	})
	t.Run("infow", func(t *testing.T) {
		ctx := log.AddTags(ctx, "planet", "pluto")
		output := captureStdout(t, func() {
			log.Infow(ctx, "hello world")
		})
		require.Contains(t, output, "INFO hello world planet=pluto")
	})
}

func TestLogf(t *testing.T) {
	old := slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(old)
	cases := []struct {
		assertion string
		f         func(context.Context, string, ...interface{})
		contains  string
	}{
		{
			"infof",
			log.Infof,
			"INFO hello world",
		},
		{
			"warnf",
			log.Warnf,
			"WARN hello world",
		},
		{
			"errorf",
			log.Errorf,
			"ERROR hello world",
		},
		{
			"debugf",
			log.Debugf,
			"DEBUG hello world",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ctx := context.Background()
			output := captureStdout(t, func() {
				c.f(ctx, "hello %s", "world")
			})
			require.Contains(t, output, c.contains)
		})
	}
}

func TestLogLeveling(t *testing.T) {
	old := slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(old)
	s := captureStdout(t, func() {
		log.Debugf(context.Background(), "foo")
		log.Debugw(context.Background(), "bar")
	})
	require.Contains(t, s, "DEBUG foo")
	require.Contains(t, s, "DEBUG bar")

	slog.SetLogLoggerLevel(slog.LevelInfo)
	s = captureStdout(t, func() {
		log.Debugf(context.Background(), "foo")
		log.Debugw(context.Background(), "bar")
	})
	require.Equal(t, "", s)
}

func TestLogw(t *testing.T) {
	old := slog.SetLogLoggerLevel(slog.LevelDebug)
	defer slog.SetLogLoggerLevel(old)
	cases := []struct {
		assertion string
		f         func(ctx context.Context, msg string, keyvals ...any)
		contains  string
	}{
		{
			"infow",
			log.Infow,
			"INFO hello world=earth",
		},
		{
			"warnw",
			log.Warnw,
			"WARN hello world=earth",
		},
		{
			"errorw",
			log.Errorw,
			"ERROR hello world=earth",
		},
		{
			"debugf",
			log.Debugw,
			"DEBUG hello world=earth",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			ctx := context.Background()
			output := captureStdout(t, func() {
				c.f(ctx, "hello", "world", "earth")
			})
			require.Contains(t, output, c.contains)
		})
	}
}
