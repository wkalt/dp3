package log

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

type contextKey int

const (
	logTagKey contextKey = iota
)

func AddTags(ctx context.Context, kvs ...any) context.Context {
	if len(kvs)%2 != 0 {
		panic("log: AddTags requires an even number of arguments")
	}
	tags := ctx.Value(logTagKey)
	if tags == nil {
		tags = []any{}
	}
	return context.WithValue(
		ctx,
		logTagKey,
		append(tags.([]any), kvs...),
	)
}

func fromContext(ctx context.Context) []any {
	tags, _ := ctx.Value(logTagKey).([]any)
	return tags
}

func levelf(ctx context.Context, level slog.Level, format string, args ...any) {
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	r := slog.NewRecord(time.Now(), level, fmt.Sprintf(format, args...), pcs[0])
	tags := fromContext(ctx)
	for i := 0; i < len(tags); i += 2 {
		r.Add(tags[i].(string), tags[i+1])
	}
	handler := slog.Default().Handler()
	if handler.Enabled(ctx, level) {
		if err := slog.Default().Handler().Handle(ctx, r); err != nil {
			slog.ErrorContext(ctx, "error handling log record", "error", err)
		}
	}
}

func Infof(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelInfo, format, args...)
}

func Errorf(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelError, format, args...)
}

func Debugf(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelDebug, format, args...)
}

func Warnf(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelWarn, format, args...)
}

func levelw(ctx context.Context, level slog.Level, msg string, keyvals ...any) {
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	for i := 0; i < len(keyvals); i += 2 {
		r.Add(keyvals[i].(string), keyvals[i+1])
	}
	tags := fromContext(ctx)
	for i := 0; i < len(tags); i += 2 {
		r.Add(tags[i].(string), tags[i+1])
	}
	handler := slog.Default().Handler()
	if handler.Enabled(ctx, level) {
		if err := handler.Handle(ctx, r); err != nil {
			slog.ErrorContext(ctx, "error handling log record", "error", err)
		}
	}
}

func Infow(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelInfo, msg, keyvals...)
}

func Errorw(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelError, msg, keyvals...)
}

func Debugw(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelDebug, msg, keyvals...)
}

func Warnw(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelWarn, msg, keyvals...)
}
