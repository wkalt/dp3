package log

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

/*
log implements context-based logging using the slog structured logging package.
All logging in dp3 should use these functions. This is for ergonomics and also
for the "AddTags" functionality, which adds a log key to the context that is
then propagated in all descendent logging calls. We use this for instance, to
ensure that all logging related to a request is tagged with the request ID.

There are "f" and "w" versions of each function. The "f" version takes a format
string and parameters, and the "w" version takes an even-length list of
key-value pairs.
*/

////////////////////////////////////////////////////////////////////////////////

type contextKey int

const (
	logTagKey contextKey = iota
)

// AddTags adds key-value pairs to the log context.
func AddTags(ctx context.Context, kvs ...any) context.Context {
	if len(kvs)%2 != 0 {
		panic("log: AddTags requires an even number of arguments")
	}
	value := ctx.Value(logTagKey)
	tags := []any{}
	if value != nil {
		tagsValue, ok := value.([]any)
		if !ok {
			panic("log: invalid log tags value")
		}
		tags = append(tags, tagsValue...)
	}
	return context.WithValue(
		ctx,
		logTagKey,
		append(tags, kvs...),
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
		key, ok := tags[i].(string)
		if !ok {
			panic("log: invalid log tag key")
		}
		r.Add(key, tags[i+1])
	}
	handler := slog.Default().Handler()
	if handler.Enabled(ctx, level) {
		if err := slog.Default().Handler().Handle(ctx, r); err != nil {
			slog.ErrorContext(ctx, "error handling log record", "error", err)
		}
	}
}

// Infof logs a message with some additional context.
func Infof(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelInfo, format, args...)
}

// Errorf logs an error message with some additional context.
func Errorf(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelError, format, args...)
}

// Debugf logs a debug message with some additional context.
func Debugf(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelDebug, format, args...)
}

// Warnf logs a warning message with some additional context.
func Warnf(ctx context.Context, format string, args ...any) {
	levelf(ctx, slog.LevelWarn, format, args...)
}

func levelw(ctx context.Context, level slog.Level, msg string, keyvals ...any) {
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	for i := 0; i < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			panic("log: invalid log key")
		}
		r.Add(key, keyvals[i+1])
	}
	tags := fromContext(ctx)
	for i := 0; i < len(tags); i += 2 {
		key, ok := tags[i].(string)
		if !ok {
			panic("log: invalid log tag key")
		}
		r.Add(key, tags[i+1])
	}
	handler := slog.Default().Handler()
	if handler.Enabled(ctx, level) {
		if err := handler.Handle(ctx, r); err != nil {
			slog.ErrorContext(ctx, "error handling log record", "error", err)
		}
	}
}

// Infow logs a message with some additional context.
func Infow(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelInfo, msg, keyvals...)
}

// Errorw logs an error message with some additional context.
func Errorw(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelError, msg, keyvals...)
}

// Debugw logs a debug message with some additional context.
func Debugw(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelDebug, msg, keyvals...)
}

// Warnw logs a warning message with some additional context.
func Warnw(ctx context.Context, msg string, keyvals ...any) {
	levelw(ctx, slog.LevelWarn, msg, keyvals...)
}
