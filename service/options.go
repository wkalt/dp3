package service

import (
	"log/slog"

	"github.com/wkalt/dp3/storage"
)

/*
Functional options for the dp3 service.
*/

////////////////////////////////////////////////////////////////////////////////

// DP3Option is a functional option for the DP3 service.
type DP3Option func(*DP3Options)

// DP3Options contains options for the DP3 service.
type DP3Options struct {
	CacheSizeBytes  uint64
	Port            int
	LogLevel        slog.Level
	SyncWorkers     int
	StorageProvider storage.Provider
	DatabasePath    string
	WALDir          string
	AllowedOrigins  []string
}

// WithAllowedOrigins sets the allowed origins for CORS.
func WithAllowedOrigins(origins []string) DP3Option {
	return func(opts *DP3Options) {
		opts.AllowedOrigins = origins
	}
}

// WithCacheSizeMegabytes sets the cache size in megabytes.
func WithCacheSizeMegabytes(size uint64) DP3Option {
	return func(opts *DP3Options) {
		opts.CacheSizeBytes = size * 1024 * 1024
	}
}

// WithSyncWorkers sets the number of concurrent WAL sync workers.
func WithSyncWorkers(workers int) DP3Option {
	return func(opts *DP3Options) {
		opts.SyncWorkers = workers
	}
}

// WithWALDir sets the directory for the write-ahead log.
func WithWALDir(dir string) DP3Option {
	return func(opts *DP3Options) {
		opts.WALDir = dir
	}
}

// WithDatabasePath sets the path to the rootmap database.
func WithDatabasePath(path string) DP3Option {
	return func(opts *DP3Options) {
		opts.DatabasePath = path
	}
}

// WithPort sets the port to listen on.
func WithPort(port int) DP3Option {
	return func(opts *DP3Options) {
		opts.Port = port
	}
}

// WithLogLevel sets the log level.
func WithLogLevel(level slog.Level) DP3Option {
	return func(opts *DP3Options) {
		opts.LogLevel = level
	}
}

// WithStorageProvider sets the storage provider for primary storage.
func WithStorageProvider(store storage.Provider) DP3Option {
	return func(opts *DP3Options) {
		opts.StorageProvider = store
	}
}
