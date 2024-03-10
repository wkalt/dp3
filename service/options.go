package service

// DP3Option is a functional option for the DP3 service.
type DP3Option func(*DP3Options)

// DP3Options contains options for the DP3 service.
type DP3Options struct {
	CacheSizeBytes uint64
	DataDir        string
	Port           int
}

// WithCacheSizeMegabytes sets the cache size in megabytes.
func WithCacheSizeMegabytes(size uint64) DP3Option {
	return func(opts *DP3Options) {
		opts.CacheSizeBytes = size * 1024 * 1024
	}
}

// WithDataDir sets the data directory.
func WithDataDir(dir string) DP3Option {
	return func(opts *DP3Options) {
		opts.DataDir = dir
	}
}

// WithPort sets the port to listen on.
func WithPort(port int) DP3Option {
	return func(opts *DP3Options) {
		opts.Port = port
	}
}
