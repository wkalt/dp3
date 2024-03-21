package wal

/*
Options for the WAL manager.
*/

////////////////////////////////////////////////////////////////////////////////

import "time"

type config struct {
	mergeSizeThreshold  int
	staleBatchThreshold time.Duration
	targetFileSize      int

	inactiveBatchMergeInterval time.Duration
}

// Option is a function that modifies the WAL manager configuration.
type Option func(*config)

// WithMergeSizeThreshold sets the batch data size at which the WAL manager
// merge a batch, regardless of whether data is still coming in. Supplied in
// bytes.
func WithMergeSizeThreshold(size int) Option {
	return func(c *config) {
		c.mergeSizeThreshold = size
	}
}

// WithStaleBatchThreshold sets the time threshold after which a batch is
// considered to be no longer receiving writes, thus indicating it should be
// merged into storage (regardless of size).
func WithStaleBatchThreshold(d time.Duration) Option {
	return func(c *config) {
		c.staleBatchThreshold = d
	}
}

// WithTargetFileSize sets the target size for WAL files. When the active file
// exceeds this size, the WAL manager will close it and rotate the log.
func WithTargetFileSize(size int) Option {
	return func(c *config) {
		c.targetFileSize = size
	}
}

// WithInactiveBatchMergeInterval sets the polling interval to check for inactive batches.
func WithInactiveBatchMergeInterval(secs int) Option {
	return func(c *config) {
		c.inactiveBatchMergeInterval = time.Duration(secs) * time.Second
	}
}
