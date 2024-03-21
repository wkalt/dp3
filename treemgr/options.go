package treemgr

import (
	"github.com/wkalt/dp3/wal"
)

type config struct {
	waldir        string
	walopts       []wal.Option
	walBufferSize int
	syncWorkers   int
}

// Option is an option for the tree manager.
type Option func(*config)

// WithWALOpts passes the supplied set of options to the walmgr constructor.
func WithWALOpts(opts ...wal.Option) Option {
	return func(c *config) {
		c.walopts = append(c.walopts, opts...)
	}
}

// WithWALDir sets the directory for the WAL.
func WithWALDir(dir string) Option {
	return func(c *config) {
		c.waldir = dir
	}
}

// WithWALBufferSize sets the buffer size of the notification channel between
// the WAL and the tree manager's batch sync workers. A zero value will result
// in an unbuffered channel.
func WithWALBufferSize(size int) Option {
	return func(c *config) {
		c.walBufferSize = size
	}
}

// WithSyncWorkers sets the tree manager's sync worker count. Sync workers are
// responsible for merging partial trees from the WAL into storage. A given
// producer/topic will always be routed to a single sync worker.
func WithSyncWorkers(n int) Option {
	return func(c *config) {
		c.syncWorkers = n
	}
}
