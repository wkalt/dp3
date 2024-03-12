package versionstore

import "context"

/*
The version store is used to generate version numbers for the nodestore. When a
server starts, it will reserve a large range of versions from the version store.
Taking the lock for reserving the range is assumed to be expensive which
motivates the reservation approach.

Since writes only go through one server at a time, this lets us assume that
bigger versions imply newer writes. If the cluster is rebalanced, nodes must
restart, and when they restart they will reset their counters bigger than any
version that any server has ever written or reserved.
*/

////////////////////////////////////////////////////////////////////////////////

type Versionstore interface {
	Next(ctx context.Context) (uint64, error)
}
