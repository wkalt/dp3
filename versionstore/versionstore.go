package versionstore

import "context"

type Versionstore interface {
	Next(ctx context.Context) (uint64, error)
}
