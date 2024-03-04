package versionstore

import "context"

type Versionstore interface {
	Next(context.Context) (uint64, error)
}
