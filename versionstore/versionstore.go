package versionstore

type Versionstore interface {
	Next() (uint64, error)
}
