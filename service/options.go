package service

type DP3Option func(*DP3Options)

type DP3Options struct {
	CacheSizeBytes uint64
	Port           int
}
