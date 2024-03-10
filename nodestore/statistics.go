package nodestore

import "fmt"

type Statistics struct {
	MessageCount uint64 `json:"messageCount"`
}

func (s *Statistics) Add(other *Statistics) {
	s.MessageCount += other.MessageCount
}

func (s *Statistics) String() string {
	return fmt.Sprintf("(count=%d)", s.MessageCount)
}
