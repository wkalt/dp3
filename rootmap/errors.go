package rootmap

import "fmt"

type StreamNotFoundError struct {
	ProducerID string
	Topic      string
}

func (e StreamNotFoundError) Error() string {
	return fmt.Sprintf("stream %s:%s not found", e.ProducerID, e.Topic)
}

func (e StreamNotFoundError) Is(target error) bool {
	_, ok := target.(StreamNotFoundError)
	return ok
}
