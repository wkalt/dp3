package rootmap

import "fmt"

type StreamNotFoundError struct {
	Database   string
	ProducerID string
	Topic      string
}

func (e StreamNotFoundError) Error() string {
	return fmt.Sprintf("stream %s:%s:%s not found", e.Database, e.ProducerID, e.Topic)
}

func NewStreamNotFoundError(database, producerID, topic string) StreamNotFoundError {
	return StreamNotFoundError{database, producerID, topic}
}

func (e StreamNotFoundError) Is(target error) bool {
	_, ok := target.(StreamNotFoundError)
	return ok
}
