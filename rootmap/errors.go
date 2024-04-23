package rootmap

import "fmt"

type TableNotFoundError struct {
	Database   string
	ProducerID string
	Topic      string
}

func (e TableNotFoundError) Error() string {
	return fmt.Sprintf("table %s:%s:%s not found", e.Database, e.ProducerID, e.Topic)
}

func NewTableNotFoundError(database, producerID, topic string) TableNotFoundError {
	return TableNotFoundError{database, producerID, topic}
}

func (e TableNotFoundError) Is(target error) bool {
	_, ok := target.(TableNotFoundError)
	return ok
}
