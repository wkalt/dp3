package rootmap

import "fmt"

type TableNotFoundError struct {
	Database string
	Producer string
	Topic    string
}

func (e TableNotFoundError) Error() string {
	return fmt.Sprintf("table %s:%s:%s not found", e.Database, e.Producer, e.Topic)
}

func NewTableNotFoundError(database, producer, topic string) TableNotFoundError {
	return TableNotFoundError{database, producer, topic}
}

func (e TableNotFoundError) Is(target error) bool {
	_, ok := target.(TableNotFoundError)
	return ok
}
