package rootmap

type StreamNotFoundError struct {
	StreamID string
}

func (e StreamNotFoundError) Error() string {
	return "stream " + e.StreamID + " not found"
}

func (e StreamNotFoundError) Is(target error) bool {
	_, ok := target.(StreamNotFoundError)
	return ok
}
