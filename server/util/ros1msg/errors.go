package ros1msg

type ShortReadError struct {
	typeName string
}

func (e ShortReadError) Error() string {
	return "short read on " + e.typeName
}

func (e ShortReadError) Is(err error) bool {
	_, ok := err.(ShortReadError)
	return ok
}
