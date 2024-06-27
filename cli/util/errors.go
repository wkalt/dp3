package util

type APIError struct {
	err    string
	detail string
}

func (e APIError) Error() string {
	return e.err
}

func (e APIError) Detail() string {
	return e.detail
}

func NewAPIError(err string, detail string) APIError {
	return APIError{
		err:    err,
		detail: detail,
	}
}
