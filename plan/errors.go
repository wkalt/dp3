package plan

type BadPlanError struct {
	Err error
}

func (e BadPlanError) Error() string {
	return e.Err.Error()
}

func (e BadPlanError) Is(target error) bool {
	_, ok := target.(BadPlanError)
	return ok
}
