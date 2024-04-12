package executor

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
)

/*
Errors returned by the executor.
*/

////////////////////////////////////////////////////////////////////////////////

// FieldNotFoundError is an error returned when a field is not found.
type FieldNotFoundError struct {
	field  string
	fields []util.Named[schema.PrimitiveType]
}

func (e FieldNotFoundError) Is(target error) bool {
	_, ok := target.(FieldNotFoundError)
	return ok
}

func (e FieldNotFoundError) Error() string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("Field %s not found.", e.field))
	if len(e.fields) > 0 {
		sb.WriteString(" Available fields: ")
		for i, f := range e.fields {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(f.String())
		}
		sb.WriteString(".")
	}
	return sb.String()
}

// newErrFieldNotFound creates a new FieldNotFoundError.
func newErrFieldNotFound(field string, fields []util.Named[schema.PrimitiveType]) FieldNotFoundError {
	return FieldNotFoundError{
		field:  field,
		fields: fields,
	}
}

// ErrUnknownTable is an error returned when a table is not found.
var ErrUnknownTable = errors.New("unknown table")
