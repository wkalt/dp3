package executor

import (
	"fmt"
	"strings"

	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
)

type FieldNotFoundError struct {
	Field  string
	Fields []util.Named[schema.PrimitiveType]
}

func (e FieldNotFoundError) Is(target error) bool {
	_, ok := target.(FieldNotFoundError)
	return ok
}

func NewErrFieldNotFound(field string, fields []util.Named[schema.PrimitiveType]) FieldNotFoundError {
	return FieldNotFoundError{
		Field:  field,
		Fields: fields,
	}
}

func (e FieldNotFoundError) Error() string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("Field %s not found.", e.Field))
	if len(e.Fields) > 0 {
		sb.WriteString(" Available fields: ")
		for i, f := range e.Fields {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(f.String())
		}
		sb.WriteString(".")
	}
	return sb.String()
}
