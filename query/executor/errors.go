package executor

import (
	"strings"

	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
)

type ErrFieldNotFound struct {
	Field  string
	Fields []util.Named[schema.PrimitiveType]
}

func NewErrFieldNotFound(field string, fields []util.Named[schema.PrimitiveType]) ErrFieldNotFound {
	return ErrFieldNotFound{
		Field:  field,
		Fields: fields,
	}
}

func (e ErrFieldNotFound) Error() string {
	sb := &strings.Builder{}
	sb.WriteString("field not found: ")
	sb.WriteString(e.Field)
	sb.WriteString(" (available fields: ")
	for i, f := range e.Fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(f.String())
	}
	sb.WriteString(")")
	return sb.String()
}
