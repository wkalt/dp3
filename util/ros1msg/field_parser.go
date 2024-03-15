package ros1msg

import (
	"fmt"

	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/schema"
)

/*
Public functions related to parsing ROS1 message bytes. Contains two main
methods - ParseMessage and AnalyzeSchema. AnalyzeSchema is called once on a
schema.Schema, to generate names for all "values of interest". This involves
some string concatenation activity that is otherwise unrelated to parsing.

ParseMessage is called once per message and extracts the same interesting values
via duplicated code logic - the underlying mechanics must be kept in sync.

On each call to ParseMessage, results may be zipped up with the
previously-obtained schema analysis to produce an association of nested paths to
values.
*/

////////////////////////////////////////////////////////////////////////////////

// ParseMessage extracts all interesting values from an MCAP message, according
// to the construction of the supplied field skipper.
func ParseMessage(parser Parser, data []byte, values *[]any) error {
	_, err := parser(data, values, true)
	if err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}
	return nil
}

// AnalyzeSchema returns a list of Named[schema.PrimitiveType] that represent
// interesting values in a message. The length and ordering of this list match
// the response of ParseMessage.
func AnalyzeSchema(s schema.Schema) []util.Named[schema.PrimitiveType] {
	fields := []util.Named[schema.PrimitiveType]{}
	for _, f := range s.Fields {
		types := []schema.Type{f.Type}
		names := []string{f.Name}
		for len(types) > 0 {
			t := types[0]
			types = types[1:]
			name := names[0]
			names = names[1:]
			if t.Primitive > 0 {
				fields = append(fields, util.NewNamed(name, t.Primitive))
				continue
			}
			if t.Array {
				if t.FixedSize > 0 && t.FixedSize < 10 {
					elementtypes := make([]schema.Type, 0, t.FixedSize+len(types))
					elementnames := make([]string, 0, t.FixedSize+len(names))
					for i := 0; i < t.FixedSize; i++ {
						elementtypes = append(elementtypes, *t.Items)
						elementnames = append(elementnames, fmt.Sprintf("%s[%d]", name, i))
					}
					// straight to the front
					types = append(elementtypes, types...)
					names = append(elementnames, names...)
				}
				continue
			}
			if t.Record {
				for _, f := range t.Fields {
					types = append(types, f.Type)
					names = append(names, name+"."+f.Name)
				}
				continue
			}
		}
	}
	return fields
}

// GenParser converts a schema.Schema to a ROS1 fieldskipper.
func GenParser(s schema.Schema) Parser {
	return recordToFieldParser(s.Fields)
}
