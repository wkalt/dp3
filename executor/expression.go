package executor

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/ql"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/ros1msg"
	"github.com/wkalt/dp3/util/schema"
)

/*
This file is responsible for compiling where clauses into filter functions that
can be executed on message bytes.

The main complication with this is that it must be done dynamically and during
query execution -- not before. The reason is that we don't know the schema of
the data until we see it from the leaf nodes, and our trees/tables are not
guaranteed to have a fully consistent schema. These issues can probably be
addressed with time through external storage/caching of schemas. Today we don't
have it though.

Instead, we use the "expression" structure to hold state about the filters we
have compiled for schemas we have seen in the course of a query execution.

A filter has signature func(*Tuple) (bool, error), where the bool indicates
whether the evaluted tuple should be passed up the chain. A where condition
enters this infrastructure as either an "or", "and", or "binary expression".
*/

////////////////////////////////////////////////////////////////////////////////

// expression holds a where clause and manages the compilation of new filters
// for it as new schemas are observed in the playback stream.
type expression struct {
	filters map[*fmcap.Schema]func(*tuple) (bool, error)
	node    *plan.Node
	target  string
}

// newExpression creates a new expression.
func newExpression(target string, node *plan.Node) *expression {
	return &expression{
		filters: make(map[*fmcap.Schema]func(*tuple) (bool, error)),
		target:  target,
		node:    node,
	}
}

// filter evaluates a tuple against the where clause. This is the function
// passed to the filter node constructor.
func (e *expression) filter(t *tuple) (bool, error) {
	fn, ok := e.filters[t.schema]
	if !ok {
		var err error
		fn, err = e.compileFilter(t.schema)
		if err != nil {
			return false, err
		}
		e.filters[t.schema] = fn
	}
	return fn(t)
}

func gatherInvolvedColumns(node *plan.Node) ([]string, error) {
	terms := []string{}
	err := plan.Traverse(node, func(node *plan.Node) error {
		if node.Type == plan.BinaryExpression {
			_, dealiased, found := strings.Cut(*node.BinaryOpField, ".")
			if !found {
				return fmt.Errorf("failed to dealias field %s", *node.BinaryOpField)
			}
			terms = append(terms, dealiased)
		}
		return nil
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to gather involved terms: %w", err)
	}
	return terms, nil
}

// compileFilter compiles a filter function for a schema.
func (e *expression) compileFilter(targetSchema *fmcap.Schema) (func(t *tuple) (bool, error), error) { // nolint: funlen
	pkg, name, found := strings.Cut(targetSchema.Name, "/")
	if !found {
		pkg = ""
		name = targetSchema.Name
	}
	parsed, err := ros1msg.ParseROS1MessageDefinition(pkg, name, targetSchema.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message definition %s: %w", targetSchema.Name, err)
	}
	// available and requested columns for the schema/query.
	available := schema.AnalyzeSchema(*parsed)
	requested, err := gatherInvolvedColumns(e.node)
	if err != nil {
		return nil, fmt.Errorf("failed to rewrite where clause: %w", err)
	}
	// find the annotated available column associated with each requested. If
	// any are not available, error here.
	columns := make([]util.Named[schema.PrimitiveType], 0, len(requested))
	colnames := make([]string, 0, len(requested))
	for _, colname := range requested {
		var found bool
		for _, field := range available {
			if field.Name == colname {
				columns = append(columns, field)
				colnames = append(colnames, colname)
				found = true
				break
			}
		}
		if !found {
			return nil, newErrFieldNotFound(e.target+"."+colname, available)
		}
	}
	var filterfn func([]any) (bool, error)
	switch e.node.Type {
	case plan.Or:
		filterfn, err = e.compileOrExpression(columns, e.node)
		if err != nil {
			return nil, fmt.Errorf("failed to compile filter function for %s: %w", targetSchema.Name, err)
		}
	case plan.BinaryExpression:
		filterfn, err = e.compileBinaryExpression(columns, e.node)
		if err != nil {
			return nil, fmt.Errorf("failed to compile filter function for %s: %w", targetSchema.Name, err)
		}
	case plan.And:
		filterfn, err = e.compileAndExpression(columns, e.node)
		if err != nil {
			return nil, fmt.Errorf("failed to compile filter function for %s: %w", targetSchema.Name, err)
		}
	}
	decoder := ros1msg.NewDecoder(nil)
	parser, err := schema.NewParser(parsed, colnames, decoder)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser for %s: %w", targetSchema.Name, err)
	}
	return func(t *tuple) (bool, error) {
		_, values, err := parser.Parse(t.message.Data)
		if err != nil {
			return false, fmt.Errorf("failed to parse message: %w", err)
		}
		return filterfn(values)
	}, nil
}

// compileOrExpression compiles an "or" expression. An or expression is a list
// of "and" expressions.
func (e *expression) compileOrExpression(
	fields []util.Named[schema.PrimitiveType],
	node *plan.Node,
) (func(values []any) (bool, error), error) {
	var err error
	orterms := make([]func([]any) (bool, error), len(node.Children))
	for i, child := range node.Children {
		orterms[i], err = e.compileAndExpression(fields, child)
		if err != nil {
			return nil, err
		}
	}
	return func(values []any) (bool, error) {
		for _, term := range orterms {
			if ok, err := term(values); ok || err != nil {
				return ok, err
			}
		}
		return false, nil
	}, nil
}

// compileBinaryExpression compiles a binary expression.
func (e *expression) compileBinaryExpression(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
) (func([]any) (bool, error), error) {
	if expr.Type != plan.BinaryExpression {
		return nil, errors.New("not a binary expression")
	}
	alias, dealiased, found := strings.Cut(*expr.BinaryOpField, ".")
	if !found {
		return nil, fmt.Errorf("invalid field %s", *expr.BinaryOpField)
	}

	// ignore clauses not targeted at this table. This should have been
	// intercepted in the plan step, so should not occur in practice.
	if alias != e.target {
		return nil, ErrUnknownTable
	}

	switch *expr.BinaryOp {
	case "=":
		return e.compileExprEquals(fields, expr, dealiased)
	case "<":
		return e.compileExprLessThan(fields, expr, dealiased)
	case ">":
		return e.compileExprGreaterThan(fields, expr, dealiased)
	case "<=":
		return e.compileExprLessThanEquals(fields, expr, dealiased)
	case ">=":
		return e.compileExprGreaterThanEquals(fields, expr, dealiased)
	case "!=":
		return e.compileExprNotEquals(fields, expr, dealiased)
	case "~":
		return e.compileExprRegex(fields, expr, dealiased)
	case "~*":
		return e.compileExprCaseInsensitiveRegex(fields, expr, dealiased)
	default:
		return nil, fmt.Errorf("unrecognized operator %s", *expr.BinaryOp)
	}
}

// compileAndExpression compiles an "and" expression. An and expression is a
// list of binary expressions, and evaluates to true if all its children do.
func (e *expression) compileAndExpression(
	fields []util.Named[schema.PrimitiveType],
	node *plan.Node,
) (func(values []any) (bool, error), error) {
	terms := make([]func([]any) (bool, error), 0, len(node.Children))
	for _, expr := range node.Children {
		term, err := e.compileBinaryExpression(fields, expr)
		if err != nil {
			return nil, err
		}
		if err == nil {
			terms = append(terms, term)
		}
	}
	return func(values []any) (bool, error) {
		for _, term := range terms {
			if ok, err := term(values); !ok || err != nil {
				return ok, err
			}
		}
		return true, nil
	}, nil
}

// compileEqualBool returns a function that compares a boolean field to a
// boolean value.
func compileEqualBool(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Bool == nil {
		return nil, fmt.Errorf("boolean field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(bool)
		if !ok {
			return false, fmt.Errorf("field %d is not a boolean", i)
		}
		return v == *value.Bool, nil
	}, nil
}

// compileEqualString returns a function that compares a string field to a
// string value.
func compileEqualString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		s, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return s == *value.Text, nil
	}, nil
}

// compileEqualUint8 returns a function that compares a uint8 field to a uint8
// value.
func compileEqualUint8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint8)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint8", i)
		}
		return int64(v) == *value.Integer, nil
	}, nil
}

// compileEqualUint16 returns a function that compares a uint16 field to a uint16
// value.
func compileEqualUint16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint16)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint16", i)
		}
		return int64(v) == *value.Integer, nil
	}, nil
}

// compileEqualUint32 returns a function that compares a uint32 field to a uint32
// value.
func compileEqualUint32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint32)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint32", i)
		}
		return int64(v) == *value.Integer, nil
	}, nil
}

// compileEqualUint64 returns a function that compares a uint64 field to a uint64
// value.
func compileEqualUint64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint64)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint64", i)
		}
		return int64(v) == *value.Integer, nil
	}, nil
}

// compileEqualInt8 returns a function that compares an int8 field to an int8
// value.
func compileEqualInt8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int8)
		if !ok {
			return false, fmt.Errorf("field %d is not an int8", i)
		}
		return int64(v) == *value.Integer, nil
	}, nil
}

// compileEqualInt16 returns a function that compares an int16 field to an int16
// value.
func compileEqualInt16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int16)
		if !ok {
			return false, fmt.Errorf("field %d is not an int16", i)
		}
		return int64(v) == *value.Integer, nil
	}, nil
}

// compileEqualInt32 returns a function that compares an int32 field to an int32
// value.
func compileEqualInt32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int32)
		if !ok {
			return false, fmt.Errorf("field %d is not an int32", i)
		}
		return int64(v) == *value.Integer, nil
	}, nil
}

// compileEqualInt64 returns a function that compares an int64 field to an int64
// value.
func compileEqualInt64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int64)
		if !ok {
			return false, fmt.Errorf("field %d is not an int64", i)
		}
		return v == *value.Integer, nil
	}, nil
}

// compileEqualFloat32 returns a function that compares a float32 field to a
// float32 value.
func compileEqualFloat32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float32 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float32)
		if !ok {
			return false, fmt.Errorf("field %d is not a float32", i)
		}
		return float64(v) == term, nil
	}, nil
}

// compileEqualFloat64 returns a function that compares a float64 field to a
// float64 value.
func compileEqualFloat64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float64 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float64)
		if !ok {
			return false, fmt.Errorf("field %d is not a float64", i)
		}
		return v == term, nil
	}, nil
}

// compileExprEquals compiles an equals expression by dispatching to the filter
// of the appropriate type.
func (e *expression) compileExprEquals(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	value := *expr.BinaryOpValue
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.BOOL:
				return compileEqualBool(i, value)
			case schema.STRING:
				return compileEqualString(i, value)
			case schema.UINT8:
				return compileEqualUint8(i, value)
			case schema.UINT16:
				return compileEqualUint16(i, value)
			case schema.UINT32:
				return compileEqualUint32(i, value)
			case schema.UINT64:
				return compileEqualUint64(i, value)
			case schema.INT8:
				return compileEqualInt8(i, value)
			case schema.INT16:
				return compileEqualInt16(i, value)
			case schema.INT32:
				return compileEqualInt32(i, value)
			case schema.INT64:
				return compileEqualInt64(i, value)
			case schema.FLOAT32:
				return compileEqualFloat32(i, value)
			case schema.FLOAT64:
				return compileEqualFloat64(i, value)
			default:
				return nil, fmt.Errorf("unsupported type for equals %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}

// compileLessThanUint8 returns a function that compares a uint8 field to a
// uint8 value.
func compileLessThanUint8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint8)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint8", i)
		}
		return int64(v) < *value.Integer, nil
	}, nil
}

// compileLessThanUint16 returns a function that compares a uint16 field to a
// uint16 value.
func compileLessThanUint16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint16)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint16", i)
		}
		return int64(v) < *value.Integer, nil
	}, nil
}

// compileLessThanUint32 returns a function that compares a uint32 field to a
// uint32 value.
func compileLessThanUint32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint32)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint32", i)
		}
		return int64(v) < *value.Integer, nil
	}, nil
}

// compileLessThanUint64 returns a function that compares a uint64 field to a
// uint64 value.
func compileLessThanUint64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint64)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint64", i)
		}
		return int64(v) < *value.Integer, nil
	}, nil
}

// compileLessThanInt8 returns a function that compares an int8 field to an int8
// value.
func compileLessThanInt8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int8)
		if !ok {
			return false, fmt.Errorf("field %d is not an int8", i)
		}
		return int64(v) < *value.Integer, nil
	}, nil
}

// compileLessThanInt16 returns a function that compares an int16 field to an
// int16 value.
func compileLessThanInt16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int16)
		if !ok {
			return false, fmt.Errorf("field %d is not an int16", i)
		}
		return int64(v) < *value.Integer, nil
	}, nil
}

// compileLessThanInt32 returns a function that compares an int32 field to an
// int32 value.
func compileLessThanInt32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int32)
		if !ok {
			return false, fmt.Errorf("field %d is not an int32", i)
		}
		return int64(v) < *value.Integer, nil
	}, nil
}

// compileLessThanInt64 returns a function that compares an int64 field to an
// int64 value.
func compileLessThanInt64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int64)
		if !ok {
			return false, fmt.Errorf("field %d is not an int64", i)
		}
		return v < *value.Integer, nil
	}, nil
}

// compileLessThanFloat32 returns a function that compares a float32 field to a
// float32 value.
func compileLessThanFloat32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float32 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float32)
		if !ok {
			return false, fmt.Errorf("field %d is not a float32", i)
		}
		return float64(v) < term, nil
	}, nil
}

// compileLessThanFloat64 returns a function that compares a float64 field to a
// float64 value.
func compileLessThanFloat64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float64 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float64)
		if !ok {
			return false, fmt.Errorf("field %d is not a float64", i)
		}
		return v < term, nil
	}, nil
}

// compileLessThanString returns a function that compares a string field to a
// string value.
func compileLessThanString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return v < *value.Text, nil
	}, nil
}

// compileExprLessThan compiles a less than expression by dispatching to the
// filter of the appropriate type.
// nolint: dupl
func (e *expression) compileExprLessThan(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	value := *expr.BinaryOpValue
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.UINT8:
				return compileLessThanUint8(i, value)
			case schema.UINT16:
				return compileLessThanUint16(i, value)
			case schema.UINT32:
				return compileLessThanUint32(i, value)
			case schema.UINT64:
				return compileLessThanUint64(i, value)
			case schema.INT8:
				return compileLessThanInt8(i, value)
			case schema.INT16:
				return compileLessThanInt16(i, value)
			case schema.INT32:
				return compileLessThanInt32(i, value)
			case schema.INT64:
				return compileLessThanInt64(i, value)
			case schema.FLOAT32:
				return compileLessThanFloat32(i, value)
			case schema.FLOAT64:
				return compileLessThanFloat64(i, value)
			case schema.STRING:
				return compileLessThanString(i, value)
			default:
				return nil, fmt.Errorf("unsupported type for less than %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}

// compileGreaterThanUint8 returns a function that compares a uint8 field to a
// uint8 value.
func compileGreaterThanUint8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint8)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint8", i)
		}
		return int64(v) > *value.Integer, nil
	}, nil
}

// compileGreaterThanUint16 returns a function that compares a uint16 field to a
// uint16 value.
func compileGreaterThanUint16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint16)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint16", i)
		}
		return int64(v) > *value.Integer, nil
	}, nil
}

// compileGreaterThanUint32 returns a function that compares a uint32 field to a
// uint32 value.
func compileGreaterThanUint32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint32)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint32", i)
		}
		return int64(v) > *value.Integer, nil
	}, nil
}

// compileGreaterThanUint64 returns a function that compares a uint64 field to a
// uint64 value.
func compileGreaterThanUint64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint64)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint64", i)
		}
		return int64(v) > *value.Integer, nil
	}, nil
}

// compileGreaterThanInt8 returns a function that compares an int8 field to an
// int8 value.
func compileGreaterThanInt8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int8)
		if !ok {
			return false, fmt.Errorf("field %d is not an int8", i)
		}
		return int64(v) > *value.Integer, nil
	}, nil
}

// compileGreaterThanInt16 returns a function that compares an int16 field to an
// int16 value.
func compileGreaterThanInt16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int16)
		if !ok {
			return false, fmt.Errorf("field %d is not an int16", i)
		}
		return int64(v) > *value.Integer, nil
	}, nil
}

// compileGreaterThanInt32 returns a function that compares an int32 field to an
// int32 value.
func compileGreaterThanInt32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int32)
		if !ok {
			return false, fmt.Errorf("field %d is not an int32", i)
		}
		return int64(v) > *value.Integer, nil
	}, nil
}

// compileGreaterThanInt64 returns a function that compares an int64 field to an
// int64 value.
func compileGreaterThanInt64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int64)
		if !ok {
			return false, fmt.Errorf("field %d is not an int64", i)
		}
		return v > *value.Integer, nil
	}, nil
}

// compileGreaterThanFloat32 returns a function that compares a float32 field to
// a float32 value.
func compileGreaterThanFloat32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float32 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float32)
		if !ok {
			return false, fmt.Errorf("field %d is not a float32", i)
		}
		return float64(v) > term, nil
	}, nil
}

// compileGreaterThanFloat64 returns a function that compares a float64 field to
// a float64 value.
func compileGreaterThanFloat64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float64 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float64)
		if !ok {
			return false, fmt.Errorf("field %d is not a float64", i)
		}
		return v > term, nil
	}, nil
}

// compileGreaterThanString returns a function that compares a string field to a
// string value.
func compileGreaterThanString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return v > *value.Text, nil
	}, nil
}

// compileExprGreaterThan compiles a greater than expression by dispatching to
// the filter of the appropriate type.
// nolint: dupl
func (e *expression) compileExprGreaterThan(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	value := *expr.BinaryOpValue
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.UINT8:
				return compileGreaterThanUint8(i, value)
			case schema.UINT16:
				return compileGreaterThanUint16(i, value)
			case schema.UINT32:
				return compileGreaterThanUint32(i, value)
			case schema.UINT64:
				return compileGreaterThanUint64(i, value)
			case schema.INT8:
				return compileGreaterThanInt8(i, value)
			case schema.INT16:
				return compileGreaterThanInt16(i, value)
			case schema.INT32:
				return compileGreaterThanInt32(i, value)
			case schema.INT64:
				return compileGreaterThanInt64(i, value)
			case schema.FLOAT32:
				return compileGreaterThanFloat32(i, value)
			case schema.FLOAT64:
				return compileGreaterThanFloat64(i, value)
			case schema.STRING:
				return compileGreaterThanString(i, value)
			default:
				return nil, fmt.Errorf("unsupported type for less than %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}

// compileLessThanEqualsUint8 returns a function that compares a uint8 field to
// a uint8 value.
func compileLessThanEqualsUint8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint8)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint8", i)
		}
		return int64(v) <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsUint16 returns a function that compares a uint16 field to
// a uint16 value.
func compileLessThanEqualsUint16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint16)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint16", i)
		}
		return int64(v) <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsUint32 returns a function that compares a uint32 field to
// a uint32 value.
func compileLessThanEqualsUint32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint32)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint32", i)
		}
		return int64(v) <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsUint64 returns a function that compares a uint64 field to
// a uint64 value.
func compileLessThanEqualsUint64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint64)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint64", i)
		}
		return int64(v) <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsInt8 returns a function that compares an int8 field to an
// int8 value.
func compileLessThanEqualsInt8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int8)
		if !ok {
			return false, fmt.Errorf("field %d is not an int8", i)
		}
		return int64(v) <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsInt16 returns a function that compares an int16 field to
// an int16 value.
func compileLessThanEqualsInt16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int16)
		if !ok {
			return false, fmt.Errorf("field %d is not an int16", i)
		}
		return int64(v) <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsInt32 returns a function that compares an int32 field to
// an int32 value.
func compileLessThanEqualsInt32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int32)
		if !ok {
			return false, fmt.Errorf("field %d is not an int32", i)
		}
		return int64(v) <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsInt64 returns a function that compares an int64 field to
// an int64 value.
func compileLessThanEqualsInt64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int64)
		if !ok {
			return false, fmt.Errorf("field %d is not an int64", i)
		}
		return v <= *value.Integer, nil
	}, nil
}

// compileLessThanEqualsFloat32 returns a function that compares a float32 field
// to a float32 value.
func compileLessThanEqualsFloat32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float32 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float32)
		if !ok {
			return false, fmt.Errorf("field %d is not a float32", i)
		}
		return float64(v) <= term, nil
	}, nil
}

// compileLessThanEqualsFloat64 returns a function that compares a float64 field
// to a float64 value.
func compileLessThanEqualsFloat64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float64 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float64)
		if !ok {
			return false, fmt.Errorf("field %d is not a float64", i)
		}
		return v <= term, nil
	}, nil
}

// compileLessThanEqualsString returns a function that compares a string field
// to a string value.
func compileLessThanEqualsString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return v <= *value.Text, nil
	}, nil
}

// compileExprLessThanEquals compiles a less than or equals expression by
// dispatching to the filter of the appropriate type.
// nolint: dupl
func (e *expression) compileExprLessThanEquals(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	value := *expr.BinaryOpValue
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.UINT8:
				return compileLessThanEqualsUint8(i, value)
			case schema.UINT16:
				return compileLessThanEqualsUint16(i, value)
			case schema.UINT32:
				return compileLessThanEqualsUint32(i, value)
			case schema.UINT64:
				return compileLessThanEqualsUint64(i, value)
			case schema.INT8:
				return compileLessThanEqualsInt8(i, value)
			case schema.INT16:
				return compileLessThanEqualsInt16(i, value)
			case schema.INT32:
				return compileLessThanEqualsInt32(i, value)
			case schema.INT64:
				return compileLessThanEqualsInt64(i, value)
			case schema.FLOAT32:
				return compileLessThanEqualsFloat32(i, value)
			case schema.FLOAT64:
				return compileLessThanEqualsFloat64(i, value)
			case schema.STRING:
				return compileLessThanEqualsString(i, value)
			default:
				return nil, fmt.Errorf("unsupported type for less than %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}

// compileGreaterThanEqualsUint8 returns a function that compares a uint8 field to
// a uint8 value.
func compileGreaterThanEqualsUint8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint8)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint8", i)
		}
		return int64(v) >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsUint16 returns a function that compares a uint16 field
// to a uint16 value.
func compileGreaterThanEqualsUint16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint16)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint16", i)
		}
		return int64(v) >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsUint32 returns a function that compares a uint32 field
// to a uint32 value.
func compileGreaterThanEqualsUint32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint32)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint32", i)
		}
		return int64(v) >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsUint64 returns a function that compares a uint64 field
// to a uint64 value.
func compileGreaterThanEqualsUint64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint64)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint64", i)
		}
		return int64(v) >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsInt8 returns a function that compares an int8 field to
// an int8 value.
func compileGreaterThanEqualsInt8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int8)
		if !ok {
			return false, fmt.Errorf("field %d is not an int8", i)
		}
		return int64(v) >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsInt16 returns a function that compares an int16 field
// to an int16 value.
func compileGreaterThanEqualsInt16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int16)
		if !ok {
			return false, fmt.Errorf("field %d is not an int16", i)
		}
		return int64(v) >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsInt32 returns a function that compares an int32 field
// to an int32 value.
func compileGreaterThanEqualsInt32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int32)
		if !ok {
			return false, fmt.Errorf("field %d is not an int32", i)
		}
		return int64(v) >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsInt64 returns a function that compares an int64 field
// to an int64 value.
func compileGreaterThanEqualsInt64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int64)
		if !ok {
			return false, fmt.Errorf("field %d is not an int64", i)
		}
		return v >= *value.Integer, nil
	}, nil
}

// compileGreaterThanEqualsFloat32 returns a function that compares a float32
// field to a float32 value.
func compileGreaterThanEqualsFloat32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float32 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float32)
		if !ok {
			return false, fmt.Errorf("field %d is not a float32", i)
		}
		return float64(v) >= term, nil
	}, nil
}

// compileGreaterThanEqualsFloat64 returns a function that compares a float64
// field to a float64 value.
func compileGreaterThanEqualsFloat64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float64 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float64)
		if !ok {
			return false, fmt.Errorf("field %d is not a float64", i)
		}
		return v >= term, nil
	}, nil
}

// compileGreaterThanEqualsString returns a function that compares a string field
// to a string value.
func compileGreaterThanEqualsString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return v >= *value.Text, nil
	}, nil
}

// compileExprGreaterThanEquals compiles a greater than or equals expression by
// dispatching to the filter of the appropriate type.
// nolint: dupl
func (e *expression) compileExprGreaterThanEquals(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	value := *expr.BinaryOpValue
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.UINT8:
				return compileGreaterThanEqualsUint8(i, value)
			case schema.UINT16:
				return compileGreaterThanEqualsUint16(i, value)
			case schema.UINT32:
				return compileGreaterThanEqualsUint32(i, value)
			case schema.UINT64:
				return compileGreaterThanEqualsUint64(i, value)
			case schema.INT8:
				return compileGreaterThanEqualsInt8(i, value)
			case schema.INT16:
				return compileGreaterThanEqualsInt16(i, value)
			case schema.INT32:
				return compileGreaterThanEqualsInt32(i, value)
			case schema.INT64:
				return compileGreaterThanEqualsInt64(i, value)
			case schema.FLOAT32:
				return compileGreaterThanEqualsFloat32(i, value)
			case schema.FLOAT64:
				return compileGreaterThanEqualsFloat64(i, value)
			case schema.STRING:
				return compileGreaterThanEqualsString(i, value)
			default:
				return nil, fmt.Errorf("unsupported type for less than %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}

// compileEqualsUint8 returns a function that compares a uint8 field to a uint8
// value.
func compileNotEqualsUint8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint8)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint8", i)
		}
		return int64(v) != *value.Integer, nil
	}, nil
}

// compileEqualsUint16 returns a function that compares a uint16 field to a
// uint16 value.
func compileNotEqualsUint16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint16)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint16", i)
		}
		return int64(v) != *value.Integer, nil
	}, nil
}

// compileEqualsUint32 returns a function that compares a uint32 field to a
// uint32 value.
func compileNotEqualsUint32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint32)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint32", i)
		}
		return int64(v) != *value.Integer, nil
	}, nil
}

// compileEqualsUint64 returns a function that compares a uint64 field to a
// uint64 value.
func compileNotEqualsUint64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("uint64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(uint64)
		if !ok {
			return false, fmt.Errorf("field %d is not a uint64", i)
		}
		return int64(v) != *value.Integer, nil
	}, nil
}

// compileEqualsInt8 returns a function that compares an int8 field to an int8
// value.
func compileNotEqualsInt8(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int8 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int8)
		if !ok {
			return false, fmt.Errorf("field %d is not an int8", i)
		}
		return int64(v) != *value.Integer, nil
	}, nil
}

// compileEqualsInt16 returns a function that compares an int16 field to an
// int16 value.
func compileNotEqualsInt16(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int16 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int16)
		if !ok {
			return false, fmt.Errorf("field %d is not an int16", i)
		}
		return int64(v) != *value.Integer, nil
	}, nil
}

// compileEqualsInt32 returns a function that compares an int32 field to an
// int32 value.
func compileNotEqualsInt32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int32 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int32)
		if !ok {
			return false, fmt.Errorf("field %d is not an int32", i)
		}
		return int64(v) != *value.Integer, nil
	}, nil
}

// compileEqualsInt64 returns a function that compares an int64 field to an
// int64 value.
func compileNotEqualsInt64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Integer == nil {
		return nil, fmt.Errorf("int64 field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(int64)
		if !ok {
			return false, fmt.Errorf("field %d is not an int64", i)
		}
		return v != *value.Integer, nil
	}, nil
}

// compileEqualsFloat32 returns a function that compares a float32 field to a
// float32 value.
func compileNotEqualsFloat32(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float32 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float32)
		if !ok {
			return false, fmt.Errorf("field %d is not a float32", i)
		}
		return float64(v) != term, nil
	}, nil
}

// compileEqualsFloat64 returns a function that compares a float64 field to a
// float64 value.
func compileNotEqualsFloat64(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Float == nil && value.Integer == nil {
		return nil, fmt.Errorf("float64 field %d incompatible with param %s", i, value.String())
	}
	var term float64
	if value.Float != nil {
		term = *value.Float
	} else {
		term = float64(*value.Integer)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(float64)
		if !ok {
			return false, fmt.Errorf("field %d is not a float64", i)
		}
		return v != term, nil
	}, nil
}

// compileEqualsString returns a function that compares a string field to a
// string value.
func compileNotEqualsString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return v != *value.Text, nil
	}, nil
}

// compileExprNotEquals compiles a not equals expression by dispatching to the
// filter of the appropriate type.
// nolint: dupl
func (e *expression) compileExprNotEquals(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	value := *expr.BinaryOpValue
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.UINT8:
				return compileNotEqualsUint8(i, value)
			case schema.UINT16:
				return compileNotEqualsUint16(i, value)
			case schema.UINT32:
				return compileNotEqualsUint32(i, value)
			case schema.UINT64:
				return compileNotEqualsUint64(i, value)
			case schema.INT8:
				return compileNotEqualsInt8(i, value)
			case schema.INT16:
				return compileNotEqualsInt16(i, value)
			case schema.INT32:
				return compileNotEqualsInt32(i, value)
			case schema.INT64:
				return compileNotEqualsInt64(i, value)
			case schema.FLOAT32:
				return compileNotEqualsFloat32(i, value)
			case schema.FLOAT64:
				return compileNotEqualsFloat64(i, value)
			case schema.STRING:
				return compileNotEqualsString(i, value)
			default:
				return nil, fmt.Errorf("unsupported type for less than %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}

// compileExprRegexString returns a function that compares a string field to a
// regex value.
func compileExprRegexString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	re, err := regexp.Compile(strings.ToLower(*value.Text))
	if err != nil {
		return nil, fmt.Errorf("failed to compile regex %s: %w", *value.Text, err)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return re.MatchString(strings.ToLower(v)), nil
	}, nil
}

// compileExprRegex compiles a regex expression by dispatching to the filter of
// the appropriate type.
func (e *expression) compileExprRegex(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.STRING:
				return compileExprRegexString(i, *expr.BinaryOpValue)
			default:
				return nil, fmt.Errorf("unsupported type for operator '~': %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}

// compileExprCaseInsensitiveRegexString returns a function that compares a
// string field to a case-insensitive regex value.
func compileExprCaseInsensitiveRegexString(
	i int,
	value ql.Value,
) (func([]any) (bool, error), error) {
	if value.Text == nil {
		return nil, fmt.Errorf("string field %d incompatible with param %s", i, value.String())
	}
	re, err := regexp.Compile(strings.ToLower(*value.Text))
	if err != nil {
		return nil, fmt.Errorf("failed to compile regex %s: %w", *value.Text, err)
	}
	return func(values []any) (bool, error) {
		v, ok := values[i].(string)
		if !ok {
			return false, fmt.Errorf("field %d is not a string", i)
		}
		return re.MatchString(strings.ToLower(v)), nil
	}, nil
}

// compileExprCaseInsensitiveRegex compiles a case-insensitive regex expression
// by dispatching to the filter of the appropriate type.
func (e *expression) compileExprCaseInsensitiveRegex(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.STRING:
				return compileExprCaseInsensitiveRegexString(i, *expr.BinaryOpValue)
			default:
				return nil, fmt.Errorf("unsupported type for operator '~*': %s", field.Value)
			}
		}
	}
	return nil, newErrFieldNotFound(*expr.BinaryOpField, fields)
}
