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

type expression struct {
	filters map[*fmcap.Schema]func(*Tuple) (bool, error)
	node    *plan.Node
	values  []any
	target  string
}

func newExpression(target string, node *plan.Node) *expression {
	return &expression{
		filters: make(map[*fmcap.Schema]func(*Tuple) (bool, error)),
		target:  target,
		node:    node,
	}
}

func (e *expression) filter(t *Tuple) (bool, error) {
	fn, ok := e.filters[t.Schema]
	if !ok {
		var err error
		fn, err = e.compileFilter(t.Schema)
		if err != nil {
			return false, err
		}
		e.filters[t.Schema] = fn
	}
	return fn(t)
}

func (e *expression) compileFilter(schema *fmcap.Schema) (func(t *Tuple) (bool, error), error) {
	pkg, name, found := strings.Cut(schema.Name, "/")
	if !found {
		pkg = ""
		name = schema.Name
	}
	parsed, err := ros1msg.ParseROS1MessageDefinition(pkg, name, schema.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message definition %s: %w", schema.Name, err)
	}
	fields := ros1msg.AnalyzeSchema(*parsed)
	filterfn, err := e.compileOrExpr(fields, e.node)
	if err != nil {
		return nil, fmt.Errorf("failed to compile filter function for %s: %w", schema.Name, err)
	}
	parser := ros1msg.GenSkipper(*parsed)
	return func(t *Tuple) (bool, error) {
		_, err := parser.Parse(t.Message.Data, &e.values, true)
		if err != nil {
			return false, fmt.Errorf("failed to parse message: %w", err)
		}
		defer func() {
			e.values = e.values[:0]
		}()
		return filterfn(e.values)
	}, nil
}

func (e *expression) compileOrExpr(
	fields []util.Named[schema.PrimitiveType],
	node *plan.Node,
) (func(values []any) (bool, error), error) {
	var err error
	orterms := make([]func([]any) (bool, error), len(node.Children))
	for i, child := range node.Children {
		orterms[i], err = e.compileAndExpr(fields, child)
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

var ErrUnknownTable = errors.New("unknown table")

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

	// ignore clauses not targeted at this table. Note we need a better way to
	// handle this, possibly in the plan step.
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
		return e.compileExprLike(fields, expr, dealiased)
	case "~*":
		return e.compileExprILike(fields, expr, dealiased)
	default:
		return nil, fmt.Errorf("unrecognized operator %s", *expr.BinaryOp)
	}
}

func (e *expression) compileAndExpr(
	fields []util.Named[schema.PrimitiveType],
	node *plan.Node,
) (func(values []any) (bool, error), error) {
	terms := make([]func([]any) (bool, error), 0, len(node.Children))
	for _, expr := range node.Children {
		term, err := e.compileBinaryExpression(fields, expr)
		if err != nil && !errors.Is(err, ErrUnknownTable) {
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
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}

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
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}

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
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}

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
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}

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
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}

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
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}

func compileExprLikeString(
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

func (e *expression) compileExprLike(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.STRING:
				return compileExprLikeString(i, *expr.BinaryOpValue)
			default:
				return nil, fmt.Errorf("unsupported type for operator '~': %s", field.Value)
			}
		}
	}
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}

func compileExprILikeString(
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

func (e *expression) compileExprILike(
	fields []util.Named[schema.PrimitiveType],
	expr *plan.Node,
	dealiased string,
) (func([]any) (bool, error), error) {
	for i, field := range fields {
		if field.Name == dealiased {
			switch field.Value {
			case schema.STRING:
				return compileExprILikeString(i, *expr.BinaryOpValue)
			default:
				return nil, fmt.Errorf("unsupported type for operator '~*': %s", field.Value)
			}
		}
	}
	return nil, NewErrFieldNotFound(*expr.BinaryOpField, fields)
}
