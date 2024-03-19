package ros1msg

import (
	"fmt"
	"strings"

	"github.com/wkalt/dp3/util/schema"
)

/*
This file contains the ParseROS1MessageDefinition function, which accepts a
[]byte-valued ROS1 message definition with name and package, and returns a
*schema.Schema.

It does this by calling the participle parser on the message definition to
create a participle AST, and then transforming that AST into a schema.Schema,
which will be friendlier to work with. The participle AST does not leave the
ros1msg package.
*/

////////////////////////////////////////////////////////////////////////////////

var (
	primitiveTypes = map[string]schema.PrimitiveType{ // nolint:gochecknoglobals
		"int8":     schema.INT8,
		"int16":    schema.INT16,
		"int32":    schema.INT32,
		"int64":    schema.INT64,
		"uint8":    schema.UINT8,
		"uint16":   schema.UINT16,
		"uint32":   schema.UINT32,
		"uint64":   schema.UINT64,
		"float32":  schema.FLOAT32,
		"float64":  schema.FLOAT64,
		"string":   schema.STRING,
		"bool":     schema.BOOL,
		"time":     schema.TIME,
		"duration": schema.DURATION,
		"char":     schema.CHAR,
		"byte":     schema.BYTE,
	}
)

// ParseROS1MessageDefinition parses a ROS1 message definition and returns a
// schema.Schema representation of it.
func ParseROS1MessageDefinition(pkg string, name string, msgdef []byte) (*schema.Schema, error) {
	ast, err := MessageDefinitionParser.ParseBytes("", msgdef)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ros1 message definition: %w", err)
	}
	return transformAST(pkg, name, *ast)
}

func resolveType(pkg string, subdeps map[string]Definition, t *ROSType) (*schema.Type, error) {
	primitive, isPrimitive := primitiveTypes[t.Name]
	isArray := t.Array

	if isPrimitive && !isArray {
		return &schema.Type{
			Primitive: primitive,
		}, nil
	}

	if isPrimitive && isArray {
		return &schema.Type{
			Array:     isArray,
			FixedSize: t.FixedSize,
			Items:     &schema.Type{Primitive: primitive},
		}, nil
	}

	if isArray {
		subdep, err := getSubdep(pkg, subdeps, t.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve array type %s: %w", t.Name, err)
		}
		items, err := resolveSubdef(subdeps, subdep)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve subdef type %s: %w", t.Name, err)
		}
		return &schema.Type{
			Array:     true,
			FixedSize: t.FixedSize,
			Items:     items,
		}, nil
	}

	// record type
	subdep, err := getSubdep(pkg, subdeps, t.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve record type %s/%s: %w", pkg, t.Name, err)
	}
	return resolveSubdef(subdeps, subdep)
}

func resolveSubdef(subdeps map[string]Definition, def Definition) (*schema.Type, error) {
	t := &schema.Type{
		Record: true,
		Fields: []schema.Field{},
	}
	if !strings.Contains(def.Header.Type, "/") {
		return nil, fmt.Errorf("failed to resolve subdef type %s", def.Header.Type)
	}
	pkg := strings.Split(def.Header.Type, "/")[0]
	for _, element := range def.Elements {
		switch item := element.(type) {
		case ROSField:
			resolvedType, err := resolveType(pkg, subdeps, item.Type)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve type: %w", err)
			}
			t.Fields = append(t.Fields, schema.Field{
				Name: item.Name,
				Type: *resolvedType,
			})
		default:
			continue // Skip constants.
		}
	}
	return t, nil
}

// getSubdep returns the subdep with the given name, or if that doesn't exist,
// the one with the given package and name. ros1msg allows schema names in
// messages to be either namespaced by a package or not. If they are not
// namespaced, they inherit the package of their parent message.
func getSubdep(parentPkg string, subdeps map[string]Definition, name string) (Definition, error) {
	subdep, ok := subdeps[name]
	if !ok {
		subdep, ok = subdeps[parentPkg+"/"+name]
		if !ok {
			return Definition{}, fmt.Errorf("failed to resolve subdep %s/%s", parentPkg, name)
		}
	}
	return subdep, nil
}

func transformAST(pkg string, name string, ast MessageDefinition) (*schema.Schema, error) {
	subdefinitions := make(map[string]Definition)
	for _, definition := range ast.Definitions {
		if definition.Header.Type == "std_msgs/Header" {
			subdefinitions["Header"] = definition
			subdefinitions["std_msgs/Header"] = definition
			continue
		}
		subdefinitions[definition.Header.Type] = definition
	}
	s := schema.Schema{Name: pkg + "/" + name}
	for _, element := range ast.Elements {
		switch item := element.(type) {
		case ROSField:
			resolvedType, err := resolveType(pkg, subdefinitions, item.Type)
			if err != nil {
				return nil, err
			}
			s.Fields = append(s.Fields, schema.Field{
				Name: item.Name,
				Type: *resolvedType,
			})
		default:
			continue // skip constants
		}
	}
	return &s, nil
}
