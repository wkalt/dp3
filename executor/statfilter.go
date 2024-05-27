package executor

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/util/trigram"
)

type statfilterfn func(*nodestore.Child) (bool, error)

func NewStatFilter(node *plan.Node) (statfilterfn, error) {
	if node == nil {
		return passthroughFilter, nil
	}
	return compileFilter(node)
}

func compileFilter(node *plan.Node) (statfilterfn, error) {
	switch node.Type {
	case plan.Or:
		return compileOrFilter(node)
	case plan.BinaryExpression:
		return compileBinaryExpressionFilter(node)
	case plan.And:
		return compileAndFilter(node)
	}
	return passthroughFilter, nil
}

func compileAndFilter(node *plan.Node) (statfilterfn, error) {
	filters := make([]statfilterfn, len(node.Children))
	var err error
	for i, child := range node.Children {
		filters[i], err = compileFilter(child)
		if err != nil {
			return nil, err
		}
	}
	return func(child *nodestore.Child) (bool, error) {
		for _, filter := range filters {
			ok, err := filter(child)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	}, nil
}

func compileOrFilter(node *plan.Node) (statfilterfn, error) {
	filters := make([]statfilterfn, len(node.Children))
	var err error
	for i, child := range node.Children {
		filters[i], err = compileFilter(child)
		if err != nil {
			return nil, err
		}
	}
	return func(child *nodestore.Child) (bool, error) {
		for _, filter := range filters {
			ok, err := filter(child)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}, nil
}

func compileExprEqualsIntFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(int64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x >= int64(numstat.Min) && x <= int64(numstat.Max), nil
	}, nil
}

func compileExprEqualsStringFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	s := value.Value().(string)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	signature := trigram.NewSignature(12)
	signature.AddString(s)
	return func(child *nodestore.Child) (bool, error) {
		textstat, err := child.GetTextStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		if !textstat.TrigramSignature.Contains(signature) {
			return false, nil
		}
		return s >= textstat.Min && s <= textstat.Max, nil
	}, nil
}

func compileExprEqualsFloatFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(float64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x >= numstat.Min && x <= numstat.Max, nil
	}, nil
}

func compileExprEqualsFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	switch value.Value().(type) {
	case string:
		return compileExprEqualsStringFilter(node)
	case int64:
		return compileExprEqualsIntFilter(node)
	case float64:
		return compileExprEqualsFloatFilter(node)
	case bool:
		return passthroughFilter, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", value.Value())
	}
}

func passthroughFilter(child *nodestore.Child) (bool, error) {
	return true, nil
}

func compileExprLessThanStringFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	s := value.Value().(string)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		textstat, err := child.GetTextStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return s > textstat.Min, nil
	}, nil
}

func compileExprLessThanIntFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(int64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x > int64(numstat.Min), nil
	}, nil
}

func compileExprLessThanFloatFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(float64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x > numstat.Min, nil
	}, nil
}

func compileExprLessThanFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	switch value.Value().(type) {
	case string:
		return compileExprLessThanStringFilter(node)
	case int64:
		return compileExprLessThanIntFilter(node)
	case float64:
		return compileExprLessThanFloatFilter(node)
	case bool:
		return passthroughFilter, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", value.Value())
	}
}

func compileExprGreaterThanStringFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	s := value.Value().(string)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		textstat, err := child.GetTextStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return s < textstat.Max, nil
	}, nil
}

func compileExprGreaterThanIntFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(int64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x < int64(numstat.Max), nil
	}, nil
}

func compileExprGreaterThanFloatFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(float64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x < numstat.Max, nil
	}, nil
}

func compileExprGreaterThanFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	switch value.Value().(type) {
	case string:
		return compileExprGreaterThanStringFilter(node)
	case int64:
		return compileExprGreaterThanIntFilter(node)
	case float64:
		return compileExprGreaterThanFloatFilter(node)
	case bool:
		return passthroughFilter, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", value.Value())
	}
}

func compileExprLessThanEqualString(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	s := value.Value().(string)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		textstat, err := child.GetTextStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return s >= textstat.Min, nil
	}, nil
}

func compileExprLessThanEqualInt(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(int64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x >= int64(numstat.Min), nil
	}, nil
}

func compileExprLessThanEqualFloat(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(float64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x >= numstat.Min, nil
	}, nil
}

func compileExprLessThanEqualFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	switch value.Value().(type) {
	case string:
		return compileExprLessThanEqualString(node)
	case int64:
		return compileExprLessThanEqualInt(node)
	case float64:
		return compileExprLessThanEqualFloat(node)
	case bool:
		return passthroughFilter, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", value.Value())
	}
}

func compileExprGreaterThanEqualFilter(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	switch value.Value().(type) {
	case string:
		return compileExprGreaterThanEqualString(node)
	case int64:
		return compileExprGreaterThanEqualInt(node)
	case float64:
		return compileExprGreaterThanEqualFloat(node)
	case bool:
		return passthroughFilter, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", value.Value())
	}
}

func compileExprGreaterThanEqualString(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	s := value.Value().(string)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		textstat, err := child.GetTextStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return s <= textstat.Max, nil
	}, nil
}

func compileExprGreaterThanEqualInt(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(int64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x <= int64(numstat.Max), nil
	}, nil
}

func compileExprGreaterThanEqualFloat(node *plan.Node) (statfilterfn, error) {
	value := *node.BinaryOpValue
	x := value.Value().(float64)
	_, fieldname, found := strings.Cut(*node.BinaryOpField, ".")
	if !found {
		return passthroughFilter, nil
	}
	return func(child *nodestore.Child) (bool, error) {
		numstat, err := child.GetNumStat(fieldname)
		if err != nil {
			if errors.Is(err, nodestore.ErrNoStatsFound) {
				return true, nil
			}
			return true, fmt.Errorf("failed to get statistics: %w", err)
		}
		return x <= numstat.Max, nil
	}, nil
}

func compileBinaryExpressionFilter(node *plan.Node) (statfilterfn, error) {
	switch *node.BinaryOp {
	case "=":
		return compileExprEqualsFilter(node)
	case "<":
		return compileExprLessThanFilter(node)
	case ">":
		return compileExprGreaterThanFilter(node)
	case "<=":
		return compileExprLessThanEqualFilter(node)
	case ">=":
		return compileExprGreaterThanEqualFilter(node)
	case "!=":
		return passthroughFilter, nil
	case "~":
		return passthroughFilter, nil
	case "~*":
		return passthroughFilter, nil
	default:
		return nil, fmt.Errorf("unrecognized operator %s", *node.BinaryOp)
	}
}
