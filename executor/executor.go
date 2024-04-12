package executor

import (
	"context"
	"errors"
	"fmt"
	"io"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/plan"
	"github.com/wkalt/dp3/tree"
	"github.com/wkalt/dp3/util"
)

/*
The executor module implements an iterator-style query executor with a limited
set of operators:
  * scan: reads tuples from a tree
  * merge: does a logtime-ordered merge of children
  * asof: joins two tables based on time proximity
  * limit: limits the number of tuples returned
  * offset: skips the first n tuples
  * filter: filters tuples based on a predicate

Queries arrive as a tree of plan nodes, which are compiled to a tree of executor
nodes. The execution tree is executed by repeatedly calling Next on the root
node until an io.EOF occurs.
*/

////////////////////////////////////////////////////////////////////////////////

// Run compiles a plan tree to an executor tree, and executes it to completion.
func Run(
	ctx context.Context,
	w io.Writer,
	node *plan.Node,
	scanFactory func(ctx context.Context, producer string, table string, start, end uint64) (*tree.Iterator, error),
) error {
	root, err := CompilePlan(ctx, node, scanFactory)
	if err != nil {
		return err
	}
	defer root.Close()
	initialized := false
	var mc *mcap.MergeCoordinator
	for {
		tuple, err := root.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next message: %w", err)
		}
		// defer initialization until we successfully pull a message to avoid
		// schema conflicts. todo: we need to be able to check the schemas prior
		// to running the executor - they should be extracted into a /schemas
		// directory and referencable by hash.
		if !initialized {
			writer, err := mcap.NewWriter(w)
			if err != nil {
				return fmt.Errorf("failed to construct mcap writer: %w", err)
			}
			defer writer.Close()
			if err := writer.WriteHeader(&fmcap.Header{}); err != nil {
				return fmt.Errorf("failed to write header: %w", err)
			}
			mc = mcap.NewMergeCoordinator(writer)
			initialized = true
		}

		if err := mc.Write(tuple.Schema, tuple.Channel, tuple.Message); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
	}
	return nil
}

type ScanFactory func(ctx context.Context, producer string, table string, start, end uint64) (*tree.Iterator, error)

// CompilePlan compiles a "plan tree" -- a tree of plan nodes -- to a tree of
// executor nodes.
func CompilePlan(
	ctx context.Context,
	node *plan.Node,
	scanFactory ScanFactory,
) (Node, error) {
	switch node.Type {
	case plan.MergeJoin:
		return compileMergeJoin(ctx, node, scanFactory)
	case plan.AsofJoin:
		return compileAsofJoin(ctx, node, scanFactory)
	case plan.Limit:
		return compileLimit(ctx, node, scanFactory)
	case plan.Offset:
		return compileOffset(ctx, node, scanFactory)
	case plan.Scan:
		return compileScan(ctx, node, scanFactory)
	default:
		return nil, fmt.Errorf("unrecognized node type %s", node.Type)
	}
}

func compileMergeJoin(ctx context.Context, node *plan.Node, sf ScanFactory) (Node, error) {
	nodes := make([]Node, len(node.Children))
	for i, child := range node.Children {
		var err error
		nodes[i], err = CompilePlan(ctx, child, sf)
		if err != nil {
			return nil, err
		}
	}
	return NewMergeNode(nodes...), nil
}

func compileAsofJoin(ctx context.Context, node *plan.Node, sf ScanFactory) (Node, error) {
	left, err := CompilePlan(ctx, node.Children[0], sf)
	if err != nil {
		return nil, err
	}
	right, err := CompilePlan(ctx, node.Children[1], sf)
	if err != nil {
		return nil, err
	}
	if l := len(node.Args); l != 2 && l != 4 {
		return nil, fmt.Errorf("expected 2 or 4 arguments, got %d", l)
	}
	var threshold uint64
	if len(node.Args) > 2 {
		units, ok := node.Args[2].(string)
		if !ok {
			return nil, errors.New("expected string units")
		}
		quantity, ok := node.Args[3].(int)
		if !ok {
			return nil, fmt.Errorf("failed to parse quantity: %w", err)
		}
		threshold = uint64(quantity) * map[string]uint64{
			"nanoseconds":  1,
			"microseconds": 1e3,
			"milliseconds": 1e6,
			"seconds":      1e9,
			"minutes":      60 * 1e9,
		}[units]
	}
	keyword, ok := node.Args[0].(string)
	if !ok {
		return nil, errors.New("expected string keyword")
	}
	immediate, ok := node.Args[1].(bool)
	if !ok {
		return nil, errors.New("expected bool immediate")
	}
	switch keyword {
	case "precedes":
		return NewAsofJoinNode(left, right, immediate, threshold), nil
	case "succeeds":
		return NewAsofJoinNode(right, left, immediate, threshold), nil
	case "neighbors":
		return nil, errors.New("not implemented")
	default:
		return nil, fmt.Errorf("unrecognized keyword %s", keyword)
	}
}

func compileLimit(ctx context.Context, node *plan.Node, sf ScanFactory) (Node, error) {
	child, err := CompilePlan(ctx, node.Children[0], sf)
	if err != nil {
		return nil, err
	}
	return NewLimitNode(*node.Limit, child), nil
}

func compileOffset(ctx context.Context, node *plan.Node, sf ScanFactory) (Node, error) {
	child, err := CompilePlan(ctx, node.Children[0], sf)
	if err != nil {
		return nil, err
	}
	return NewOffsetNode(*node.Offset, child), nil
}

func compileScan(ctx context.Context, node *plan.Node, sf ScanFactory) (Node, error) {
	table, ok := node.Args[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string table, got %T", node.Args[0])
	}
	alias, ok := node.Args[1].(string)
	if !ok {
		return nil, fmt.Errorf("expected string alias, got %T", node.Args[1])
	}
	producer, ok := node.Args[2].(string)
	if !ok {
		return nil, fmt.Errorf("expected string producer, got %T", node.Args[1])
	}
	var err error
	var start, end uint64
	if node.Args[3] == "all-time" {
		start = 0
		end = ^uint64(0)
	} else {
		start, ok = node.Args[3].(uint64)
		if !ok {
			return nil, fmt.Errorf("expected uint64 start time, got %T", node.Args[2])
		}
		end, ok = node.Args[4].(uint64)
		if !ok {
			return nil, fmt.Errorf("expected uint64 end time, got %T", node.Args[3])
		}
	}
	it, err := sf(ctx, producer, table, start, end)
	if err != nil {
		return nil, err
	}
	scan := NewScanNode(table, it)
	if len(node.Children) > 0 {
		expr := newExpression(util.When(alias != "", alias, table), node.Children[0])
		return NewFilterNode(expr.filter, scan), nil
	}

	return scan, nil
}
