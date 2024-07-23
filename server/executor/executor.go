package executor

import (
	"context"
	"errors"
	"fmt"
	"io"

	fmcap "github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/server/mcap"
	"github.com/wkalt/dp3/server/nodestore"
	"github.com/wkalt/dp3/server/plan"
	"github.com/wkalt/dp3/server/util"
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

Queries arrive as a tree of plan nodes (represented with a root node), which are
compiled to a tree of executor nodes, which implement the Node interface.

The Node interface exposes a method Next(), which returns either a tuple or an
io.EOF error. The query is executed by repeatedly calling next on the root node
of the tree, until an io.EOF comes out.
*/

////////////////////////////////////////////////////////////////////////////////

// Run compiles a plan tree to an executor tree, and executes it to completion.
func Run(ctx context.Context, w io.Writer, node *plan.Node, scanFactory ScanFactory,
	explain bool,
	limit int,
	offset int,
	skeleton bool,
) error {
	root, err := CompilePlan(ctx, node, scanFactory)
	if err != nil {
		return err
	}
	var initialized bool
	var mc *mcap.MergeCoordinator
	ctx = util.WithContext(ctx, "query")
	root = addExternalPaging(root, limit, offset)
	for {
		tuple, err := root.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next message: %w", err)
		}
		// defer initialization until we successfully pull a message, in order
		// to let the executor error on a schema conflict if necessary.
		if !initialized {
			if mc, err = initializeMergeCoordinator(w); err != nil {
				return fmt.Errorf("failed to initialize merge coordinator: %w", err)
			}
			initialized = true
			defer mc.Close()
		}
		if !explain {
			if err := mc.Write(tuple.schema, tuple.channel, tuple.message, skeleton); err != nil {
				return fmt.Errorf("failed to write message: %w", err)
			}
		}
	}
	// If we never initialized, we got no results but also no errors. Write an
	// empty file to the output since the output writer has not otherwise been
	// initialized.
	if !initialized {
		if mc, err = initializeMergeCoordinator(w); err != nil {
			return fmt.Errorf("failed to initialize merge coordinator: %w", err)
		}
		defer mc.Close()
	}
	if err := root.Close(ctx); err != nil {
		return fmt.Errorf("failed to close root node: %w", err)
	}
	if explain {
		metadata, err := util.MetadataFromContext(ctx)
		if err != nil {
			return fmt.Errorf("failed to get metadata: %w", err)
		}
		if err := mc.WriteMetadata(metadata); err != nil {
			return fmt.Errorf("failed to write exec context metadata: %w", err)
		}
	}
	return nil
}

func initializeMergeCoordinator(w io.Writer) (*mcap.MergeCoordinator, error) {
	writer, err := mcap.NewWriter(w)
	if err != nil {
		return nil, fmt.Errorf("failed to construct mcap writer: %w", err)
	}
	if err := writer.WriteHeader(&fmcap.Header{}); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}
	mc := mcap.NewMergeCoordinator(writer)
	return mc, nil
}

type ScanFactory func(
	ctx context.Context,
	database string,
	producer string,
	table string,
	descending bool,
	start, end uint64,
	childFilter func(*nodestore.Child) (bool, error),
) (mcap.ContextMessageIterator, error)

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
	return maybeWrapWithStats(node, NewMergeNode(node.Descending, nodes...), "merge"), nil
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
		return maybeWrapWithStats(
			node,
			NewAsofJoinNode(left, right, immediate, threshold),
			"asof",
		), nil
	case "succeeds":
		return maybeWrapWithStats(
			node,
			NewAsofJoinNode(right, left, immediate, threshold),
			"asof",
		), nil
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
	return maybeWrapWithStats(node, NewLimitNode(*node.Limit, child), "limit"), nil
}

func compileOffset(ctx context.Context, node *plan.Node, sf ScanFactory) (Node, error) {
	child, err := CompilePlan(ctx, node.Children[0], sf)
	if err != nil {
		return nil, err
	}
	return maybeWrapWithStats(node, NewOffsetNode(*node.Offset, child), "offset"), nil
}

func maybeWrapWithStats(node *plan.Node, n Node, label string) Node {
	if node.Explain {
		return NewNodeStats(n, label)
	}
	return n
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
	database, ok := node.Args[2].(string)
	if !ok {
		return nil, fmt.Errorf("expected string database, got %T", node.Args[2])
	}
	producer, ok := node.Args[3].(string)
	if !ok {
		return nil, fmt.Errorf("expected string producer, got %T", node.Args[3])
	}
	var err error
	var start, end uint64
	if node.Args[4] == "all-time" {
		start = 0
		end = ^uint64(0)
	} else {
		start, ok = node.Args[4].(uint64)
		if !ok {
			return nil, fmt.Errorf("expected uint64 start time, got %T", node.Args[4])
		}
		end, ok = node.Args[5].(uint64)
		if !ok {
			return nil, fmt.Errorf("expected uint64 end time, got %T", node.Args[5])
		}
	}

	var childFilter func(*nodestore.Child) (bool, error)
	if len(node.Children) > 0 {
		childFilter, err = NewStatFilter(node.Children[0])
		if err != nil {
			return nil, err
		}
	}

	it, err := sf(ctx, database, producer, table, node.Descending, start, end, childFilter)
	if err != nil {
		return nil, err
	}

	scan := maybeWrapWithStats(node, NewScanNode(table, it), "scan")
	if len(node.Children) > 0 {
		expr := newExpression(util.When(alias != "", alias, table), node.Children[0])
		return maybeWrapWithStats(node, NewFilterNode(expr.filter, scan), "filter"), nil
	}
	return scan, nil
}

func addExternalPaging(node Node, limit int, offset int) Node {
	if offset > 0 {
		node = NewOffsetNode(offset, node)
	}
	if limit > 0 {
		node = NewLimitNode(limit, node)
	}
	return node
}
