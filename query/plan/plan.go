package plan

import (
	"fmt"
	"math"
	"strings"

	"github.com/wkalt/dp3/query/ql"
)

/*
The plan module is responsible for converting raw query AST into a tree of "plan
nodes". The plan nodes mirror the structure of the executor nodes in most
respects, but are a bit more amenable to generic manipulation without
invoking the executor's dependencies on the storage system.
*/

////////////////////////////////////////////////////////////////////////////////

// NodeType is the type of a plan node.
type NodeType int

const (
	// MergeJoin is a merge join node.
	MergeJoin NodeType = iota
	// AsofJoin is an as-of join node.
	AsofJoin
	// Scan is a scan node.
	Scan
	// Limit is a limit node.
	Limit
	// Offset is an offset node.
	Offset
	// And is an and node.
	And
	// Or is an or node.
	Or
	// BinaryExpression is a binary expression node.
	BinaryExpression
)

// String returns a string representation of the node type.
func (n NodeType) String() string {
	switch n {
	case MergeJoin:
		return "merge"
	case AsofJoin:
		return "asof"
	case Scan:
		return "scan"
	case Limit:
		return "limit"
	case Offset:
		return "offset"
	case And:
		return "and"
	case Or:
		return "or"
	case BinaryExpression:
		return "binaryexpr"
	default:
		panic("unknown")
	}
}

// Node represents a plan node.
type Node struct {
	Type     NodeType
	Args     []any
	Children []*Node

	BinaryOp      *string
	BinaryOpField *string
	BinaryOpValue *ql.Value

	Offset *int
	Limit  *int
}

// traverse a plan tree, executing pre and post-order transformations.
func traverse(n *Node, pre func(n *Node), post func(n *Node)) {
	if pre != nil {
		pre(n)
	}
	for _, c := range n.Children {
		traverse(c, pre, post)
	}
	if post != nil {
		post(n)
	}
}

// String returns a string representation of the node.
func (n Node) String() string {
	switch n.Type {
	case BinaryExpression:
		return fmt.Sprintf("[binexp [%s %s %s]]", *n.BinaryOp, *n.BinaryOpField, n.BinaryOpValue)
	case Limit:
		return fmt.Sprintf("[limit %d %s]", *n.Limit, n.Children[0])
	case Offset:
		return fmt.Sprintf("[offset %d %s]", *n.Offset, n.Children[0])
	}
	children := make([]string, len(n.Children))
	for i, c := range n.Children {
		children[i] = c.String()
	}
	args := ""
	if len(n.Args) > 0 {
		for i, arg := range n.Args {
			if i > 0 {
				args += " "
			}
			args += fmt.Sprintf("%v", arg)
		}
		args = fmt.Sprintf(" (%s)", args)
	}
	childrenTerm := ""
	if len(children) > 0 {
		childrenTerm = " " + strings.Join(children, " ")
	}
	return fmt.Sprintf("[%s%s%s]", n.Type, args, childrenTerm)
}

// compileBinaryExpr compiles an AST binary expression to a plan node.
func compileBinaryExpr(expr ql.BinaryExpression) *Node {
	return &Node{
		Type: BinaryExpression,

		BinaryOp:      &expr.Op,
		BinaryOpField: &expr.Left,
		BinaryOpValue: &expr.Right,
	}
}

// wrapWithPaging wraps a plan node in limit and offset nodes according to the
// supplied list of clauses.
func wrapWithPaging(node *Node, paging []ql.PagingTerm) *Node {
	limit := -1
	offset := -1
	for _, clause := range paging {
		switch clause.Keyword {
		case "limit":
			limit = clause.Value
		case "offset":
			offset = clause.Value
		}
	}
	if offset > -1 {
		node = &Node{
			Type:     Offset,
			Offset:   &offset,
			Children: []*Node{node},
		}
	}
	if limit > -1 {
		node = &Node{
			Type:     Limit,
			Limit:    &limit,
			Children: []*Node{node},
		}
	}
	return node
}

// compileAJ compiles an AST as-of join to a plan node.
func compileAJ(left *Node, ast ql.AJ) *Node {
	right := compileSelect(ast.Select)
	args := []any{
		ast.Keyword,
	}
	if ast.Constraint != nil {
		args = append(args,
			ast.Constraint.Units,
			ast.Constraint.Quantity,
		)
	}
	return &Node{
		Type:     AsofJoin,
		Children: []*Node{left, right},
		Args:     args,
	}
}

// compileMJ compiles an AST merge join to a plan node.
func compileMJ(left *Node, ast ql.MJ) *Node {
	right := compileSelect(ast.Select)
	return &Node{
		Type:     MergeJoin,
		Children: []*Node{left, right},
	}
}

// compileSelect compiles an AST select to a plan node.
func compileSelect(ast ql.Select) *Node {
	base := &Node{
		Type: Scan,
		Args: []any{ast.Entity},
	}
	if ast.AJ != nil {
		return compileAJ(base, *ast.AJ)
	}
	if ast.MJ != nil {
		return compileMJ(base, *ast.MJ)
	}
	return base
}

// CompileQuery compiles an AST query to a plan node.
func CompileQuery(ast ql.Query) (*Node, error) {
	start := int64(0)
	end := int64(math.MaxInt64)
	var err error
	if ast.Between != nil {
		start, err = ast.Between.From.Nanos()
		if err != nil {
			return nil, fmt.Errorf("failed to parse start time: %w", err)
		}
		end, err = ast.Between.To.Nanos()
		if err != nil {
			return nil, fmt.Errorf("failed to parse end time: %w", err)
		}
	}
	producer := ast.From
	base := compileSelect(ast.Select)
	// Push where clauses down to the appropriate scan nodes, and also add
	// target and start/end time constraints.
	traverse(base, nil, func(n *Node) {
		if n.Type != Scan {
			return
		}
		table := n.Args[0]
		for _, where := range ast.Where {
			for _, binexp := range where.AndExprs {
				if strings.HasPrefix(binexp.Left, fmt.Sprint(table)+".") {
					n.Children = append(n.Children, compileBinaryExpr(binexp))
				}
			}
		}
		n.Args = append(n.Args, producer)
		if start == 0 && end == math.MaxInt64 {
			n.Args = append(n.Args, "all-time")
		} else {
			n.Args = append(n.Args, uint64(start), uint64(end))
		}
	})
	if len(ast.PagingClause) > 0 {
		base = wrapWithPaging(base, ast.PagingClause)
	}
	return base, nil
}
