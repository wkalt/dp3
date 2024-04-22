package plan

import (
	"fmt"
	"math"
	"strings"

	"github.com/wkalt/dp3/ql"
	"github.com/wkalt/dp3/util"
	"golang.org/x/exp/maps"
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

	Subexpression
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
func traverse(n *Node, pre func(n *Node) error, post func(n *Node) error) error {
	if pre != nil {
		if err := pre(n); err != nil {
			return err
		}
	}
	for _, c := range n.Children {
		if err := traverse(c, pre, post); err != nil {
			return err
		}
	}
	if post != nil {
		if err := post(n); err != nil {
			return err
		}
	}
	return nil
}

// String returns a string representation of the node.
func (n Node) String() string {
	children := make([]string, len(n.Children))
	for i, c := range n.Children {
		children[i] = c.String()
	}
	switch n.Type {
	case BinaryExpression:
		return fmt.Sprintf("[binexp [%s %s %s]]", *n.BinaryOp, *n.BinaryOpField, n.BinaryOpValue)
	case Limit:
		return fmt.Sprintf("[limit %d %s]", *n.Limit, n.Children[0])
	case Offset:
		return fmt.Sprintf("[offset %d %s]", *n.Offset, n.Children[0])
	case AsofJoin:
		args := []string{
			fmt.Sprintf("%v", n.Args[0]),
			util.When(n.Args[1].(bool), "immediate", "full"), // this is the reason for the separate case
		}
		if len(n.Args) > 2 {
			args = append(args, fmt.Sprint(n.Args[2]), fmt.Sprint(n.Args[3]))
		}
		argsStr := " (" + strings.Join(args, " ") + ") "
		return fmt.Sprintf("[%s%s%s]", n.Type, argsStr, strings.Join(children, " "))
	}
	args := ""
	if len(n.Args) > 0 {
		count := 0
		for _, arg := range n.Args {
			term := fmt.Sprintf("%v", arg)
			if term == "" {
				continue
			}
			if count > 0 {
				args += " "
			}
			args += fmt.Sprintf("%v", arg)
			count++
		}
		args = fmt.Sprintf(" (%s)", args)
	}
	childrenTerm := ""
	if len(children) > 0 {
		childrenTerm = " " + strings.Join(children, " ")
	}
	return fmt.Sprintf("[%s%s%s]", n.Type, args, childrenTerm)
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
		ast.Immediate,
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
		Args: []any{ast.Entity, ast.Alias},
	}
	if ast.AJ != nil {
		return compileAJ(base, *ast.AJ)
	}
	if ast.MJ != nil {
		return compileMJ(base, *ast.MJ)
	}
	return base
}

// compileCondition compiles an AST condition to a plan node.
func compileCondition(ast ql.Condition) *Node {
	// if RHS is nil, this is a subexpression.
	if ast.RHS == nil && ast.Operand.Subexpression != nil {
		return compileExpression(*ast.Operand.Subexpression)
	}

	// otherwise it's a binary expression.
	op := ast.RHS.Op
	value := ast.RHS.Value
	lhs := ast.Operand.Value
	return &Node{
		Type:          BinaryExpression,
		BinaryOp:      &op,
		BinaryOpField: lhs,
		BinaryOpValue: &value,
	}
}

// compileOrCondition compiles an AST or condition to a plan node.
func compileOrCondition(ast ql.OrCondition) *Node {
	children := make([]*Node, len(ast.And))
	for i, clause := range ast.And {
		children[i] = compileCondition(*clause)
	}
	node := &Node{
		Type:     And,
		Children: children,
	}
	return node
}

// compileExpression compiles an AST expression to a plan node.
func compileExpression(ast ql.Expression) *Node {
	children := make([]*Node, len(ast.Or))
	for i, or := range ast.Or {
		node := compileOrCondition(*or)
		children[i] = node
	}
	return &Node{
		Type:     Or,
		Children: children,
	}
}

// computeAlias detemines the alias involved in an expression, in the binary
// expressions and scans. If more than one alias is involved, or no aliases are
// found, an error is returned.
func computeAlias(expr *Node) (string, error) {
	var alias string
	err := traverse(expr, nil, func(n *Node) error {
		var nodeAlias string
		switch n.Type {
		case Scan:
			table := n.Args[0].(string)
			alias := n.Args[1].(string)
			nodeAlias = util.When(alias == "", table, alias)
		case BinaryExpression:
			parts := strings.Split(*n.BinaryOpField, ".")
			if len(parts) < 2 {
				return BadPlanError{fmt.Errorf("field %s must be qualified with a dot", *n.BinaryOpField)}
			}
			nodeAlias = parts[0]
		default:
			return nil
		}
		if alias == "" {
			alias = nodeAlias
			return nil
		}
		if alias != nodeAlias {
			return BadPlanError{fmt.Errorf("expression subtree references more than one alias: %s, %s", alias, nodeAlias)}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if alias == "" {
		return "", BadPlanError{fmt.Errorf("no alias found in expression %v", expr)}
	}
	return alias, nil
}

// SplitExpression splits an expression node into one expression per alias. If
// multiple children of the expression node have the same alias, they are joined
// under an Or node.
func splitExpression(expr *Node) (map[string]*Node, error) {
	subexprs := map[string]*Node{}
	for _, child := range expr.Children {
		alias, err := computeAlias(child)
		if err != nil {
			return nil, err
		}
		if existing, ok := subexprs[alias]; ok {
			subexprs[alias] = &Node{
				Type:     Or,
				Children: []*Node{existing, child},
			}
			continue
		}
		subexprs[alias] = child
	}
	return subexprs, nil
}

// CompileQuery compiles an AST query to a plan node.
func CompileQuery(database string, ast ql.Query) (*Node, error) {
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

	subexprs := map[string]*Node{}
	if ast.Where != nil {
		expr := compileExpression(*ast.Where)
		subexprs, err = splitExpression(expr)
		if err != nil {
			return nil, err
		}
	}
	if err := traverse(
		base,
		composePushdowns(
			pullUpMergeJoins,
			pushDownFilters(subexprs, database, producer, uint64(start), uint64(end)),
			ensureAliasesResolve(),
		),
		composePushdowns(
			pullUpUnaryExprs,
		),
	); err != nil {
		return nil, err
	}
	if len(subexprs) > 0 {
		subalias := maps.Keys(subexprs)[0]
		return nil, BadPlanError{fmt.Errorf("unresolved table alias: %s", subalias)}
	}
	if len(ast.PagingClause) > 0 {
		base = wrapWithPaging(base, ast.PagingClause)
	}
	return base, nil
}

// Post-order: push the entire where clause down to each scan node,
// since we don't know about schemas here. The executor will resolve
// it according to the schema of the data and error if nonsense is
// submitted.
func pushDownFilters(exprs map[string]*Node, database string, producer string, start, end uint64) func(n *Node) error {
	return func(n *Node) error {
		if n.Type != Scan {
			return nil
		}
		table := n.Args[0].(string)
		alias := n.Args[1].(string)
		nodeAlias := util.When(alias == "", table, alias)
		if expr, ok := exprs[nodeAlias]; ok {
			n.Children = append(n.Children, expr)
			delete(exprs, nodeAlias)
		}
		n.Args = append(n.Args, database, producer)
		if start == 0 && end == math.MaxInt64 {
			n.Args = append(n.Args, "all-time")
			return nil
		}
		n.Args = append(n.Args, start, end)
		return nil
	}
}

// composePushdowns composes a list of pushdown functions into a single function.
func composePushdowns(pushdowns ...func(n *Node) error) func(n *Node) error {
	return func(n *Node) error {
		for _, pushdown := range pushdowns {
			if err := pushdown(n); err != nil {
				return err
			}
		}
		return nil
	}
}

// Ensure that every binary expression is referencing a known alias.
func ensureAliasesResolve() func(n *Node) error {
	aliases := map[string]string{}
	return func(n *Node) error {
		switch n.Type {
		case Scan:
			table, alias := n.Args[0].(string), n.Args[1].(string)
			if alias == "" {
				alias = table
			}
			if existing, ok := aliases[alias]; ok && existing != table {
				return BadPlanError{fmt.Errorf("conflicting alias %s for tables %s, %s", alias, existing, table)}
			}
			aliases[alias] = table
			return nil
		case BinaryExpression:
			left := *n.BinaryOpField
			alias := strings.Split(left, ".")[0]
			if _, ok := aliases[alias]; !ok {
				return BadPlanError{fmt.Errorf("unknown table alias %s", alias)}
			}
			return nil
		default:
			return nil
		}
	}
}

// Pull up children of unary subexpressions, or, and and.
func pullUpUnaryExprs(n *Node) error {
	switch n.Type {
	case Subexpression, Or, And:
		if len(n.Children) > 1 {
			return nil
		}
		*n = *n.Children[0]
	default:
		return nil
	}
	return nil
}

// Pull children of nested merge joins up to the top level.
func pullUpMergeJoins(n *Node) error {
	if n.Type != MergeJoin {
		return nil
	}
	newChildren := []*Node{}
	queue := []*Node{n}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if node.Type == MergeJoin {
			queue = append(queue, node.Children...)
			continue
		}
		newChildren = append(newChildren, node)
	}
	n.Children = newChildren
	return nil
}
