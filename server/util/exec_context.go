package util

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/foxglove/mcap/go/mcap"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type contextKey int

const (
	ContextKey contextKey = iota
)

type Context struct {
	Name     string             `json:"name"`
	Values   map[string]float64 `json:"values"`
	Data     map[string]string  `json:"data"`
	Children []*Context         `json:"children"`

	mtx *sync.Mutex
}

func newContext(name string) *Context {
	return &Context{
		Name:   name,
		Values: make(map[string]float64),
		Data:   make(map[string]string),
		mtx:    &sync.Mutex{},
	}
}

func WithContext(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, ContextKey, newContext(name))
}

func IncContextValue(ctx context.Context, name string, inc float64) {
	c := fromContext(ctx)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.Values[name] += inc
}

func DecContextValue(ctx context.Context, name string, dec float64) {
	c := fromContext(ctx)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.Values[name] -= dec
}

func SetContextValue(ctx context.Context, name string, value float64) {
	c := fromContext(ctx)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.Values[name] = value
}

func SetContextData(ctx context.Context, key string, data string) {
	c := fromContext(ctx)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.Data == nil {
		c.Data = make(map[string]string)
	}
	c.Data[key] = data
}

func fromContext(ctx context.Context) *Context {
	value := ctx.Value(ContextKey)
	if value != nil {
		if c, ok := value.(*Context); ok {
			return c
		}
	}
	return newContext("")
}

func WithChildContext(ctx context.Context, name string) (context.Context, *Context) {
	c := fromContext(ctx)
	child := newContext(name)
	c.Children = append(c.Children, child)
	return context.WithValue(ctx, ContextKey, child), child
}

func MetadataFromContext(ctx context.Context) (*mcap.Metadata, error) {
	c := fromContext(ctx)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.ToMetadata()
}

func (c *Context) ToMetadata() (*mcap.Metadata, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("failed to exec context to JSON: %w", err)
	}
	return &mcap.Metadata{
		Name: c.Name,
		Metadata: map[string]string{
			"context": string(data),
		},
	}, nil
}

func (ec *Context) Print() string {
	buf := &bytes.Buffer{}
	queue := []Pair[int, *Context]{NewPair(0, ec.Children[0])}
	for len(queue) > 0 {
		pair := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		indent, ctx := pair.First, pair.Second

		elapsedToFirstTuple, timingNode := ctx.Values["elapsed_to_first_tuple"]
		delete(ctx.Values, "elapsed_to_first_tuple")
		elapsedToLastTuple := ctx.Values["elapsed_to_last_tuple"]
		delete(ctx.Values, "elapsed_to_last_tuple")
		tuplesOut := ctx.Values["tuples_out"]
		delete(ctx.Values, "tuples_out")
		bytesOut := ctx.Values["bytes_out"]
		delete(ctx.Values, "bytes_out")

		var labels string
		if len(ctx.Data) > 0 {
			labels += "["
			for i, key := range Okeys(ctx.Data) {
				if i > 0 {
					labels += " "
				}
				val := ctx.Data[key]
				labels += fmt.Sprintf("%s=%v", key, val)
			}
			labels += "] "
		}

		caser := cases.Title(language.English)
		var timingInfo string
		if timingNode {
			timingInfo = fmt.Sprintf("(elapsed=%d...%d, rows=%d, total=%s, width=%s)",
				int(elapsedToFirstTuple),
				int(elapsedToLastTuple),
				int(tuplesOut),
				HumanBytes(uint64(bytesOut)),
				HumanBytes(uint64(float64(bytesOut)/float64(tuplesOut))))
		}

		fmt.Fprintf(buf, "%s%s %s%s\n", strings.Repeat("  ", indent),
			caser.String(ctx.Name), labels, timingInfo)

		for _, child := range ctx.Children {
			queue = append(queue, NewPair(indent+2, child))
		}
	}

	lines := strings.Split(buf.String(), "\n")
	maxWidth := 0
	for _, line := range lines {
		if len(line) > maxWidth {
			maxWidth = len(line)
		}
	}

	output := &bytes.Buffer{}
	header := "QUERY PLAN"
	center := (maxWidth - len(header)) / 2
	headerline := fmt.Sprintf("%s%s", strings.Repeat(" ", center), header)
	fmt.Fprintln(output, headerline)
	fmt.Fprintln(output, strings.Repeat("-", maxWidth))
	fmt.Fprintln(output, buf.String())
	for _, k := range Okeys(ec.Data) {
		fmt.Fprintln(output, k+":"+ec.Data[k])
	}
	for _, k := range Okeys(ec.Values) {
		fmt.Fprintf(output, "%s: %d\n", k, int(ec.Values[k]))
	}
	if len(ec.Data) > 0 || len(ec.Values) > 0 {
		fmt.Fprintln(output)
	}
	return output.String()
}
