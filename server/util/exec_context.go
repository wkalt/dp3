package util

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/foxglove/mcap/go/mcap"
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
