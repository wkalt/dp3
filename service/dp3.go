package service

import (
	"context"
	"log"

	"github.com/google/uuid"
	"github.com/wkalt/dp3/tree"
)

type DP3 struct {
	streams map[uuid.UUID]*tree.Tree
}

func (dp3 *DP3) Start(ctx context.Context) {
	log.Println("Starting dp3 service...")
	<-ctx.Done()
}

func NewDP3Service() *DP3 {
	return &DP3{
		streams: make(map[uuid.UUID]*tree.Tree),
	}
}
