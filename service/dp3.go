package service

import (
	"context"
	"log"
)

type DP3 struct {
}

func (dp3 *DP3) Start(ctx context.Context) {
	log.Println("Starting dp3 service...")
	<-ctx.Done()
}

func NewDP3Service() *DP3 {
	return &DP3{}
}
