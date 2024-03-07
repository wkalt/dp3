package main

import (
	"context"
	"fmt"

	"github.com/wkalt/dp3/service"
)

func main() {
	ctx := context.Background()
	dp3 := service.NewDP3Service()

	// Start the service
	if err := dp3.Start(ctx); err != nil {
		fmt.Printf("failed to start service: %v\n", err)
	}
}
