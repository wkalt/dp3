package main

import (
	"context"
	"log"

	"github.com/wkalt/dp3/service"
)

// This isn't the real entrypoint - it's just used to start the vscode debugger
// til we figure a better way out. The real entrypoint is from the CLI binary.
func main() {
	ctx := context.Background()
	dp3 := service.NewDP3Service()

	// Start the service
	if err := dp3.Start(ctx); err != nil {
		log.Printf("failed to start service: %v\n", err)
	}
}
