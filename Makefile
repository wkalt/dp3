test:
	GOEXPERIMENT=nocoverageredesign go test -cover ./...

lint:
	golangci-lint run ./...

build:
	go build -o dp3 ./client/dp3
