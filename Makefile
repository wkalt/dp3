test:
	GOEXPERIMENT=nocoverageredesign go test -cover ./...

lint:
	golangci-lint run ./...

build:
	go build -o dp3 ./client/dp3

clean:
	rm -rf data/*
	rm -rf waldir/*
	rm dp3.db*
