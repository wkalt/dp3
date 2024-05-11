SHELL:=/bin/bash

test:
	GOEXPERIMENT=nocoverageredesign go test -cover ./...

lint:
	golangci-lint run ./...

build:
	go build -o dp3 ./client/dp3

paperbuild:
	docker build doc/paper -t paperbuilder
	docker run -v ./doc/paper:/data paperbuilder

papercheck: paperbuild
	git status --porcelain doc/paper/dp3.pdf

clean:
	rm -rf data/*
	rm -rf waldir/*
	rm dp3.db*
