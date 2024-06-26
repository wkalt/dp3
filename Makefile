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

deploy: build
	scp dp3 web@wyattalt.com:~/
	rsync -av ~/.dp3/ web@wyattalt.com:~/
	ssh web@wyattalt.com "mv ~/dp3 ~/bin/dp3"
	ssh web@wyattalt.com "systemctl --user daemon-reload"
	ssh web@wyattalt.com "systemctl restart --user server.dp3.dev"
	ssh web@wyattalt.com "systemctl restart --user demo.dp3.dev"
