SHELL:=/bin/bash

test:
	$(MAKE) -C server test
	$(MAKE) -C cli test

build:
	$(MAKE) -C cli build
	mv cli/dp3 .

lint:
	$(MAKE) -C server lint

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
