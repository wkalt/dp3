module github.com/wkalt/dp3

go 1.21.5

require (
	github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify v1.8.4
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.15.12 // indirect
	github.com/pierrec/lz4/v4 v4.1.12 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/foxglove/mcap/go/mcap v1.2.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/relvacode/iso8601 v1.4.0
	golang.org/x/exp v0.0.0-20240222234643-814bf88cf225
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/foxglove/mcap/go/mcap => ../../repos/mcap/go/mcap
