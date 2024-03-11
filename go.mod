module github.com/wkalt/dp3

go 1.22.0

require (
	github.com/alecthomas/participle/v2 v2.1.1
	github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify v1.8.4
	go.starlark.net v0.0.0-20240307200823-981680b3e495
	google.golang.org/protobuf v1.33.0
)

require (
	github.com/foxglove/go-rosbag v0.0.5 // indirect
	github.com/foxglove/mcap/go/ros v0.0.0-20240308001013-cec54d928a2e // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.15.15 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/foxglove/mcap/go/mcap v1.2.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/relvacode/iso8601 v1.4.0
	github.com/wkalt/ros1utils v0.0.0-20240311041704-c47139f1a580
	golang.org/x/exp v0.0.0-20240222234643-814bf88cf225
	golang.org/x/sync v0.6.0
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/foxglove/mcap/go/mcap => ../../repos/mcap/go/mcap
