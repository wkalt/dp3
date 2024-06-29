module github.com/wkalt/dp3/cli

go 1.22.3

require (
	github.com/bmatcuk/doublestar/v4 v4.6.1
	github.com/chzyer/readline v1.5.1
	github.com/fatih/color v1.17.0
	github.com/minio/minio-go/v7 v7.0.72
	github.com/relvacode/iso8601 v1.4.0
	github.com/spf13/cobra v1.8.1
	github.com/wkalt/dp3/server v0.0.0-20240627201238-68955869f27e
	golang.org/x/sync v0.7.0
	golang.org/x/text v0.16.0
)

require (
	github.com/DataDog/sketches-go v1.4.6 // indirect
	github.com/alecthomas/participle/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/foxglove/mcap/go/mcap v1.4.1 // indirect
	github.com/go-echarts/go-echarts/v2 v2.2.3 // indirect
	github.com/go-echarts/statsview v0.3.4 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rs/cors v1.11.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	github.com/wkalt/migrate v0.0.0-20240613031459-65aa49660c06 // indirect
	golang.org/x/crypto v0.24.0 // indirect
	golang.org/x/exp v0.0.0-20240613232115-7f521ea00fb8 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/wkalt/dp3/server => ../server
