# dp3: multimodal log database
dp3 is an experimental database for management of multimodal log data, such as
logs produced by sensors and internal processing logic on robots.

It is under development as a class project in Joe Hellerstein's CS286 class at
UC Berkeley and is not suitable for production usage.

## Quickstart

### Requirements
* Go 1.22

The following instructions will start dp3 with a data directory on local disk.

1. Build the dp3 binary.

```
    make build
```

2. Start the server.

```
    mkdir data
    ./dp3 server --data-dir data
```

3. Import your data

```
    ./dp3 import --producer my-robot example-data/fix.mcap
```

5. Query the data

```
    ./dp3 export --producer my-robot -t /fix --json
```

```json
{"topic":"/fix","sequence":1193398,"log_time":1479513060.001367422,"publish_time":1479513060.001367422,"data":{"header":{"seq":115877,"stamp":1479513060.000963926,"frame_id":"/imu"},"status":{"status":0,"service":0},"latitude":37.39954376220703,"longitude":-122.10643005371094,"altitude":-8.820882797241211,"position_covariance":[0,0,0,0,0,0,0,0,0],"position_covariance_type":0}}
{"topic":"/fix","sequence":1193405,"log_time":1479513060.003966743,"publish_time":1479513060.003966743,"data":{"header":{"seq":115878,"stamp":1479513060.003520011,"frame_id":"/imu"},"status":{"status":0,"service":0},"latitude":37.39954376220703,"longitude":-122.10643005371094,"altitude":-8.820837020874023,"position_covariance":[0,0,0,0,0,0,0,0,0],"position_covariance_type":0}}
{"topic":"/fix","sequence":1193411,"log_time":1479513060.006441819,"publish_time":1479513060.006441819,"data":{"header":{"seq":115879,"stamp":1479513060.005985975,"frame_id":"/imu"},"status":{"status":0,"service":0},"latitude":37.39954376220703,"longitude":-122.10643005371094,"altitude":-8.82080078125,"position_covariance":[0,0,0,0,0,0,0,0,0],"position_covariance_type":0}}
```

6. Inspect multigranular statistics

```

    $ ./dp3 statrange --start 2010-01-01 --end 2020-01-01 -t /fix --producer my-robot --granularity 6000

        |           Start           |            End            |  Type  |           Field           |       Name       |          Value          |
    |---------------------------|---------------------------|--------|---------------------------|------------------|-------------------------|
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | header.seq                | mean             | 63119.5                 |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | header.seq                | min              | 1                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | header.seq                | max              | 126238                  |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | header.seq                | sum              | 7.968079441e+09         |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | text   | header.frame_id           | min              | /imu                    |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | text   | header.frame_id           | max              | /imu                    |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.status             | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.status             | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.status             | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.status             | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.service            | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.service            | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.service            | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | status.service            | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | latitude                  | mean             | 37.399634673678946      |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | latitude                  | min              | 37.39691925048828       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | latitude                  | max              | 37.402809143066406      |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | latitude                  | sum              | 4.721255081935883e+06   |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | longitude                 | mean             | -122.10455175078349     |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | longitude                 | min              | -122.1085205078125      |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | longitude                 | max              | -122.10015869140625     |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | longitude                 | sum              | -1.5414234403915405e+07 |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | altitude                  | mean             | -10.400682659429746     |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | altitude                  | min              | -14.843596458435059     |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | altitude                  | max              | -6.972412109375         |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | altitude                  | sum              | -1.3129613775610924e+06 |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[0]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[0]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[0]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[0]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[1]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[1]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[1]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[1]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[2]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[2]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[2]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[2]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[3]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[3]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[3]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[3]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[4]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[4]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[4]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[4]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[5]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[5]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[5]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[5]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[6]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[6]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[6]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[6]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[7]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[7]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[7]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[7]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[8]    | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[8]    | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[8]    | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance[8]    | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance_type  | mean             | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance_type  | min              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance_type  | max              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | float  | position_covariance_type  | sum              | 0                       |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | int    |                           | messageCount     | 126238                  |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | int    |                           | byteCount        | 1.514856e+07            |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | int    |                           | minObservedTime  | 1.4795127703096174e+18  |
    | 2016-11-18T14:56:00-08:00 | 2016-11-18T16:00:00-08:00 | int    |                           | maxObservedTime  | 1.4795130859038692e+18  |

```

See `dp3 -h` for an up to date listing of commands.

## Background
Multimodal log data may be characterized by,
* High frequencies
* Large volumes
* Highly variable message sizes and schemas (images, pointclouds, text logs,
  numeric measurements, compressed video, ...)
* Various different message encodings (protobuf, ros1msg, cdr, flatbuffers, ...)
* Timeseries oriented

Common workloads on the data are,
* Stream data in time order at a point in time, for a particular device or
  simulation, on a selection of "topics", into some sort of visualization
  application such as [webviz](https://webviz.io/) or [Foxglove
  Studio](https://foxglove.dev/), or export to a local file and view with
  [rviz](https://wiki.ros.org/rviz) or otherwise locally analyze. The selection
  of topics in this kind of query can be wide - frequently in the dozens or
  hundreds, and infrequently in the thousands depending on the architecture of
  the producer.
* Run heavy computational workloads on a "narrow" selection of topics. For
  instance, run distributed Spark jobs over hundreds of terabytes of images.
  This workload may care less or not at all about ordering, but cares a lot
  about throughput, and that the available throughput, availability, and cost
  scalability effectively matches that of the underlying storage and
  networking, either on-premise or in the cloud.
* Summarize individual message field values at multiple granularities, at
  low-enough latencies to drive responsive web applications. Consider for
  instance the plotting facilities available in datadog or cloud monitoring
  systems like Cloudwatch or GCP stackdriver, which can (sometimes) plot
  multigranular statistical aggregates spanning many weeks or years and
  trillions of points in under a second.

dp3 attempts to address all three of these in a single easy-to-administer
solution.

## Architecture
The architecture of dp3 is substantially inspired by
[btrdb](https://www.usenix.org/system/files/conference/fast16/fast16-papers-andersen.pdf).
It differs in support for multimodal data and multiplexed playback, and in
drawing a slightly different contract with its consumers, based on "topics" and
"producer IDs" rather than "stream IDs".

In large deployments, dp3 is envisioned as a component within a greater
domain-specific data infrastructure. However, in smaller deployments the hope
of dp3 is that incorporation of topics and producers in the core data model
will enable orgs to make use of dp3 "off the bat" without secondary indicies.

### Glossary
* **Producer ID**: a unique identifier assigned by the user to the producer of some
  data. For instance, a device identifier or a simulation run ID.
* **Topic**: a string identifying a logical channel in the customer's data stream.
  For instance, "/images" or "/imu". See http://wiki.ros.org/Topics for more
  information on how topics relate to robot architecture.
* **MCAP**: a heterogeneously-schematized binary log container format. See
  https://mcap.dev/.

### Multigranular summarization
dp3's underlying storage is a time-partitioned tree spanning a range of time
from the epoch to a future date. The typical height of the tree is 5 but it can
vary based on parameter selection. Data is stored in the leaf nodes, and the
inner nodes contain pointers to children as well as statistical summaries of
children. Data consists of nanosecond-timestamped messages.

In the service, nodes are cached on read in an LRU cache of configurable byte
capacity. In production deployments, this cache will be sized such that most
important inner nodes will fit within it at all times. Multigranular
summarization requires traversing the tree down to a sufficiently granular
height, and then scanning the statistical summaries at that height for the
requested range of time. If the cache is performing well this operation can be
done in RAM.

### Low-latency playback
Input files are associated with a producer ID by the user. During ingestion
they are split by topic and messages are routed to a tree associated with that
topic and the producer ID. Merged playback on a selection of topics requires
doing simultaneous scans of one tree per topic, feeding into a streaming merge.

### Read scalability for ML jobs
The query execution logic of dp3 can be coded in fat client libraries in other
languages like python or scala. Large heavy read jobs can use one of these
clients to execute their business. The ML cluster simply needs to query for the
current root location, which can be done once and then passed to the job.

With the dp3 server out of the way, all the heavy reading goes straight to S3
and can scale accordingly. This mode of operation does come with some
compromises - clients are accessing data directly which complicates your ACL
management - but these complexities may be preferable to running an expensive
and dynamically scaling service that, for many of these workloads, might as
well be doing S3 passthrough.

### MCAP-based
Data in the leaf nodes is stored in MCAP format. Initial focus is on
ros1msg-serialized messages, but this should be extensible to other formats in
use. The format of the playback datastream is also MCAP.

Users who are already producing MCAP files, such as ROS 2 users, will have
automatic compatibility between dp3 and all of their internal data tooling. The
message bytes logged by the device are exactly the ones stored in the database.

Users of ROS 1 bag files can try dp3 by converting their bags to mcap with the
[mcap CLI tool](https://github.com/foxglove/mcap?tab=readme-ov-file#cli-tool).

## Developers
We use golangci-lint for linting.  To install it it follow the directions here: https://golangci-lint.run/usage/install.


### Run the tests
To run the tests:
```
    make test
```

To run the linter:
```
    make lint
```

### Build the binary
```
    make build
```

### Profiling the server
Profiles can be generated with the pprof webserver on port 6060. For example,

* Heap snapshot
```
    go tool pprof http://localhost:6060/debug/pprof/heap
```

* CPU profile
```
    go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
```

* Goroutine blocking
```
    go tool pprof http://localhost:6060/debug/pprof/block
```

* Mutex contention
```
    go tool pprof http://localhost:6060/debug/pprof/mutex
```

* Function tracing
```
    curl -o trace.out http://localhost:6060/debug/pprof/trace?seconds=5
    go tool trace trace.out
```

See https://pkg.go.dev/net/http/pprof for additional information about pprof.
