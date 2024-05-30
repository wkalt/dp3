package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/chzyer/readline"
	"github.com/relvacode/iso8601"
	"github.com/spf13/cobra"
	cutil "github.com/wkalt/dp3/client/dp3/util"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/treemgr"
	"github.com/wkalt/dp3/util"
	"github.com/wkalt/dp3/util/httputil"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	clientNoPager bool
)

const (
	prefix  = `# `
	artwork = `
__        _      _____ 
\ \    __| |_ __|___ / 
 \ \  / _` + "`" + ` | '_ \ |_ \ 
 / / | (_| | |_) |__) |
/_/   \__,_| .__/____/ 
           |_|         
`
)

func withPaging(pager string, f func(io.Writer) error) error {
	if pager == "" || clientNoPager {
		return f(os.Stdout)
	}

	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("failed to make a pipe: %w", err)
	}
	defer w.Close()

	stdout := os.Stdout
	stderr := os.Stderr
	stdin := os.Stdin
	os.Stdout = w

	cmd := exec.Command(pager, "-F")
	cmd.Stdin = r
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		os.Stdout = stdout
		os.Stderr = stderr
		os.Stdin = stdin
	}()

	done := make(chan struct{})

	go func() {
		_ = cmd.Run()
		done <- struct{}{}
	}()

	errs := make(chan error)
	go func() {
		if err := f(w); err != nil {
			errs <- err
		}
		if err := w.Close(); err != nil {
			fmt.Println("error closing pipe: %w", err)
		}
	}()

	select {
	case <-done:
		return nil
	case err := <-errs:
		return err
	}
}

func executeQuery(database string, query string, explain bool) error {
	req := &routes.QueryRequest{
		Query: query,
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return fmt.Errorf("error encoding request: %w", err)
	}
	url := fmt.Sprintf("%s/databases/%s/query", serverURL, database)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return fmt.Errorf("error calling export: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		response := &httputil.ErrorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}
		return cutil.NewAPIError(response.Error, response.Detail)
	}

	if !explain {
		pager := maybePager()
		return withPaging(pager, func(w io.Writer) error {
			return mcap.MCAPToJSON(w, resp.Body)
		})
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %w", err)
	}

	reader, err := mcap.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating mcap reader: %w", err)
	}
	defer reader.Close()

	info, err := reader.Info()
	if err != nil {
		return fmt.Errorf("error reading mcap info: %w", err)
	}

	for _, idx := range info.MetadataIndexes {
		if idx.Name == "query" {
			metadata, err := reader.GetMetadata(idx.Offset)
			if err != nil {
				return fmt.Errorf("error reading metadata: %w", err)
			}
			context := &util.Context{}
			if err := json.Unmarshal([]byte(metadata.Metadata["context"]), context); err != nil {
				return fmt.Errorf("error unmarshalling context: %w", err)
			}
			printExecContext(context)
			return nil
		}
	}
	return nil
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func maybePager() string {
	pager := os.Getenv("PAGER")
	if pager != "" {
		return pager
	}
	if fileExists("/usr/bin/less") {
		return "/usr/bin/less"
	}
	return ""
}

func printError(err error) {
	fmt.Println("ERROR: " + err.Error())
	apierr := cutil.APIError{}
	if errors.As(err, &apierr) {
		if apierr.Detail() != "" {
			fmt.Println("DETAIL: " + apierr.Detail())
		}
	}
}

func run() error {
	l, err := readline.NewEx(&readline.Config{
		Prompt:          "dp3:[default] # ",
		HistoryFile:     "/tmp/dp3-history.tmp",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		VimMode:         false,
	})
	if err != nil {
		return err
	}
	fmt.Print(artwork)
	fmt.Println(`Type "help" for help.`)
	fmt.Println()
	defer l.Close()
	l.CaptureExitSignal()
	log.SetOutput(l.Stderr())

	lines := []string{}
	database := "default"
	for {
		line, err := l.Readline()
		if err != nil {
			if errors.Is(err, readline.ErrInterrupt) {
				l.SetPrompt(fmt.Sprintf("dp3:[%s] # ", database))
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		line = strings.TrimSpace(line)
		chomped := strings.TrimSuffix(line, ";")

		switch {
		case line == "":
			continue
		case line == "help", strings.HasPrefix(line, ".h"):
			_, topic, _ := strings.Cut(line, " ")
			fmt.Println(help[topic])
			continue
		case strings.HasPrefix(line, ".connect"):
			parts := strings.Split(line, " ")[1:]
			if len(parts) != 1 {
				printError(errors.New("usage: .connect <database>"))
				continue
			}
			database = parts[0]
			l.SetPrompt(fmt.Sprintf("dp3:[%s] # ", database))
			continue
		case strings.HasPrefix(line, ".statrange"):
			if err := handleStatRange(database, chomped); err != nil {
				printError(err)
			}
			continue
		case strings.HasPrefix(line, ".import"):
			if err := handleImport(database, chomped); err != nil {
				printError(err)
			}
			continue
		case strings.HasPrefix(line, ".delete"):
			if err := handleDelete(database, chomped); err != nil {
				printError(err)
			}
			continue
		case strings.HasPrefix(line, ".tables"):
			if err := handleTables(database, chomped); err != nil {
				printError(err)
			}
			continue
		case strings.HasPrefix(line, ".truncate"):
			if err := handleTruncate(database, chomped); err != nil {
				printError(err)
			}
			continue
		case strings.HasPrefix(line, "."):
			printError(errors.New("unrecognized command: " + line))
			continue
		}

		explain := strings.HasPrefix(line, "explain")
		lines = append(lines, line)
		if !strings.HasSuffix(line, ";") {
			l.SetPrompt("... # ")
			continue
		}
		query := strings.Join(lines, " ")
		lines = lines[:0]
		l.SetPrompt(fmt.Sprintf("dp3:[%s] # ", database))
		l.SaveHistory(query)
		if err := executeQuery(database, query, explain); err != nil {
			printError(err)
		}
	}

	return nil
}

func handleDelete(database string, line string) (err error) {
	parts := strings.Split(line, " ")[1:]
	if len(parts) < 4 {
		return errors.New("not enough arguments")
	}
	producer := parts[0]
	topic := parts[1]

	var starttime, endtime time.Time
	if n, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
		starttime = time.Unix(0, n)
	} else {
		starttime, err = iso8601.Parse([]byte(parts[2]))
		if err != nil {
			return fmt.Errorf("failed to parse start time: %w", err)
		}
	}
	if n, err := strconv.ParseInt(parts[3], 10, 64); err == nil {
		endtime = time.Unix(0, n)
	} else {
		endtime, err = iso8601.Parse([]byte(parts[3]))
		if err != nil {
			return fmt.Errorf("failed to parse end time: %w", err)
		}
	}
	return doDelete(database, producer, topic, starttime.UnixNano(), endtime.UnixNano())
}

func doDelete(database, producer, topic string, start, end int64) error {
	req := &routes.DeleteRequest{
		Database:   database,
		ProducerID: producer,
		Topic:      topic,
		Start:      uint64(start),
		End:        uint64(end),
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return fmt.Errorf("error encoding request: %w", err)
	}
	resp, err := http.Post(serverURL+"/delete", "application/json", buf)
	if err != nil {
		return fmt.Errorf("error calling delete: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		response := &httputil.ErrorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}
		return cutil.NewAPIError(response.Error, response.Detail)
	}
	return nil
}

func handleTruncate(database string, line string) error {
	parts := strings.Split(line, " ")[1:]
	if len(parts) < 3 {
		return errors.New("not enough arguments")
	}
	producer := parts[0]
	topic := parts[1]

	if parts[2] == "now" {
		return doTruncate(database, producer, topic, time.Now().UnixNano())
	}
	if n, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
		return doTruncate(database, producer, topic, n)
	}
	timestamp, err := iso8601.Parse([]byte(parts[2]))
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}
	return doTruncate(database, producer, topic, timestamp.UnixNano())
}

func doTruncate(database, producer, topic string, timestamp int64) error {
	req := &routes.TruncateRequest{
		Database:   database,
		ProducerID: producer,
		Topic:      topic,
		Timestamp:  timestamp,
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return fmt.Errorf("error encoding request: %w", err)
	}
	resp, err := http.Post(serverURL+"/truncate", "application/json", buf)
	if err != nil {
		return fmt.Errorf("error calling truncate: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		response := &httputil.ErrorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}
		return cutil.NewAPIError(response.Error, response.Detail)
	}
	return nil
}

func handleImport(database string, line string) error {
	parts := strings.Split(line, " ")[1:]
	if len(parts) < 2 {
		return errors.New("not enough arguments")
	}
	producer := parts[0]
	pattern := parts[1]
	paths, err := doublestar.FilepathGlob(pattern)
	if err != nil {
		return fmt.Errorf("error globbing: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no files found matching %s", pattern)
	}
	workers := runtime.NumCPU() / 2
	return doImport(database, producer, paths, workers)
}

func handleStatRange(database string, line string) error {
	parts := strings.Split(line, " ")[1:]
	if len(parts) < 2 {
		return errors.New("not enough arguments")
	}
	producer := parts[0]
	topic := parts[1]
	granularity := 60
	if len(parts) > 2 {
		var err error
		granularity, err = strconv.Atoi(parts[2])
		if err != nil {
			return fmt.Errorf("invalid granularity: %w", err)
		}
	}
	start := "1970-01-01T00:00:00Z"
	end := "2050-01-01T00:00:00Z"
	if len(parts) == 5 {
		start = parts[3]
		end = parts[4]
	}
	starttime, err := iso8601.Parse([]byte(start))
	if err != nil {
		return fmt.Errorf("failed to parse start time: %w", err)
	}
	endtime, err := iso8601.Parse([]byte(end))
	if err != nil {
		return fmt.Errorf("failed to parse end time: %w", err)
	}
	pager := maybePager()
	return withPaging(pager, func(w io.Writer) error {
		return printStatRange(w, database, producer, topic, uint64(granularity)*1e9, starttime, endtime)
	})
}

func parseErrorResponse(resp *http.Response) error {
	response := &httputil.ErrorResponse{}
	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return fmt.Errorf("error decoding response: %w", err)
	}
	return cutil.NewAPIError(response.Error, response.Detail)
}

func printTables(w io.Writer, database string, producerID string, topic string) error {
	var historical bool
	if producerID != "" && topic != "" {
		historical = true
	}
	values := url.Values{}
	values.Add("producer", url.QueryEscape(producerID))
	values.Add("topic", url.QueryEscape(topic))
	values.Add("historical", strconv.FormatBool(historical))
	url := fmt.Sprintf(serverURL+"/databases/%s/tables?%s", database, values.Encode())
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error calling tables: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return parseErrorResponse(resp)
	}

	response := []treemgr.Table{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("error decoding response: %s", err)
	}

	// Display a grouping appropriate to the request.
	switch {
	case producerID == "" && topic == "":
		// present an aggregation over producers
		headers := []string{
			"Topic",
			"Timestamp",
			"Message count",
			"Bytes uncompressed",
			"Min observed time",
			"Max observed time",
		}
		data := [][]string{}
		topicListings := map[string][]treemgr.Table{}
		for _, table := range response {
			topicListings[table.Root.Topic] = append(topicListings[table.Root.Topic], table)
		}
		for topic, listings := range topicListings {
			var timestamp string
			var messageCount, byteCount uint64
			minObservedTime := int64(math.MaxInt64)
			maxObservedTime := int64(0)
			for _, listing := range listings {
				if listing.Root.Timestamp > timestamp {
					timestamp = listing.Root.Timestamp
				}
				for _, child := range listing.Children {
					if child == nil {
						continue
					}
					for _, stats := range child.Statistics {
						messageCount += uint64(stats.MessageCount)
						byteCount += uint64(stats.BytesUncompressed)
						if stats.MinObservedTime < minObservedTime {
							minObservedTime = stats.MinObservedTime
						}
						if stats.MaxObservedTime > maxObservedTime {
							maxObservedTime = stats.MaxObservedTime
						}
					}
				}
			}
			data = append(data, []string{
				topic,
				timestamp,
				strconv.FormatUint(messageCount, 10),
				util.HumanBytes(byteCount),
				time.Unix(0, minObservedTime).Format(time.RFC3339),
				time.Unix(0, maxObservedTime).Format(time.RFC3339),
			})
		}
		cutil.PrintTable(w, headers, data)
		return nil
	case topic != "":
		headers := []string{
			"Topic",
			"Producer",
			"Version",
			"Timestamp",
			"Message count",
			"Bytes uncompressed",
			"Min observed time",
			"Max observed time",
		}
		data := [][]string{}
		for _, table := range response {
			var messageCount uint64
			var byteCount uint64
			minObservedTime := int64(math.MaxInt64)
			maxObservedTime := int64(0)
			for _, child := range table.Children {
				if child == nil {
					continue
				}
				for _, stats := range child.Statistics {
					messageCount += uint64(stats.MessageCount)
					byteCount += uint64(stats.BytesUncompressed)
					if stats.MinObservedTime < minObservedTime {
						minObservedTime = stats.MinObservedTime
					}
					if stats.MaxObservedTime > maxObservedTime {
						maxObservedTime = stats.MaxObservedTime
					}
				}
			}
			data = append(data, []string{
				table.Root.Topic,
				table.Root.Producer,
				strconv.FormatUint(table.Root.Version, 10),
				table.Root.Timestamp,
				strconv.FormatUint(messageCount, 10),
				util.HumanBytes(byteCount),
				time.Unix(0, minObservedTime).Format(time.RFC3339),
				time.Unix(0, maxObservedTime).Format(time.RFC3339),
			})
		}
		cutil.PrintTable(w, headers, data)
		return nil
	}

	return nil
}

func handleTables(database string, line string) error {
	parts := strings.Split(line, " ")[1:]

	switch len(parts) {
	case 0:
		// all topics, all producers
		return printTables(os.Stdout, database, "", "")
	case 1:
		topic := parts[0]
		return printTables(os.Stdout, database, "", topic)
	case 2:
		topic, producer := parts[0], parts[1]
		return printTables(os.Stdout, database, producer, topic)
	default:
		return errors.New("too many arguments")
	}
}

func printExecContext(ec *util.Context) {
	buf := &bytes.Buffer{}
	queue := []util.Pair[int, *util.Context]{util.NewPair(0, ec.Children[0])}
	for len(queue) > 0 {
		pair := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		indent, ctx := pair.First, pair.Second

		elapsedToFirstTuple := ctx.Values["elapsed_to_first_tuple"]
		delete(ctx.Values, "elapsed_to_first_tuple")
		elapsedToLastTuple := ctx.Values["elapsed_to_last_tuple"]
		delete(ctx.Values, "elapsed_to_last_tuple")
		tuplesOut := ctx.Values["tuples_out"]
		delete(ctx.Values, "tuples_out")
		bytesOut := ctx.Values["bytes_out"]
		delete(ctx.Values, "bytes_out")

		var labels string
		if len(ctx.Data) > 0 {
			labels += "["
			for i, key := range util.Okeys(ctx.Data) {
				if i > 0 {
					labels += " "
				}
				val := ctx.Data[key]
				labels += fmt.Sprintf("%s=%v", key, val)
			}
			labels += "] "
		}

		caser := cases.Title(language.English)

		timingInfo := fmt.Sprintf("(elapsed=%d...%d, rows=%d, total=%s, width=%s)",
			int(elapsedToFirstTuple),
			int(elapsedToLastTuple),
			int(tuplesOut),
			util.HumanBytes(uint64(bytesOut)),
			util.HumanBytes(uint64(float64(bytesOut)/float64(tuplesOut))))

		fmt.Fprintf(buf, "%s%s %s%s\n", strings.Repeat("  ", indent),
			caser.String(ctx.Name), labels, timingInfo)

		var statline string
		if _, ok := ctx.Values["inner_nodes_filtered"]; ok {
			statline = fmt.Sprintf(
				"Statistics: inner_filtered=%d inner_scanned=%d leaf_filtered=%d leaf_scanned=%d",
				int(ctx.Values["inner_nodes_filtered"]),
				int(ctx.Values["inner_nodes_scanned"]),
				int(ctx.Values["leaf_nodes_filtered"]),
				int(ctx.Values["leaf_nodes_scanned"]),
			)

		}
		if statline != "" {
			fmt.Fprintf(buf, "%s%s\n", strings.Repeat("  ", indent+1), statline)
		}
		for _, child := range ctx.Children {
			queue = append(queue, util.NewPair(indent+2, child))
		}
	}

	lines := strings.Split(buf.String(), "\n")
	maxWidth := 0
	for _, line := range lines {
		if len(line) > maxWidth {
			maxWidth = len(line)
		}
	}
	header := "QUERY PLAN"
	center := (maxWidth - len(header)) / 2
	headerline := fmt.Sprintf("%s%s", strings.Repeat(" ", center), header)
	fmt.Println(headerline)
	fmt.Println(strings.Repeat("-", maxWidth))
	fmt.Println(buf.String())
	for _, k := range util.Okeys(ec.Data) {
		fmt.Println(k + ":" + ec.Data[k])
	}
	for _, k := range util.Okeys(ec.Values) {
		fmt.Printf("%s: %d\n", k, int(ec.Values[k]))
	}
	if len(ec.Data) > 0 || len(ec.Values) > 0 {
		fmt.Println()
	}
}

// NB: editing the text in here can be very prone to hard to spot alignment bugs
// for code examples and list items. Justify all text to the left margin, and
// indent list items with two spaces.
var help = map[string]string{
	"": `The dp3 client is an interactive interpreter for dp3.  dp3 is a
multimodal log database for low-latency playback and analytics.

The client supports interaction via either queries or dot commands. The
supported dot commands are:
  .h [topic] to print help text. If topic is blank, prints this text.
  .connect [database] to connect to a database
  .statrange to run a statrange query
  .import to import data to the database
  .delete to delete data from the database
  .truncate to truncate data from the database
  .tables to inspect tables available in the database

Available help topics are:
  query: Show examples of query syntax.
  statrange: Explain the .statrange command.
  import: Explain the .import command.
  delete: Explain the .delete command.
  truncate: Explain the .truncate command.
  tables: Explain the .tables command.

Any input aside from "help" that does not start with a dot is interpreted as
a query. Queries are terminated with a semicolon.`,

	// query
	"query": `dp3 uses a SQL-like query language geared toward merging, as-of
joins, and heterogeneous resultsets. Queries are scoped to a single producer
(e.g a device, a simulation run) and whatever topics that producer uses.
Queries can span multiple lines and are terminated with a semicolon.

Supposing a producer called my-robot with various standard ROS topics, some
example queries are:

Read all messages on a single topic:
  from my-robot /tf;

Read all messages from a time-ordered merge of multiple topics:
  from my-robot /tf, /imu;

Filtering with a where clause
  from my-robot /fix where /fix.header.frame_id = "/imu";

Read /diagnostics and /fix messages where /diagnostics precedes /fix by less than one second
  from my-robot /diagnostics precedes /fix by less than 1 seconds where /fix.header.frame_id = "/imu";

Paging with limit and offset
  from my-robot /diagnostics limit 10 offset 5;

Results are always ordered on log time.`,

	// statrange
	"statrange": `The .statrange command is used to summarize field-level
statistics for a producer and topic at a chosen level of granularity.

The syntax is:
  .statrange producer topic granularity start end

For example,
  .statrange my-robot /diagnostics 60 2024-01-01 2024-01-02

Producer and topic are required, and if start and end are supplied
granularity must be as well.

Granularity is in seconds. The minimum and default value is 60. The user's
requested granularity is advisory: the server may return a more granular
summarization than requested.

Start and end are ISO8601 timestamps. If they are unsupplied, the full 
available range will be summarized.`,

	// import
	"import": `The .import command is used to import data into dp3. The syntax is:
  .import producer file

Multiple files can be imported using filepath globbing, for example:
  .import my-robot /path/to/data/**/*.mcap

If done this way, the import will be spread over cpucount/2 workers.

The supplied file must be in mcap format and for now, messages must be
serialized with ros1msg encoding. Such a file is obtainable by converting a
ros1 bag file with the mcap CLI tool.

Imports are staged through a write ahead log prior to landing in final
storage.  After the import completes it will take a few seconds for the
final WAL writes to get to storage. If a shutdown occurs during this time
the data will be picked up again on startup.`,

	// truncate
	"truncate": `The .truncate command is used to truncate data from dp3. The syntax
is:
  .truncate producer topic timestamp

where timestamp is an ISO-8601 timestamp or "now". The command will return
immediately (on flush of the truncation to the WAL). There will be a delay of
a few seconds before the WAL is flushed to storage and the effects of the
truncate are visible.
  `,

	// delete
	"delete": `The .delete command is used to delete data from dp3. The syntax
is:
  .delete producer topic start end

where start and end are ISO-8601 timestamps. The command will return
immediately (on flush of the deletion to the WAL). There will be a delay of
a few seconds before the WAL is flushed to storage and the effects of the
delete are visible.
  `,

	// tables
	"tables": `The .tables command is used to inspect tables available in the database.
It can be called in three ways:

  1. .tables
  2. .tables topic
  3. .tables topic producer

With no arguments, it will show a listing of all available topics, with
count/size statistics aggregated across producers.

When supplied a topic, it will give a listing for that topic for each
producer.

When supplied both a topic and a producer, it will list all historical tree
versions for that producer/topic.`,
}

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "dp3 interactive client",
	Run: func(cmd *cobra.Command, args []string) {
		if err := run(); err != nil {
			fmt.Println("error running client:", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(clientCmd)

	clientCmd.Flags().BoolVar(&clientNoPager, "no-pager", false, "Disable the pager")
}
