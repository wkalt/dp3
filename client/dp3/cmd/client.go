package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/chzyer/readline"
	"github.com/relvacode/iso8601"
	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/client/dp3/util"
	"github.com/wkalt/dp3/routes"
	"github.com/wkalt/dp3/util/httputil"
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

func withPaging(pager string, f func(io.WriteCloser) error) error {
	if pager == "" {
		return f(os.Stdout)
	}

	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("failed to make a pipe: %w", err)
	}

	stdout := os.Stdout
	os.Stdout = w

	cmd := exec.Command(pager)
	cmd.Stdin = r
	cmd.Stdout = stdout
	cmd.Stderr = os.Stderr

	done := make(chan struct{})

	go func() {
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "error running pager: "+err.Error())
		}
		done <- struct{}{}
	}()

	go f(w)

	<-done
	w.Close()
	os.Stdout = stdout
	return nil
}

func executeQuery(s string) error {
	req := &routes.QueryRequest{
		Query: s,
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return fmt.Errorf("error encoding request: %w", err)
	}
	resp, err := http.Post("http://localhost:8089/query", "application/json", buf)
	if err != nil {
		return fmt.Errorf("error calling export: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		response := &httputil.ErrorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}
		return errors.New(response.Error)
	}
	pager := maybePager()
	return withPaging(pager, func(w io.WriteCloser) error {
		defer w.Close()
		return util.MCAPToJSON(w, resp.Body)
	})
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

func printError(s string) {
	fmt.Println("ERROR: " + s)
}

func run() error {
	l, err := readline.NewEx(&readline.Config{
		Prompt:          "dp3 # ",
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
	for {
		line, err := l.Readline()
		if err != nil {
			if errors.Is(err, readline.ErrInterrupt) {
				l.SetPrompt("dp3 # ")
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		line = strings.TrimSpace(line)

		switch {
		case line == "":
			continue
		case line == "help", strings.HasPrefix(line, "\\h"):
			_, topic, _ := strings.Cut(line, " ")
			fmt.Println(help[topic])
			continue
		case strings.HasPrefix(line, "\\statrange"):
			if err := handleStatRange(line); err != nil {
				printError(err.Error())
			}
			continue
		case strings.HasPrefix(line, "\\import"):
			if err := handleImport(line); err != nil {
				printError(err.Error())
			}
			continue
		case strings.HasPrefix(line, "\\"):
			printError("recognized command: " + line)
			continue
		}

		lines = append(lines, line)
		if !strings.HasSuffix(line, ";") {
			l.SetPrompt("... # ")
			continue
		}
		query := strings.Join(lines, " ")
		lines = lines[:0]
		l.SetPrompt("dp3 # ")
		l.SaveHistory(query)
		if err := executeQuery(strings.TrimSuffix(query, ";")); err != nil {
			printError(err.Error())
		}
	}

	return nil
}

func handleImport(line string) error {
	parts := strings.Split(line, " ")[1:]
	if len(parts) < 2 {
		return errors.New("not enough arguments")
	}
	producer := parts[0]
	pattern := parts[1]
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("error globbing: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no files found matching %s", pattern)
	}
	workers := runtime.NumCPU() / 2
	return doImport(producer, paths, workers)
}

func handleStatRange(line string) error {
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
	return withPaging(pager, func(w io.WriteCloser) error {
		defer w.Close()
		return printStatRange(w, producer, topic, uint64(granularity)*1e9, starttime, endtime)
	})
}

var help = map[string]string{
	"": `The dp3 client is an interactive interpreter for dp3.  dp3 is a
multimodal log database for low-latency playback and analytics.

The client supports interaction via either queries or slash commands. The
supported slash commands are:

  \h [topic] to print help text. If topic is blank, prints this text.
  \statrange to run a statrange query
  \import to import data to the database

Available help topics are:
  query: Show examples of query syntax.
  statrange: Explain the \statrange command.
  import: Explain the \import command.

Any input aside from "help" that does not start with a backslash is interpreted
as a query. Queries are terminated with a semicolon.`,

	// query
	"query": `dp3 uses a SQL-like query language geared toward merging,
as-of joins, and heterogeneous resultsets. Queries are scoped to a single
producer (e.g a device, a simulation run) and whatever topics that producer
uses. Queries can span multiple lines and are terminated with a semicolon.

Supposing a producer called my-robot with various standard ROS topics, some
example queries are:

Read all messages on a single topic:
    from my-robot /tf;

Read all messages from a time-ordered merge of multiple topics:
	from my-robot /tf, /imu;

Filtering with a where clause
	from my-robot /fix where /fix.header.frame_id = "/imu";

Read /diagnostics and /fix messages where the /fix message is within 1 second of /diagnostics
    from my-robot /diagnostics precedes /fix by less than 1 seconds where /fix.header.frame_id = "/imu";

Paging with limit and offset
	from my-robot /diagnostics limit 10 offset 5;

Results are always ordered on log time.`,

	// statrange
	"statrange": `The \statrange command is used to summarize field-level statistics
for a producer and topic at a chosen level of granularity.

The syntax is:
  \statrange producer topic granularity start end

For example,
  \statrange my-robot /diagnostics 60 "2024-01-01" "2024-01-02"

Producer and topic are required, and if start and end are supplied granularity
must be as well.

Granularity is in seconds. The minimum and default value is 60. The user's
requested granularity is advisory: the server may return a more granular
summarization than requested.

Start and end are quoted ISO8601 timestamps.  If they are unsupplied the full
available range will be summarized.`,

	// import
	"import": `The \import command is used to import data into dp3. The syntax is:
  \import producer file

Multiple files can be imported using filepath globbing, for example:
  \import my-robot /path/to/data/**/*.mcap

If done this way, the import will be spread over cpucount/2 workers.

The supplied file must be in mcap format and for now, messages must be
serialized with ros1msg encoding. Such a file is obtainable by converting a ros1
bag file with the mcap CLI tool.

Imports are staged through a write ahead log prior to landing in final storage.
After the import completes it will take a few seconds for the final WAL writes
to get to storage. If a shutdown occurs during this time the data will be picked
up again on startup.`,
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
}
