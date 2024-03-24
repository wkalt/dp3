package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/wal"
)

// walinspectCmd represents the walinspect command
var walinspectCmd = &cobra.Command{
	Use: "walinspect [file] | less",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			bailf("Usage: dp3 walinspect [file] | less")
		}
		f, err := os.Open(args[0])
		if err != nil {
			bailf("failed to open WAL file: %v", err)
		}
		defer f.Close()
		reader, err := wal.NewReader(f)
		if err != nil {
			bailf("failed to create WAL reader: %v", err)
		}
		for {
			offset := reader.Offset()
			rectype, record, err := reader.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				bailf("failed to read record: %v", err)
			}
			switch rectype {
			case wal.WALInsert:
				req := wal.ParseInsertRecord(record)
				fmt.Printf("%d: insert (%s) %d bytes\n", offset, req.Topic, len(record))
			case wal.WALMergeRequest:
				req := wal.ParseMergeRequestRecord(record)
				addrs := ""
				for _, addr := range req.Addrs {
					addrs += fmt.Sprintf("%s ", addr)
				}
				fmt.Printf("%d: merge request %s (%s) addrs: %s\n", offset, req.BatchID, req.Topic, addrs)
			case wal.WALMergeComplete:
				req := wal.ParseMergeCompleteRecord(record)
				fmt.Printf("%d: merge complete %s\n", offset, req.BatchID)
			default:
				bailf("unknown record type: %v", rectype)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(walinspectCmd)
}
