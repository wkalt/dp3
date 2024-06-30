package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/server/tree"
	"github.com/wkalt/dp3/server/wal"
)

var (
	walInspectFile string
	walInspectID   string
)

func listWALFile(filename string) {
	f, err := os.Open(filename)
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
}

func printWALInsertID(ctx context.Context, id string) error {
	wmgr, err := wal.NewWALManager(ctx, "waldir", nil)
	if err != nil {
		return fmt.Errorf("failed to create WAL manager: %w", err)
	}

	parts := strings.Split(id, ":")
	if len(parts) != 3 {
		return fmt.Errorf("invalid WAL insert ID")
	}

	object, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse object: %w", err)
	}
	offset, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse offset: %w", err)
	}
	length, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse length: %w", err)
	}

	addr := wal.NewAddress(uint64(object), uint64(offset), uint64(length))

	_, tr, err := wmgr.GetReader(addr)
	if err != nil {
		return fmt.Errorf("failed to get WAL reader: %w", err)
	}

	s, err := tree.Print(ctx, tr)
	if err != nil {
		return fmt.Errorf("failed to print tree: %w", err)
	}
	fmt.Println(s)

	return nil
}

// walinspectCmd represents the walinspect command
var walinspectCmd = &cobra.Command{
	Use: "walinspect --file [file] | less",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if walInspectFile != "" {
			listWALFile(walInspectFile)
			return
		}

		if walInspectID != "" {
			if err := printWALInsertID(ctx, walInspectID); err != nil {
				bailf("failed to print WAL insert ID: %v", err)
			}
			return
		}

		bailf("must specify --file or --id")
	},
}

func init() {
	rootCmd.AddCommand(walinspectCmd)

	walinspectCmd.Flags().StringVarP(&walInspectFile, "file", "", "", "WAL file to inspect")
	walinspectCmd.Flags().StringVarP(&walInspectID, "id", "", "", "WAL insert ID to inspect")
}
