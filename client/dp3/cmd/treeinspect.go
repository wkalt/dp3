package cmd

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/mcap"
	"github.com/wkalt/dp3/nodestore"
	"github.com/wkalt/dp3/util"
)

var (
	treeInspectOnly      bool
	treeInspectShowLinks bool
)

var colors = []*color.Color{
	color.New(color.FgRed),
	color.New(color.FgBlue),
	color.New(color.FgYellow),
	color.New(color.FgCyan),
	color.New(color.FgGreen),
	color.New(color.FgMagenta),
	color.New(color.FgWhite),
	color.New(color.FgHiRed),
	color.New(color.FgHiBlue),
	color.New(color.FgHiYellow),
	color.New(color.FgHiCyan),
	color.New(color.FgHiGreen),
	color.New(color.FgHiMagenta),
	color.New(color.FgHiWhite),
}

func getColor(s string) *color.Color {
	num, _ := strconv.Atoi(s)
	return colors[num%len(colors)]
}

func getNode(prefix string, nodeID nodestore.NodeID) (nodestore.Node, error) {
	f, err := os.Open(path.Join(prefix, nodeID.OID()))
	if err != nil {
		return nil, fmt.Errorf("failed to open node: %w", err)
	}
	defer f.Close()
	_, err = f.Seek(int64(nodeID.Offset()), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to node: %w", err)
	}
	buf := make([]byte, nodeID.Length())
	_, err = io.ReadFull(f, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read node: %w", err)
	}
	node, err := nodestore.BytesToNode(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node: %w", err)
	}
	return node, nil
}

func leafSummary(leaf *nodestore.LeafNode) (string, error) {
	data := leaf.Data()
	reader, err := mcap.NewReader(data)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	info, err := reader.Info()
	if err != nil {
		return "", err
	}
	start := time.Unix(0, int64(info.Statistics.MessageStartTime)).Format("2006-01-02 15:04:05")
	end := time.Unix(0, int64(info.Statistics.MessageEndTime)).Format("2006-01-02 15:04:05")
	s := fmt.Sprintf(
		"[%s %s] %s, %d messages", start, end, util.HumanBytes(leaf.Size()), info.Statistics.MessageCount,
	)
	return s, nil
}

func printTree(prefix string, rootID nodestore.NodeID, only bool) error {
	nodeIDs := []nodestore.NodeID{rootID}
	indents := []int{0}
	for len(nodeIDs) > 0 {
		nodeID := nodeIDs[len(nodeIDs)-1]
		nodeIDs = nodeIDs[:len(nodeIDs)-1]

		indent := indents[len(indents)-1]
		indents = indents[:len(indents)-1]

		node, err := getNode(prefix, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}
		c := getColor(nodeID.OID())
		switch node := node.(type) {
		case *nodestore.InnerNode:
			start := time.Unix(int64(node.Start), 0).Format("2006-01-02 15:04:05")
			end := time.Unix(int64(node.End), 0).Format("2006-01-02 15:04:05")
			space := strings.Repeat(" ", indent)
			str := fmt.Sprintf("%s%s [%s %s]", space, nodeID.OID(), start, end)
			c.Println(str)
			for _, child := range node.Children {
				if child == nil {
					continue
				}
				if only && child.ID.OID() != nodeID.OID() {
					c := getColor(child.ID.OID())
					if treeInspectShowLinks {
						c.Println(space, "->")
					}
					continue
				}
				nodeIDs = append(nodeIDs, child.ID)
				indents = append(indents, indent+2)
			}
		case *nodestore.LeafNode:
			sb := &strings.Builder{}
			space := strings.Repeat(" ", indent)
			leafstr, err := leafSummary(node)
			if err != nil {
				return fmt.Errorf("failed to get leaf summary: %w", err)
			}
			c.Fprintf(sb, "%s%s %s", space, nodeID.OID(), leafstr)
			anc := node
			for anc.HasAncestor() {
				ancestorID := anc.Ancestor()
				n, err := getNode(prefix, ancestorID)
				if err != nil {
					return fmt.Errorf("failed to get ancestor %s: %w %v", ancestorID, err, anc.HasAncestor())
				}

				anc = n.(*nodestore.LeafNode)
				c := getColor(ancestorID.OID())
				s, err := leafSummary(anc)
				if err != nil {
					return fmt.Errorf("failed to get ancestor summary: %w", err)
				}
				c.Fprintf(sb, " -> %s %s", ancestorID.OID(), s)
			}
			fmt.Println(sb.String())
		}
	}
	return nil
}

var treeinspectCmd = &cobra.Command{
	Use:   "treeinspect [file]",
	Short: "Inspect the structure of a tree file.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Println(cmd.Usage())
			return
		}
		path := args[0]

		dir, _ := filepath.Split(path)

		f, err := os.Open(path)
		if err != nil {
			fmt.Printf("failed to open file: %v\n", err)
			return
		}
		defer f.Close()

		_, err = f.Seek(-24, io.SeekEnd)
		if err != nil {
			log.Printf("failed to seek to end of file: %s", err)
			return
		}
		var rootID nodestore.NodeID
		_, err = io.ReadFull(f, rootID[:])
		if err != nil {
			fmt.Printf("failed to read root ID: %s", err)
			return
		}

		err = printTree(dir, rootID, treeInspectOnly)
		if err != nil {
			fmt.Printf("failed to print tree: %v\n", err)
			return
		}
	},
}

func init() {
	rootCmd.AddCommand(treeinspectCmd)

	treeinspectCmd.Flags().BoolVarP(&treeInspectOnly, "only", "o", false, "Only print nodes from the latest tree version")
	treeinspectCmd.Flags().BoolVarP(&treeInspectShowLinks, "show-links", "l", false, "Show links between nodes")
}
