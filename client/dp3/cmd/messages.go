package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"

	"github.com/relvacode/iso8601"
	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/routes"
)

var (
	messagesProducerID string
	messagesEndDate    string
	messagesStartDate  string
	messagesTopics     []string
)

// messagesCmd represents the messages command
var messagesCmd = &cobra.Command{
	Use:   "messages",
	Short: "Query dp3 for messages",
	Run: func(cmd *cobra.Command, args []string) {
		start, err := iso8601.Parse([]byte(messagesStartDate))
		if err != nil {
			panic(err)
		}
		end, err := iso8601.Parse([]byte(messagesEndDate))
		if err != nil {
			panic(err)
		}
		messageRequest := &routes.MessagesRequest{
			Start:      uint64(start.UnixNano()),
			End:        uint64(end.UnixNano()),
			ProducerID: messagesProducerID,
			Topics:     messagesTopics,
		}
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(messageRequest); err != nil {
			panic(err)
		}
		resp, err := http.Post("http://localhost:8089/messages", "application/json", buf)
		if err != nil {
			panic(err)
		}
		_, err = os.Stdout.ReadFrom(resp.Body)
		if err != nil {
			panic(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(messagesCmd)

	messagesCmd.PersistentFlags().StringVarP(&messagesProducerID, "producer", "p", "", "Producer ID")
	messagesCmd.PersistentFlags().StringVarP(&messagesStartDate, "start", "s", "", "Start date")
	messagesCmd.PersistentFlags().StringVarP(&messagesEndDate, "end", "e", "", "End date")
	messagesCmd.PersistentFlags().StringArrayVarP(&messagesTopics, "topics", "t", []string{}, "Topics to query")

	messagesCmd.MarkFlagRequired("producer")
	messagesCmd.MarkFlagRequired("start")
	messagesCmd.MarkFlagRequired("end")
}
