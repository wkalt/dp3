package util

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/wkalt/dp3/util/httputil"
)

// MustOK will print the error message from the response and exit if the
// response is not 200.
func MustOK(resp *http.Response) {
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		response := httputil.ErrorResponse{}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			fmt.Printf("error decoding response: %s\n", err)
		} else {
			fmt.Println("ERROR: " + response.Error)
			if response.Detail != "" {
				fmt.Println("DETAIL: " + response.Detail)
			}
		}
		os.Exit(1)
	}
}
