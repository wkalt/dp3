package testutils

import (
	"fmt"
	"net"
)

/*
General purpose test utilitites.
*/

////////////////////////////////////////////////////////////////////////////////

// GetOpenPort returns an open port that can be used for testing.
func GetOpenPort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, fmt.Errorf("failed to get open port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
