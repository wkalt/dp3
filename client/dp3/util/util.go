package util

import "os"

// StdoutRedirected returns true if stdout is redirected to a file or pipe.
func StdoutRedirected() bool {
	if fi, err := os.Stdout.Stat(); err == nil {
		return (fi.Mode() & os.ModeCharDevice) == 0
	}
	return false
}
