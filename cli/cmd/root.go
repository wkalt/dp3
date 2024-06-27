package cmd

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"plugin"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/cli/client"
)

var (
	serverURL string
	sharedKey string
)

var httpc = client.NewHTTPClient(sharedKey)

var rootCmd = &cobra.Command{
	Use:   "dp3",
	Short: "dp3 client and server",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func bailf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func checkErr(err error) {
	if err != nil {
		bailf("error: %v", err)
	}
}

func configDir() string {
	home, err := os.UserHomeDir()
	checkErr(err)
	return path.Join(home, ".dp3")
}

func loadPlugins() {
	confdir := configDir()
	plugindir := filepath.Join(confdir, "plugins")
	// if the directory doesn't exist, there's nothing to load.
	if _, err := os.Stat(plugindir); os.IsNotExist(err) {
		return
	}

	checkErr(filepath.WalkDir(plugindir, func(path string, info os.DirEntry, err error) error {
		checkErr(err)
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(info.Name()) != ".so" {
			return nil
		}
		plug, err := plugin.Open(path)
		checkErr(err)

		sym, err := plug.Lookup("PluginCmd")
		checkErr(err)

		cmd, ok := sym.(**cobra.Command)
		if !ok {
			bailf("plugin %s does not export a *cobra.Command: %T", path, sym)
		}
		rootCmd.AddCommand(*cmd)
		return nil
	}))

}

func init() {
	loadPlugins()
	rootCmd.PersistentFlags().StringVarP(&serverURL, "server-url", "", "http://localhost:8089", "server-url")
	rootCmd.PersistentFlags().StringVarP(&sharedKey, "shared-key", "", "", "shared key to use for authentication")
}
