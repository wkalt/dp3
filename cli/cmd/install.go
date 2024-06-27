package cmd

import (
	"path"

	"github.com/spf13/cobra"
	"github.com/wkalt/dp3/cli/util"
)

var installCmd = &cobra.Command{
	Use:   "install [name] [filepath]",
	Short: "Install a plugin from a location on disk.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			bailf(cmd.UsageString())
		}
		pluginName := args[0]
		pluginLocation := args[1]
		pluginDir := path.Join(configDir(), "plugins", pluginName)
		checkErr(util.EnsureDirectoryExists(pluginDir))
		_, filename := path.Split(pluginLocation)
		checkErr(util.Copy(pluginLocation, path.Join(pluginDir, filename)))
	},
}

func init() {
	pluginCmd.AddCommand(installCmd)
}
