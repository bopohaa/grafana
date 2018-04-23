package commands

import (
	"errors"
	"fmt"

	"github.com/bopohaa/grafana/pkg/cmd/grafana-cli/logger"
	m "github.com/bopohaa/grafana/pkg/cmd/grafana-cli/models"
	s "github.com/bopohaa/grafana/pkg/cmd/grafana-cli/services"
	"github.com/fatih/color"
)

var ls_getPlugins func(path string) []m.InstalledPlugin = s.GetLocalPlugins

var validateLsCommand = func(pluginDir string) error {
	if pluginDir == "" {
		return errors.New("missing path flag")
	}

	logger.Debug("plugindir: " + pluginDir + "\n")
	pluginDirInfo, err := s.IoHelper.Stat(pluginDir)

	if err != nil {
		return fmt.Errorf("error: %s", err)
	}

	if pluginDirInfo.IsDir() == false {
		return errors.New("plugin path is not a directory")
	}

	return nil
}

func lsCommand(c CommandLine) error {
	pluginDir := c.PluginDirectory()
	if err := validateLsCommand(pluginDir); err != nil {
		return err
	}

	plugins := ls_getPlugins(pluginDir)

	if len(plugins) > 0 {
		logger.Info("installed plugins:\n")
	}

	for _, plugin := range plugins {
		logger.Infof("%s %s %s \n", plugin.Id, color.YellowString("@"), plugin.Info.Version)
	}

	return nil
}
