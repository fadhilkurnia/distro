package tui

import (
	"context"
	"fmt"
	"os"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/runner"

	tea "charm.land/bubbletea/v2"
	"charm.land/bubbles/v2/help"
)

// launches the TUI and blocks until the user exits it.
func Run(ctx context.Context, cfg *config.Config, pool *runner.Pool) error {
	tabs := []string{"Configs", "Instances", "Benchmarks", "Run"}
	appStyles := newStyles(true)

	m := rootModel{
		tabTitles: 	tabs,
		styles:    	appStyles,
		help:      	help.New(),
		configs: 	newConfigsModel(cfg.Nodes, cfg.Client, cfg.SSH, cfg.OutputFile, appStyles),
		instances: 	newInstancesModel(appStyles),
		benchmarks: 	newBenchmarksModel(cfg.Nodes, cfg.WarmupDuration, cfg.Duration, cfg.WriteRatio, appStyles),
		run:        	newRunModel(ctx, pool, cfg.Nodes, cfg.Client, appStyles),
	}
	p := tea.NewProgram(m)
	if _, err := p.Run(); err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}

	return nil
}
