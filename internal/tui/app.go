package tui

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/runner"
)

// launches the TUI and blocks until the user exits it.
func Run(ctx context.Context, cfg *config.Config, pool *runner.Pool) error {
	m := NewModel(ctx, cfg, pool)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
