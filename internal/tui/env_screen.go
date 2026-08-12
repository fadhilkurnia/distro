package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

func (m Model) updateEnvScreen(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		m.chosenOutputFilename = m.outputFilenameInput.Value()
		if m.chosenOutputFilename == "" {
			m.chosenOutputFilename = m.cfg.OutputFile
		}
		m.screen = screenPicker
		return m, nil
	}

	var cmd tea.Cmd
	m.outputFilenameInput, cmd = m.outputFilenameInput.Update(msg)
	return m, cmd
}

func (m Model) viewEnvScreen() string {
	var b strings.Builder

	fmt.Fprintf(&b, "Loaded configuration:\n\n")
	fmt.Fprintf(&b, "  Nodes (%d):\n", len(m.cfg.Nodes))
	for _, n := range m.cfg.Nodes {
		fmt.Fprintf(&b, "    %-8s public=%-16s private=%-16s local=%v\n", n.ID, n.PublicIP, n.PrivateIP, n.Local)
	}
	fmt.Fprintf(&b, "  SSH user:   %s\n", m.cfg.SSH.Username)
	fmt.Fprintf(&b, "  SSH key:    %s\n", m.cfg.SSH.KeyPath)
	fmt.Fprintf(&b, "  Client:     public=%s private=%s\n", m.cfg.Client.PublicIP, m.cfg.Client.PrivateIP)

	fmt.Fprintf(&b, "\nOutput filename (leave empty to use %q):\n", m.cfg.OutputFile)
	fmt.Fprintf(&b, "  %s\n", m.outputFilenameInput.View())

	fmt.Fprintf(&b, "\npress enter to continue, ctrl+c to quit\n")
	return b.String()
}
