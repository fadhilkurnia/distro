package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

func (m Model) updateCleanConfirmScreen(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok { return m, nil }

	switch key.String() {
	case "tab":
		m.removeRepo = !m.removeRepo
	case "y":
		m.screen = screenExecution
		return m.startExecution()
	case "n":
		// Declining returns to the Action screen
		m.screen = screenAction
	}
	return m, nil
}

func (m Model) viewCleanConfirmScreen() string {
	var b strings.Builder

	fmt.Fprintf(&b, "Clean %d selected instance(s).\n", m.selectedCount())
	fmt.Fprintf(&b, "This removes binaries, generated config, and logs.\n\n")

	removeRepoLabel := "Also remove repo: "
	if m.removeRepo {
		removeRepoLabel += "yes"
	} else {
		removeRepoLabel += "no"
	}
	fmt.Fprintf(&b, "%s  (tab to toggle)\n\n", removeRepoLabel)

	fmt.Fprintf(&b, "Are you sure? [y/n]\n")

	return b.String()
}
