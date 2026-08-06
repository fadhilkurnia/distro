package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/fadhilkurnia/distro/internal/gitrepo"
)

const pickerPageSize = 5

func (m Model) updatePickerScreen(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok { return m, nil }
	n := len(m.instances)
	if n == 0 { return m, nil }

	switch key.String() {
	case "up", "k":
		m.cursor = ((m.cursor-1)%n + n) % n
	case "down", "j":
		m.cursor = (m.cursor + 1) % n
	case "pgup":
		m.cursor = ((m.cursor-pickerPageSize)%n + n) % n
	case "pgdown":
		m.cursor = (m.cursor + pickerPageSize) % n
	case " ":
		m.selected[m.cursor] = !m.selected[m.cursor]
		m.cursor = (m.cursor + 1) % n
	case "enter":
		if m.anySelected() {
			m.screen = screenAction
		}
	}
	return m, nil
}

func (m Model) anySelected() bool {
	for _, s := range m.selected {
		if s { return true }
	}
	return false
}

func (m Model) viewPickerScreen() string {
	var b strings.Builder

	n := len(m.instances)
	if n == 0 {
		return "No instances available to select.\n"
	}

	wProject, wProtocol, wLang := len("Project"), len("Protocol"), len("Language")
	wConsistency, wPersistency, wVersion := len("Consistency"), len("Persistency"), len("Version")
	var wHash int
	for _, inst := range m.instances {
		wProject = max(wProject, len(inst.ProjectName))
		wProtocol = max(wProtocol, len(inst.Specification.Protocol))
		wLang = max(wLang, len(inst.Specification.Language))
		wConsistency = max(wConsistency, len(inst.Specification.Consistency))
		wPersistency = max(wPersistency, len(inst.Specification.Persistency))
		wVersion = max(wVersion, len(inst.Version.Name))
		wHash = max(wHash, len(gitrepo.ShortHash(inst.Version.CommitHash)))
	}

	fmt.Fprintf(&b, "Select instances (space to toggle, enter to continue):\n\n")
	fmt.Fprintf(&b, "      %-*s / %-*s / %-*s / %-*s / %-*s / %-*s\n",
		wProject, "Project",
		wProtocol, "Protocol",
		wLang, "Language",
		wConsistency, "Consistency",
		wPersistency, "Persistency",
		wVersion, "Version",
	)

	// How many rows of the box are already spoken for by everything
	// that isn't an instance row: border (2) + padding (2) + intro
	// line + blank (2) + column header (1) + footer blank + hint (2).
	const pickerChromeLines = 9
	visibleRows := n
	if m.height > 0 {
		visibleRows = m.height - pickerChromeLines
		if visibleRows < 3 {
			visibleRows = 3 // degrade gracefully rather than showing nothing
		}
		if visibleRows > n {
			visibleRows = n
		}
	}

	offset := m.cursor - visibleRows/2
	if offset < 0 {
		offset = 0
	}
	if offset > n-visibleRows {
		offset = n - visibleRows
	}
	if offset < 0 {
		offset = 0
	}

	if offset > 0 {
		fmt.Fprintf(&b, "  ↑ %d more above\n", offset)
	}

	for i := offset; i < offset+visibleRows; i++ {
		inst := m.instances[i]
		cursor := " "
		if i == m.cursor {
			cursor = ">"
		}
		checked := " "
		if m.selected[i] {
			checked = "x"
		}
		line := fmt.Sprintf(
			"%s [%s] %-*s / %-*s / %-*s / %-*s / %-*s / %*s (%*s)",
			cursor, checked,
			wProject, inst.ProjectName,
			wProtocol, inst.Specification.Protocol,
			wLang, inst.Specification.Language,
			wConsistency, inst.Specification.Consistency,
			wPersistency, inst.Specification.Persistency,
			wVersion, inst.Version.Name,
			wHash, gitrepo.ShortHash(inst.Version.CommitHash),
		)
		if i == m.cursor {
			line = highlightStyle.Render(line)
		}
		fmt.Fprintln(&b, line)
	}

	if offset+visibleRows < n {
		fmt.Fprintf(&b, "  ↓ %d more below\n", n-(offset+visibleRows))
	}

	fmt.Fprintf(&b, "\nctrl+c to quit\n")
	return b.String()
}
