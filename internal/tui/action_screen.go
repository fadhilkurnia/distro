package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/fadhilkurnia/distro/internal/gitrepo"
)

type actionChoice int

const (
	actionRunOnly actionChoice = iota
	actionRunWithLatency
	actionClean
)

func (a actionChoice) label() string {
	switch a {
	case actionRunOnly:
		return " Run (lifecycle only) "
	case actionRunWithLatency:
		return " Run + Latency Benchmark "
	default:
		return " Clean "
	}
}

func (m Model) updateActionScreen(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}

	switch key.String() {
	case "tab":
		m.actionChoice = (m.actionChoice + 1) % 3
	case "enter":
		if m.actionChoice == actionClean {
			m.screen = screenCleanConfirm
			return m, nil
		}
		m.screen = screenExecution
		return m.startExecution()
	}
	return m, nil
}

func (m Model) viewActionScreen() string {
	var b strings.Builder

	fmt.Fprintf(&b, "Selected (%d):\n", m.selectedCount())

	var wProject, wProtocol, wLang, wConsistency, wPersistency, wVersion, wHash int
	for i, inst := range m.instances {
		if !m.selected[i] {
			continue
		}
		wProject = max(wProject, len(inst.ProjectName))
		wProtocol = max(wProtocol, len(inst.Specification.Protocol))
		wLang = max(wLang, len(inst.Specification.Language))
		wConsistency = max(wConsistency, len(inst.Specification.Consistency))
		wPersistency = max(wPersistency, len(inst.Specification.Persistency))
		wVersion = max(wVersion, len(inst.Version.Name))
		wHash = max(wHash, len(gitrepo.ShortHash(inst.Version.CommitHash)))
	}

	for i, inst := range m.instances {
		if !m.selected[i] {
			continue
		}
		fmt.Fprintf(&b, "  %-*s / %-*s / %-*s / %-*s / %-*s / %-*s (%*s)\n",
			wProject, inst.ProjectName,
			wProtocol, inst.Specification.Protocol,
			wLang, inst.Specification.Language,
			wConsistency, inst.Specification.Consistency,
			wPersistency, inst.Specification.Persistency,
			wVersion, inst.Version.Name,
			wHash, gitrepo.ShortHash(inst.Version.CommitHash),
		)
	}

	fmt.Fprintf(&b, "\nTarget nodes (from .env, %d):\n", len(m.cfg.Nodes))
	for _, n := range m.cfg.Nodes {
		fmt.Fprintf(&b, "  %s — %s\n", n.ID, n.PublicIP)
	}

	fmt.Fprintf(&b, "\n")
	for _, choice := range []actionChoice{actionRunOnly, actionRunWithLatency, actionClean} {
		label := choice.label()
		if choice == m.actionChoice {
			label = highlightStyle.Render(label)
		}
		fmt.Fprintf(&b, "%s ", label)
	}
	fmt.Fprintf(&b, "\ntab to switch · enter to confirm · ctrl+c to quit\n")

	return b.String()
}

// selectedCount returns how many instances are currently checked.
func (m Model) selectedCount() int {
	n := 0
	for _, s := range m.selected {
		if s {
			n++
		}
	}
	return n
}
