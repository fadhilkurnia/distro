package tui

import (
	"fmt"
	"log"
	"strings"

	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/fadhilkurnia/distro/internal/gitrepo"
	"github.com/fadhilkurnia/distro/internal/registry"
)

type instanceRow struct {
	instance registry.Instance
	cells    [6]string
}

type instancesModel struct {
	rows     []instanceRow
	cursor   int
	selected map[int]bool
	locked   bool
	styles   *styles
}

func newInstancesModel(s *styles) instancesModel {
	instances := registry.GetInstances()
	rows := make([]instanceRow, 0, len(instances))
	for _, inst := range instances {
		rows = append(rows, instanceRow{
			instance: inst,
			cells: [6]string{
				inst.ProjectName,
				inst.Specification.Protocol,
				inst.Specification.Language,
				inst.Specification.Consistency,
				inst.Specification.Persistency,
				fmt.Sprintf("%s (%s)", inst.Version.Name, gitrepo.ShortHash(inst.Version.CommitHash)),
			},
		})
	}

	if f, err := tea.LogToFile("debug.log", "instances"); err == nil {
		sample := instancesModel{rows: rows, selected: make(map[int]bool), styles: s}
		log.Printf("instances view width = %d", lipgloss.Width(sample.View()))
		f.Close()
	}

	return instancesModel{
		rows:     rows,
		selected: make(map[int]bool),
		styles:   s,
	}
}

func (m instancesModel) Init() tea.Cmd {
	return nil
}

func (m instancesModel) Update(msg tea.Msg) (instancesModel, tea.Cmd) {
	keyMsg, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return m, nil
	}

	switch keyMsg.String() {
	case "up":
		if !m.locked && len(m.rows) > 0 {
			if m.cursor > 0 {
				m.cursor--
			} else {
				m.cursor = len(m.rows) - 1
			}
		}
	case "down":
		if !m.locked && len(m.rows) > 0 {
			if m.cursor < len(m.rows)-1 {
				m.cursor++
			} else {
				m.cursor = 0
			}
		}
	case "space":
		if !m.locked && len(m.rows) > 0 {
			if m.selected[m.cursor] {
				delete(m.selected, m.cursor)
			} else {
				m.selected[m.cursor] = true
			}
			if m.cursor < len(m.rows)-1 {
				m.cursor++
			} else {
				m.cursor = 0
			}
		}
	case "enter":
		if !m.locked && len(m.selected) > 0 {
			m.locked = true
		}
	case "esc":
		if m.locked {
			m.locked = false
		}
	}
	return m, nil
}

var instanceHeaders = []string{"Project", "Protocol", "Language", "Consistency", "Persistency", "Version"}

func instanceColumnWidths(rows []instanceRow) [6]int {
	var widths [6]int
	for i, h := range instanceHeaders {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i, v := range r.cells {
			if l := len(v); l > widths[i] {
				widths[i] = l
			}
		}
	}
	for i := range widths {
		widths[i] += columnPadding
	}
	return widths
}

func (m instancesModel) renderRow(i int, widths [6]int, showCursor, highlight bool) string {
	r := m.rows[i]
	prefix := "  "
	if showCursor && i == m.cursor {
		prefix = "› "
	}
	cells := make([]string, len(r.cells))
	for j, v := range r.cells {
		clamped := lipgloss.NewStyle().Width(widths[j]).Render(v)
		cells[j] = m.styles.table.Cell.Render(clamped)
	}
	row := prefix + strings.Join(cells, "")
	if highlight && m.selected[i] {
		return m.styles.selectedRow.Render(row)
	}
	return row
}

func (m instancesModel) View() string {
	widths := instanceColumnWidths(m.rows)

	headerCells := make([]string, len(instanceHeaders))
	for i, h := range instanceHeaders {
		clamped := lipgloss.NewStyle().Width(widths[i]).Render(h)
		headerCells[i] = m.styles.table.Header.Render(clamped)
	}

	var b strings.Builder
	b.WriteString("  " + lipgloss.JoinHorizontal(lipgloss.Top, headerCells...) + "\n")
	for _, i := range m.visibleIndices() {
		b.WriteString(m.renderRow(i, widths, !m.locked, !m.locked) + "\n")
	}

	framed := m.styles.tableFrame.Render(strings.TrimRight(b.String(), "\n"))
	return "Select Instances to Run:\n\n" + framed + fmt.Sprintf("\n\nInstances Selected: %d\n", len(m.selected))
}

func (m instancesModel) KeyBindings() []key.Binding {
	if m.locked {
		return []key.Binding{
			key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "edit selection")),
		}
	}
	return []key.Binding{
		key.NewBinding(key.WithKeys("up", "down"), key.WithHelp("↑/↓", "up/down")),
		key.NewBinding(key.WithKeys("space"), key.WithHelp("space", "select")),
		key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "confirm")),
	}
}

func (m instancesModel) HidesTabNav() bool {
	return false
}

func (m instancesModel) HidesArrowNav() bool {
	return false
}

func (m instancesModel) HidesLetterNav() bool {
	return false
}

// visibleIndices returns which row indices should render, in table order:
// every row while unlocked, only selected rows while locked.
func (m instancesModel) visibleIndices() []int {
	if !m.locked {
		idx := make([]int, len(m.rows))
		for i := range m.rows {
			idx[i] = i
		}
		return idx
	}
	idx := make([]int, 0, len(m.selected))
	for i := range m.rows {
		if m.selected[i] {
			idx = append(idx, i)
		}
	}
	return idx
}
