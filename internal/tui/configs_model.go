package tui

import (
	"fmt"
	"strings"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/table"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"

	"github.com/fadhilkurnia/distro/internal/config"
)

type configsModel struct {
	table         table.Model
	sshInfo       string
	filenameInput textinput.Model
	editing       bool
	styles        *styles
}

const columnPadding = 2

func columnWidth(title string, rows []table.Row, col int) int {
	max := len(title)
	for _, r := range rows {
		if l := len(r[col]); l > max {
			max = l
		}
	}
	return max + columnPadding
}

func newConfigsModel(nodes []config.Node, ssh config.SSHConfig, outputFile string, s *styles) configsModel {
	rows := make([]table.Row, 0, len(nodes))
	for _, n := range nodes {
		rows = append(rows, table.Row{n.ID, n.PublicIP, n.PrivateIP})
	}

	machinesTitle := fmt.Sprintf("Machines (%d)", len(nodes))
	columns := []table.Column{
		{Title: machinesTitle, Width: columnWidth(machinesTitle, rows, 0)},
		{Title: "Public IP", Width: columnWidth("Public IP", rows, 1)},
		{Title: "Private IP", Width: columnWidth("Private IP", rows, 2)},
	}

	totalWidth := 0
	for _, c := range columns {
		totalWidth += c.Width + 2
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithHeight(len(rows)+1),
		table.WithWidth(totalWidth),
		table.WithStyles(s.table),
	)

	sshRows := [][2]string{
		{"SSH User", ssh.Username},
		{"SSH Key", ssh.KeyPath},
	}
	sshInfo := renderKeyValue(sshRows, s)

	ti := textinput.New()
	ti.Placeholder = outputFile
	ti.SetStyles(textinput.DefaultStyles(true))
	ti.CharLimit = 156
	ti.SetWidth(40)
	ti.Focus()

	return configsModel{
		table:         t,
		sshInfo:       sshInfo,
		filenameInput: ti,
		editing:       true,
		styles:        s,
	}
}

func (m configsModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m configsModel) Update(msg tea.Msg) (configsModel, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyPressMsg); ok {
		switch keyMsg.String() {
		case "enter":
			if m.editing {
				m.editing = false
				m.filenameInput.Blur()
			}
			return m, nil
		case "esc":
			if !m.editing {
				m.editing = true
				return m, m.filenameInput.Focus()
			}
			return m, nil
		}
	}

	if !m.editing {
		return m, nil
	}

	var cmd tea.Cmd
	m.filenameInput, cmd = m.filenameInput.Update(msg)
	return m, cmd
}

func (m configsModel) View() string {
	nodesBlock := m.styles.tableFrame.Render(m.table.View())
	label := fmt.Sprintf("Benchmark output filename (defaults to %s):", m.filenameInput.Placeholder)
	return nodesBlock + "\n" + m.sshInfo + "\n\n" + label + "\n " + m.filenameInput.View()
}

func (m configsModel) OutputFilename() string {
	name := m.filenameInput.Value()
	if name == "" {
		name = m.filenameInput.Placeholder
	}
	if !strings.HasSuffix(name, ".json") {
		name += ".json"
	}
	return name
}

func (m configsModel) KeyBindings() []key.Binding {
	if m.editing {
		return []key.Binding{
			key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "confirm")),
		}
	}
	return []key.Binding{
		key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "edit filename")),
	}
}

func (m configsModel) HidesTabNav() bool {
	return false
}

func (m configsModel) HidesArrowNav() bool {
	return false
}

func (m configsModel) HidesLetterNav() bool {
	return true
}

func (m configsModel) Locked() bool {
	return !m.editing
}
