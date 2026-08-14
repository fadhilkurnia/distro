package tui

import (
	"fmt"
	"strconv"
	"strings"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/table"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"

	"github.com/fadhilkurnia/distro/internal/config"
)

const (
	latencyFieldTotal = iota
	latencyFieldWarmup
	latencyFieldDuration
	latencyFieldWritePct
	latencyFieldCount
)

var latencyLabels = [...]string{
	"Workload Type",
	"Request Workload (Total)",
	"Warm-up Duration",
	"Benchmark Duration",
	"Read/Write Ratio",
}

type latencyModel struct {
	nodes    []config.Node
	inputs   [latencyFieldCount]textinput.Model
	defaults [latencyFieldCount]string

	focused int
	locked  bool
	styles  *styles
}

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func newLatencyModel(nodes []config.Node, warmup, duration string, writeRatio float64, s *styles) latencyModel {
	defaults := [latencyFieldCount]string{
		latencyFieldTotal:    strconv.Itoa(len(nodes)), // no existing config source; 1 req/sec per node
		latencyFieldWarmup:   digitsOnly(warmup),
		latencyFieldDuration: digitsOnly(duration),
		latencyFieldWritePct: strconv.Itoa(int(writeRatio * 100)),
	}

	var inputs [latencyFieldCount]textinput.Model
	for i := range inputs {
		ti := textinput.New()
		ti.Placeholder = defaults[i]
		ti.Prompt = ""
		ti.SetStyles(textinput.DefaultStyles(true))
		ti.SetWidth(6)
		ti.CharLimit = 5
		inputs[i] = ti
	}

	inputs[latencyFieldTotal].Focus()

	return latencyModel{
		nodes:    nodes,
		inputs:   inputs,
		defaults: defaults,
		focused:  latencyFieldTotal,
		styles:   s,
	}
}

func (m latencyModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m latencyModel) currentValue(i int) string {
	if v := m.inputs[i].Value(); v != "" {
		return v
	}
	return m.defaults[i]
}

func (m latencyModel) Update(msg tea.Msg) (latencyModel, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyPressMsg); ok {
		switch keyMsg.String() {
		case "up":
			if !m.locked && m.focused > 0 {
				m.inputs[m.focused].Blur()
				m.focused--
				cmd := m.inputs[m.focused].Focus()
				return m, cmd
			}
			return m, nil
		case "down":
			if !m.locked && m.focused < latencyFieldCount-1 {
				m.inputs[m.focused].Blur()
				m.focused++
				cmd := m.inputs[m.focused].Focus()
				return m, cmd
			}
			return m, nil
		case "enter":
			if !m.locked {
				for i := range m.inputs {
					if m.inputs[i].Value() == "" {
						m.inputs[i].SetValue(m.defaults[i])
					}
				}
				m.inputs[m.focused].Blur()
				m.locked = true
			}
			return m, nil
		case "esc":
			if m.locked {
				m.locked = false
				return m, m.inputs[m.focused].Focus()
			}
			return m, nil
		}
	}

	if m.locked {
		return m, nil
	}

	prev := m.inputs[m.focused].Value()

	var cmd tea.Cmd
	m.inputs[m.focused], cmd = m.inputs[m.focused].Update(msg)

	clean := digitsOnly(m.inputs[m.focused].Value())
	if clean != m.inputs[m.focused].Value() {
		m.inputs[m.focused].SetValue(clean)
	}

	if m.focused == latencyFieldWritePct {
		v := m.inputs[m.focused].Value()
		if v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 || n > 100 {
				m.inputs[m.focused].SetValue(prev)
			}
		}
	}

	return m, cmd
}

func (m latencyModel) perNodeWorkload() string {
	total, err := strconv.Atoi(m.currentValue(latencyFieldTotal))
	if err != nil || len(m.nodes) == 0 {
		return "—"
	}
	per := float64(total) / float64(len(m.nodes))
	s := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", per), "0"), ".")
	return s + " request/sec"
}

func (m latencyModel) renderNodeTable() string {
	rows := make([]table.Row, len(m.nodes))
	workload := m.perNodeWorkload()
	for i, n := range m.nodes {
		rows[i] = table.Row{n.ID, workload}
	}

	nodeTitle := fmt.Sprintf("Node (%d)", len(m.nodes))
	columns := []table.Column{
		{Title: nodeTitle, Width: columnWidth(nodeTitle, rows, 0)},
		{Title: "Request Workload", Width: columnWidth("Request Workload", rows, 1)},
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
		table.WithStyles(m.styles.table),
	)
	return m.styles.tableFrame.Render(t.View())
}

func (m latencyModel) View() string {
	labelWidth := 0
	for _, l := range latencyLabels {
		if w := len(l) + columnPadding; w > labelWidth {
			labelWidth = w
		}
	}
	label := func(s string) string {
		return m.styles.kvLabel.Width(labelWidth).Render(s)
	}

	rowPrefix := func(fieldIdx int) string {
		if !m.locked && m.focused == fieldIdx {
			return "› "
		}
		return "  "
	}

	var lines []string
	lines = append(lines, "  "+label(latencyLabels[0])+" │ Closed Loop")
	lines = append(lines, rowPrefix(latencyFieldTotal)+label(latencyLabels[1])+" │ "+m.inputs[latencyFieldTotal].View()+" request/sec")
	lines = append(lines, rowPrefix(latencyFieldWarmup)+label(latencyLabels[2])+" │ "+m.inputs[latencyFieldWarmup].View()+" second")
	lines = append(lines, rowPrefix(latencyFieldDuration)+label(latencyLabels[3])+" │ "+m.inputs[latencyFieldDuration].View()+" second")

	writeN, err := strconv.Atoi(m.currentValue(latencyFieldWritePct))
	if err != nil {
		writeN = 0
	}
	readN := 100 - writeN
	lines = append(lines, fmt.Sprintf("%s%s │ %d%% read / %s%% write",
		rowPrefix(latencyFieldWritePct), label(latencyLabels[4]), readN, m.inputs[latencyFieldWritePct].View()))

	form := strings.Join(lines, "\n")

	return "Latency Benchmark Details\n\nBenchmark Configuration:\n\n" +
		m.renderNodeTable() + "\n\n" + form
}

func (m latencyModel) Locked() bool {
	return m.locked
}

func (m latencyModel) HidesArrowNav() bool {
	return false
}

func (m latencyModel) HidesLetterNav() bool {
	return false
}

func (m latencyModel) KeyBindings() []key.Binding {
	if m.locked {
		return []key.Binding{
			key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "edit values")),
		}
	}
	return []key.Binding{
		key.NewBinding(key.WithKeys("up", "down"), key.WithHelp("↑/↓", "up/down")),
		key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "confirm")),
		key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "return to select")),
	}
}
