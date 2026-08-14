package tui

import (
	"fmt"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/list"
	tea "charm.land/bubbletea/v2"

	"github.com/fadhilkurnia/distro/internal/config"
)

type benchmarkItem struct {
	title string
	desc  string
}

func (i benchmarkItem) Title() string       { return i.title }
func (i benchmarkItem) Description() string { return i.desc }
func (i benchmarkItem) FilterValue() string { return i.title }

var benchmarkItems = []benchmarkItem{
	{title: "Latency", desc: "Measure service latency of an Instance on a low load"},
	{title: "Placement Strategy", desc: "Measure service latency of an Instance on a simulated geolocation"},
	{title: "Capacity", desc: "Measure the maximum throughput (request/s) an Instance can handle"},
}

type placeholderDetailModel struct {
	name string
}

func (m placeholderDetailModel) Init() tea.Cmd { return nil }
func (m placeholderDetailModel) Update(msg tea.Msg) (placeholderDetailModel, tea.Cmd) {
	return m, nil
}
func (m placeholderDetailModel) View() string {
	return fmt.Sprintf("%s configuration (stub)\n\nDetails for this benchmark go here.", m.name)
}
func (m placeholderDetailModel) HidesArrowNav() bool        { return false }
func (m placeholderDetailModel) HidesLetterNav() bool       { return false }
func (m placeholderDetailModel) KeyBindings() []key.Binding { return nil }

type benchmarksModel struct {
	list     list.Model
	stage    int // 0 = list, 1 = detail
	selected int

	latency   latencyModel
	placement placeholderDetailModel
	capacity  placeholderDetailModel
	styles    *styles
}

func newBenchmarksModel(nodes []config.Node, warmup, duration string, writeRatio float64, s *styles) benchmarksModel {
	items := make([]list.Item, len(benchmarkItems))
	width := 0
	for i, b := range benchmarkItems {
		items[i] = b
		if l := len(b.desc); l > width {
			width = l
		}
	}
	width += columnPadding * 2

	delegate := list.NewDefaultDelegate()
	height := len(items)*(delegate.Height()+delegate.Spacing()) - delegate.Spacing()

	l := list.New(items, delegate, width, height)
	l.InfiniteScrolling = true
	l.SetFilteringEnabled(false)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)
	l.SetShowPagination(true)
	l.SetShowHelp(false)
	l.Paginator.SetTotalPages(1)
	l.Paginator.PerPage = len(items)
	l.KeyMap.Quit.SetEnabled(false)
	l.KeyMap.ForceQuit.SetEnabled(false)

	return benchmarksModel{
		list:      l,
		latency:   newLatencyModel(nodes, warmup, duration, writeRatio, s),
		placement: placeholderDetailModel{name: "Placement Strategy"},
		capacity:  placeholderDetailModel{name: "Capacity"},
		styles:    s,
	}
}

func (m benchmarksModel) Init() tea.Cmd {
	return m.latency.Init()
}

func (m benchmarksModel) Update(msg tea.Msg) (benchmarksModel, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyPressMsg); ok {
		switch keyMsg.String() {
		case "enter":
			if m.stage == 0 {
				m.selected = m.list.Index()
				m.stage = 1
				return m, nil
			}
		case "esc":
			if m.stage == 1 && !m.activeLocked() {
				m.stage = 0
				return m, nil
			}
		}
	}

	if m.stage == 0 {
		var cmd tea.Cmd
		m.list, cmd = m.list.Update(msg)
		return m, cmd
	}

	var cmd tea.Cmd
	switch m.selected {
	case 0:
		m.latency, cmd = m.latency.Update(msg)
	case 1:
		m.placement, cmd = m.placement.Update(msg)
	case 2:
		m.capacity, cmd = m.capacity.Update(msg)
	}
	return m, cmd
}

func (m benchmarksModel) View() string {
	if m.stage == 1 {
		switch m.selected {
		case 0:
			return m.latency.View()
		case 1:
			return m.placement.View()
		case 2:
			return m.capacity.View()
		}
	}
	return "Select Benchmarks:\n\n" + m.list.View()
}

func (m benchmarksModel) activeLocked() bool {
	switch m.selected {
	case 0:
		return m.latency.Locked()
	case 1:
		return m.placement.Locked()
	case 2:
		return m.capacity.Locked()
	}
	return false
}

func (m placeholderDetailModel) Locked() bool {
	return false
}

func (m benchmarksModel) HidesTabNav() bool {
	return false
}

func (m benchmarksModel) HidesArrowNav() bool {
	if m.stage != 1 {
		return false
	}
	switch m.selected {
	case 0:
		return m.latency.HidesArrowNav()
	case 1:
		return m.placement.HidesArrowNav()
	case 2:
		return m.capacity.HidesArrowNav()
	}
	return false
}

func (m benchmarksModel) HidesLetterNav() bool {
	if m.stage != 1 {
		return false
	}
	switch m.selected {
	case 0:
		return m.latency.HidesLetterNav()
	case 1:
		return m.placement.HidesLetterNav()
	case 2:
		return m.capacity.HidesLetterNav()
	}
	return false
}

func (m benchmarksModel) KeyBindings() []key.Binding {
	if m.stage == 1 {
		switch m.selected {
		case 0:
			return m.latency.KeyBindings()
		case 1:
			return m.placement.KeyBindings()
		case 2:
			return m.capacity.KeyBindings()
		}
	}
	return []key.Binding{
		key.NewBinding(key.WithKeys("up", "down"), key.WithHelp("↑/↓", "up/down")),
		key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "view details")),
	}
}
