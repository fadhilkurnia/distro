package tui

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	"charm.land/lipgloss/v2"
)

var (
	keyQuit = key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q", "quit"),
	)
	keyQuitEditing = key.NewBinding(
		key.WithKeys("ctrl+c"),
		key.WithHelp("ctrl+c", "quit"),
	)
	keyTabSwitch = key.NewBinding(
		key.WithKeys("left", "right", "h", "l", "p", "n"),
		key.WithHelp("←/→", "prev/next"),
	)
	keyTabArrowOnly = key.NewBinding(
		key.WithKeys("left", "right"),
		key.WithHelp("←/→", "prev/next"),
	)
)

type rootModel struct {
	tabTitles 	[]string
	active    	int
	styles    	*styles
	help      	help.Model
	width, height 	int

	configs    configsModel
	instances  instancesModel
	benchmarks benchmarksModel
	run        runModel
}


func (m rootModel) Init() tea.Cmd {
	return tea.Batch(
		m.configs.Init(),
		m.instances.Init(),
		m.benchmarks.Init(),
		m.run.Init(),
	)
}

// activeHidesTabNav reports whether the currently active sub-model wants
// left/right released for its own use instead of tab-switching.
func (m rootModel) activeHidesTabNav() bool {
	switch m.active {
	case 0:
		return m.configs.HidesTabNav()
	case 1:
		return m.instances.HidesTabNav()
	case 2:
		return m.benchmarks.HidesTabNav()
	case 3:
		return m.run.HidesTabNav()
	}
	return false
}

// activeKeyBindings returns the currently active sub-model's own key hints.
func (m rootModel) activeKeyBindings() []key.Binding {
	switch m.active {
	case 0:
		return m.configs.KeyBindings()
	case 1:
		return m.instances.KeyBindings()
	case 2:
		return m.benchmarks.KeyBindings()
	case 3:
		return m.run.KeyBindings()
	}
	return nil
}


func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg.(type) {
	case runLogMsg, runFinishedMsg:
		var cmd tea.Cmd
		m.run, cmd = m.run.Update(msg)
		return m, cmd
	}

	hideArrows := m.activeHidesArrowNav()
	hideLetters := m.activeHidesLetterNav()

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyPressMsg:
		switch keypress := msg.String(); keypress {
		case "ctrl+c":
			if m.run.Running() && m.run.cancel != nil {
				m.run.cancel()
			}
			return m, tea.Quit
		case "q":
			if !hideLetters {
				return m, tea.Quit
			}
		case "right":
			if !hideArrows {
				m.active = min(m.active+1, len(m.tabTitles)-1)
				return m, nil
			}
		case "left":
			if !hideArrows {
				m.active = max(m.active-1, 0)
				return m, nil
			}
		case "l", "n":
			if !hideLetters {
				m.active = min(m.active+1, len(m.tabTitles)-1)
				return m, nil
			}
		case "h", "p":
			if !hideLetters {
				m.active = max(m.active-1, 0)
				return m, nil
			}
		case "enter":
			if m.active == 3 && m.run.Idle() {
				allLocked := m.configs.Locked() && m.instances.Locked() && m.benchmarks.LatencyLocked()
				hasSelection := len(m.instances.SelectedInstances()) > 0
				if allLocked && hasSelection {
					var cmd tea.Cmd
					m.run, cmd = m.run.Start(
						m.instances.SelectedInstances(),
						m.benchmarks.LatencyParams(),
						m.configs.OutputFilename(),
					)
					return m, cmd
				}
			}
		case "esc":
			if m.run.Running() {
				if m.active == 3 {
					var cmd tea.Cmd
					m.run, cmd = m.run.Cancel()
					return m, cmd
				}
				return m, nil // swallow esc on other tabs while running
			}
		case "c":
			if m.active == 3 && !m.run.Running() {
				selected := m.instances.SelectedInstances()
				if len(selected) > 0 {
					var cmd tea.Cmd
					m.run, cmd = m.run.Clean(selected, false)
					return m, cmd
				}
			}
		case "C":
			if m.active == 3 && !m.run.Running() {
				selected := m.instances.SelectedInstances()
				if len(selected) > 0 {
					var cmd tea.Cmd
					m.run, cmd = m.run.Clean(selected, true)
					return m, cmd
				}
			}
		case "ctrl+l":
			if m.active == 3 {
				m.run = m.run.ClearLog()
				return m, nil
			}
			case ".": // toggle DEBUG log in run_model
			if m.active == 3 {
				m.run = m.run.ToggleDebug()
				return m, nil
			}
		}
	}

	var cmd tea.Cmd
	switch m.active {
	case 0:
		m.configs, cmd = m.configs.Update(msg)
	case 1:
		m.instances, cmd = m.instances.Update(msg)
	case 2:
		m.benchmarks, cmd = m.benchmarks.Update(msg)
	case 3:
		m.run, cmd = m.run.Update(msg)
	}
	return m, cmd
}

func (m rootModel) View() tea.View {
	if m.styles == nil {
		return tea.NewView("")
	}

	content := strings.Builder{}
	s := m.styles

	type tabInfo struct {
		title string
		style lipgloss.Style
	}

	var tabs []tabInfo
	naturalTotal := 0

	for i, t := range m.tabTitles {
		var style lipgloss.Style
		isFirst, isLast, isActive := i == 0, i == len(m.tabTitles)-1, i == m.active
		if isActive {
			style = s.activeTab
		} else {
			style = s.inactiveTab
		}
		border, _, _, _, _ := style.GetBorder()
		if isFirst && isActive {
			border.BottomLeft = "│"
		} else if isFirst && !isActive {
			border.BottomLeft = "├"
		} else if isLast && isActive {
			border.BottomRight = "│"
		} else if isLast && !isActive {
			border.BottomRight = "┤"
		}
		style = style.Border(border)

		tabs = append(tabs, tabInfo{title: t, style: style})
		naturalTotal += lipgloss.Width(style.Render(t))
	}

	views := []string{
		m.configs.View(),
		m.instances.View(),
		m.benchmarks.View(),
		m.run.View(),
	}
	activeView := views[m.active]

	// after
	// bodyWidth is driven only by Configs/Instances/Benchmarks
	// run_model's content will be wrapped down to fit that width 
	// by s.body.Render() instead of changing the bodyWidth
	bodyWidth := naturalTotal
	for _, v := range views[:3] {
		if w := lipgloss.Width(v); w > bodyWidth {
			bodyWidth = w
		}
	}

	renderedBody := s.body.Width(bodyWidth + 4).Render(activeView)
	targetWidth := lipgloss.Width(renderedBody)

	deficit := targetWidth - naturalTotal
	if deficit < 0 {
		deficit = 0
	}
	share := deficit / len(tabs)
	remainder := deficit % len(tabs)

	var renderedTabs []string
	for i, ti := range tabs {
		extra := share
		if i < remainder {
			extra++
		}
		title := ti.title
		if extra > 0 {
			title += strings.Repeat(" ", extra)
		}
		renderedTabs = append(renderedTabs, ti.style.Render(title))
	}
	row := lipgloss.JoinHorizontal(lipgloss.Top, renderedTabs...)

	content.WriteString(row)
	content.WriteString("\n")
	content.WriteString(renderedBody)

	hideArrows := m.activeHidesArrowNav()
	hideLetters := m.activeHidesLetterNav()

	var bindings []key.Binding
	switch {
	case !hideArrows && !hideLetters:
		bindings = append(bindings, keyTabSwitch)
	default:
		bindings = append(bindings, keyTabArrowOnly)
	}
	bindings = append(bindings, m.activeKeyBindings()...)
	if hideLetters {
		bindings = append(bindings, keyQuitEditing)
	} else {
		bindings = append(bindings, keyQuit)
	}
	m.help.SetWidth(bodyWidth + 4)
	content.WriteString("\n\n" + m.help.FullHelpView([][]key.Binding{bindings}))

	const topMargin = 2

	rendered := s.doc.Render(content.String())
	if m.width > 0 && m.height > 0 {
		rendered = lipgloss.Place(m.width, m.height-topMargin, lipgloss.Center, lipgloss.Top, rendered)
		rendered = strings.Repeat("\n", topMargin) + rendered
	}

	v := tea.NewView(rendered)
	v.AltScreen = true
	return v
}

func (m rootModel) activeHidesArrowNav() bool {
	switch m.active {
	case 0:
		return m.configs.HidesArrowNav()
	case 1:
		return m.instances.HidesArrowNav()
	case 2:
		return m.benchmarks.HidesArrowNav()
	case 3:
		return m.run.HidesArrowNav()
	}
	return false
}

func (m rootModel) activeHidesLetterNav() bool {
	switch m.active {
	case 0:
		return m.configs.HidesLetterNav()
	case 1:
		return m.instances.HidesLetterNav()
	case 2:
		return m.benchmarks.HidesLetterNav()
	case 3:
		return m.run.HidesLetterNav()
	}
	return false
}
