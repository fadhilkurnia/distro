package tui

import (
	"fmt"
	"os"
	"strings"
	"time"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
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
		case ".":
			if m.active == 3 {
				m.run = m.run.ToggleDebug()
				return m, nil
			}
		case "pgup":
			if m.active == 3 {
				m.run = m.run.ScrollUp()
				return m, nil
			}
		case "pgdown":
			if m.active == 3 {
				m.run = m.run.ScrollDown()
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
 
	s := m.styles
	const topMargin = 2
 
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
	}
 
	bodyWidth := naturalTotal
	contentBodyHeight := 0
	for _, v := range views {
		if w := lipgloss.Width(v); w > bodyWidth {
			bodyWidth = w
		}
		if h := lipgloss.Height(v); h > contentBodyHeight {
			contentBodyHeight = h
		}
	}
 
	// buildRow's tab-width padding and the help text below both depend
	// only on bodyWidth (already fixed above) and the active tab's
	// static key bindings — never on the body's own height. That means
	// they can be built once and reused both for measuring the
	// terminal-derived height ceiling below and for the real final
	// render, rather than needing the real body first.
	buildRow := func(renderedBodyForWidth string) string {
		targetWidth := lipgloss.Width(renderedBodyForWidth)
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
		return lipgloss.JoinHorizontal(lipgloss.Top, renderedTabs...)
	}
 
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
	helpText := m.help.FullHelpView([][]key.Binding{bindings})
 
	assemble := func(renderedBody string) string {
		content := strings.Builder{}
		content.WriteString(buildRow(renderedBody))
		content.WriteString("\n")
		content.WriteString(renderedBody)
		content.WriteString("\n\n" + helpText)
		return s.doc.Render(content.String())
	}
 
	// Determine the terminal-derived ceiling for the body's height:
	// total rows available minus every row consumed by things that
	// aren't the body. Walking the screen top to bottom: s.doc adds 1
	// row of padding above its content and 1 below (Padding(1,2,1,2));
	// s.body adds 1 row of padding above the body and 1 below
	// (Padding(1)), plus a single border line at the bottom (its top
	// border is turned off elsewhere so it visually joins the tab row
	// above it); the tab row itself is always exactly 1 line; and one
	// blank line separates the body from the help text below it. Help
	// text's own height isn't a constant — it varies by which tab is
	// active, since each tab has a different list of key bindings — so
	// it's measured directly from the string already built above rather
	// than folded into the fixed count.
	const (
		docVerticalPadding  = 2 // s.doc: 1 top + 1 bottom
		bodyVerticalPadding = 2 // s.body: 1 top + 1 bottom
		bodyBottomBorder    = 1 // s.body: only the bottom border survives UnsetBorderTop()
		tabRowHeight        = 1
		separatorBeforeHelp = 1
	)
	fixedChromeHeight := docVerticalPadding + bodyVerticalPadding + bodyBottomBorder + tabRowHeight + separatorBeforeHelp
 
	// maxBodyHeight is purely terminal-derived once a real size is
	// known — content height (contentBodyHeight, from the other three
	// tabs) no longer factors in. Run is free to be as tall as the
	// terminal allows; anything past that scrolls via pgup/pgdown
	// instead of being capped to match the other tabs' natural height.
	maxBodyHeight := contentBodyHeight // fallback only, before the first real WindowSizeMsg
	budget := -1                       // sentinel: stays -1 until a real WindowSizeMsg has arrived
	if m.height > 0 {
		budget = (m.height - topMargin) - fixedChromeHeight - lipgloss.Height(helpText)
		if budget < 1 {
			budget = 1
		}
		maxBodyHeight = budget
	}
	debugLogDimensions(m.width, m.height, bodyWidth, contentBodyHeight, fixedChromeHeight, lipgloss.Height(helpText), budget, maxBodyHeight)
 
	// Run is appended last, using bodyWidth and the (possibly
	// terminal-clamped) height ceiling derived above. Its own content
	// never grows the shared box — it wraps and scrolls within
	// whatever size is available instead, and shrinks below the
	// ceiling when its log is shorter than the budget.
	views = append(views, m.run.View(bodyWidth, maxBodyHeight))
	activeView := views[m.active]
 
	renderedBody := s.body.Width(bodyWidth + 4).Render(activeView)
	rendered := assemble(renderedBody)
 
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

// debugLogDimensions appends every computed layout number to a local log
// file, purely for manual inspection against a real terminal. Remove
// this function and its one call site in View() once you're done
// checking the numbers.
func debugLogDimensions(mWidth, mHeight, bodyWidth, contentBodyHeight, fixedChromeHeight, helpTextHeight, budget, maxBodyHeight int) {
	f, err := os.OpenFile("tui_debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return // best-effort — a logging failure shouldn't affect rendering
	}
	defer f.Close()
	fmt.Fprintf(f, "%s m.width=%d m.height=%d bodyWidth=%d contentBodyHeight=%d fixedChromeHeight=%d helpTextHeight=%d budget=%d maxBodyHeight=%d\n",
		time.Now().Format("15:04:05.000"), mWidth, mHeight, bodyWidth, contentBodyHeight, fixedChromeHeight, helpTextHeight, budget, maxBodyHeight)
}
