package tui

import (
	"charm.land/bubbles/v2/table"
	"charm.land/lipgloss/v2"

	"github.com/fadhilkurnia/distro/internal/launcher"
)

var (
	purpleLight = lipgloss.Color("#874BFD")
	purpleDark = lipgloss.Color("#7D56F4")
	lightGray = lipgloss.Color("#585858")
	white = lipgloss.Color("#FFFFFF")

	// Progress log level
	redLight    = lipgloss.Color("#D14343")
	redDark     = lipgloss.Color("#FF6B6B")
	yellowLight = lipgloss.Color("#A67C00")
	yellowDark  = lipgloss.Color("#F1C40F")
	blueLight   = lipgloss.Color("#2563EB")
	blueDark    = lipgloss.Color("#60A5FA")
)

type styles struct {
	doc         	lipgloss.Style
	inactiveTab 	lipgloss.Style
	activeTab   	lipgloss.Style
	body      	lipgloss.Style

	// configsModel
	table       	table.Styles
	tableFrame  	lipgloss.Style
	kvLabel    	lipgloss.Style
	kvValue    	lipgloss.Style

	// instancesModel
	selectedRow 	lipgloss.Style
	listHeader  	lipgloss.Style

	// runModel log levels
	logError   lipgloss.Style
	logWarning lipgloss.Style
	logInfo    lipgloss.Style
	logDebug   lipgloss.Style
}

func tabBorderWithBottom(left, middle, right string) lipgloss.Border {
	border := lipgloss.RoundedBorder()
	border.BottomLeft = left
	border.Bottom = middle
	border.BottomRight = right
	return border
}

func newStyles(bgIsDark bool) *styles {
	lightDark := lipgloss.LightDark(bgIsDark)

	inactiveTabBorder := tabBorderWithBottom("┴", "─", "┴")
	activeTabBorder := tabBorderWithBottom("┘", " ", "└")
	highlightColor := lightDark(purpleLight, purpleDark)

	s := new(styles)
	s.doc = lipgloss.NewStyle().
		Padding(1, 2, 1, 2)
	s.inactiveTab = lipgloss.NewStyle().
		Border(inactiveTabBorder, true).
		BorderForeground(highlightColor).
		Padding(0, 1)
	s.activeTab = s.inactiveTab.
		Border(activeTabBorder, true)
	s.body = lipgloss.NewStyle().
		BorderForeground(highlightColor).
		Align(lipgloss.Left).
		Border(lipgloss.NormalBorder()).
		Padding(1).
		UnsetBorderTop()

	s.table = table.DefaultStyles()
	s.table.Header = s.table.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lightGray).
		BorderBottom(true).
		Bold(false)
	s.table.Selected = lipgloss.NewStyle()
	s.tableFrame = lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lightGray)
	s.kvLabel = lipgloss.NewStyle().Padding(0, 1)
	s.kvValue = lipgloss.NewStyle().Padding(0, 1)

	s.selectedRow = lipgloss.NewStyle().
		Background(highlightColor).
		Foreground(white).
		Bold(true)
	s.listHeader = lipgloss.NewStyle().Bold(true)

	s.logError = lipgloss.NewStyle().Foreground(lightDark(redLight, redDark)).Bold(true)
	s.logWarning = lipgloss.NewStyle().Foreground(lightDark(yellowLight, yellowDark))
	s.logInfo = lipgloss.NewStyle().Foreground(lightDark(blueLight, blueDark))
	s.logDebug = lipgloss.NewStyle().Foreground(lightGray)

	return s
}

// logStyle returns the style to render a log line's "[LEVEL]" tag in,
// based on its severity.
func (s *styles) logStyle(level launcher.Level) lipgloss.Style {
	switch level {
	case launcher.LevelError:
		return s.logError
	case launcher.LevelWarning:
		return s.logWarning
	case launcher.LevelDebug:
		return s.logDebug
	default:
		return s.logInfo
	}
}

// renderKeyValue renders a bordered label|value table, sized to fit its
// content, with no truncation on either column.
func renderKeyValue(rows [][2]string, s *styles) string {
	labelWidth := 0
	valueWidth := 0
	for _, r := range rows {
		if l := len(r[0]) + columnPadding; l > labelWidth {
			labelWidth = l
		}
		if l := len(r[1]) + columnPadding; l > valueWidth {
			valueWidth = l
		}
	}

	divider := lipgloss.NewStyle().Foreground(lightGray).Render("│")

	var lines []string
	for _, r := range rows {
		label := s.kvLabel.Width(labelWidth).Render(r[0])
		value := s.kvValue.Width(valueWidth).Render(r[1])
		lines = append(lines, lipgloss.JoinHorizontal(lipgloss.Top, label, divider, value))
	}

	body := lipgloss.JoinVertical(lipgloss.Left, lines...)
	return s.tableFrame.Render(body)
}
