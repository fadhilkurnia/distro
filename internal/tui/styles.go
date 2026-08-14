package tui

import (
	"charm.land/bubbles/v2/table"
	"charm.land/lipgloss/v2"
)

var (
	purpleLight = lipgloss.Color("#874BFD")
	purpleDark = lipgloss.Color("#7D56F4")
	lightGray = lipgloss.Color("#585858")
	white = lipgloss.Color("#FFFFFF")
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

	return s
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
