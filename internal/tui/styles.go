package tui

import "github.com/charmbracelet/lipgloss"

// Color scheme. All screens reference these instead of hardcoding colors
// themselves — change a look-and-feel decision once, here, rather than
// hunting through every screen file.
var (
	colorBackground = lipgloss.Color("236") // off-screen area outside the centered box
	colorBorder     = lipgloss.Color("240") // box border
	colorMuted      = lipgloss.Color("245") // hints/footers
	colorError      = lipgloss.Color("196") // failed steps, warnings
)

// Shared styles built from the palette above.
var (
	boxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Padding(1, 2)

	// Reverse(true) swaps whatever the terminal's current fg/bg already
	// are, rather than picking explicit colors — this way the
	// highlighted row looks reasonable regardless of the user's terminal
	// theme (light or dark), instead of us guessing a color that might
	// clash. Swap this for explicit Foreground/Background colors later
	// if a specific look is wanted instead.
	highlightStyle = lipgloss.NewStyle().Reverse(true)

	mutedStyle = lipgloss.NewStyle().Foreground(colorMuted)
	errorStyle = lipgloss.NewStyle().Foreground(colorError)
)

// renderCentered wraps content in a bordered box and centers it within a
// width x height canvas, filling everything outside the box with
// colorBackground. Falls back to plain content if the terminal size isn't
// known yet (WindowSizeMsg hasn't arrived on the very first render).
func renderCentered(content string, width, height int) string {
	if width == 0 || height == 0 {
		return content
	}
	box := boxStyle.Render(content)
	return lipgloss.Place(
		width, height,
		lipgloss.Center, lipgloss.Center,
		box,
		lipgloss.WithWhitespaceBackground(colorBackground),
	)
}
