package tui

import tea "github.com/charmbracelet/bubbletea"

type postRunStage int

const (
	postRunAskClean postRunStage = iota // "Clean up now? [y/n]"
	postRunAskRemoveRepo                // "Also remove repo? [y/n]" — only reached if the first was "y"
	postRunFinished                     // reached if the first was "n" — nothing left to do
)

func (m Model) updatePostRunCleanScreen(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}

	switch m.postRunStage {
	case postRunAskClean:
		switch key.String() {
		case "y":
			m.postRunStage = postRunAskRemoveRepo
		case "n":
			m.postRunStage = postRunFinished
		}
	case postRunAskRemoveRepo:
		switch key.String() {
		case "y":
			m.removeRepo = true
			m.actionChoice = actionClean
			m.screen = screenExecution
			return m.startExecution()
		case "n":
			m.removeRepo = false
			m.actionChoice = actionClean
			m.screen = screenExecution
			return m.startExecution()
		}
	}
	return m, nil
}

func (m Model) viewPostRunCleanScreen() string {
	switch m.postRunStage {
	case postRunAskClean:
		return "Clean up now? [y/n]\n"
	case postRunAskRemoveRepo:
		return "Also remove repo? [y/n]\n"
	default:
		return "Done. ctrl+c to quit\n"
	}
}
