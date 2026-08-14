package tui

import (
	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
)

type runModel struct{}

func (m runModel) Init() tea.Cmd 				{ return nil }
func (m runModel) Update(msg tea.Msg) (runModel, tea.Cmd) 	{ return m, nil }
func (m runModel) View() string                           	{ return "run screen (placeholder)" }
func (m runModel) KeyBindings() []key.Binding              	{ return nil }

func (m runModel) HidesTabNav() bool {
	return false
}

func (m runModel) HidesArrowNav() bool {
	return false
}

func (m runModel) HidesLetterNav() bool {
	return false
}
