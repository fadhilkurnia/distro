package tui

import (
	"context"
	"fmt"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)

// screen identifies which of the fixed, forward-only screens is active.
type screen int

const (
	screenEnv screen = iota
	screenPicker
	screenAction
	screenCleanConfirm // only reached when Action = Clean
	screenExecution
	screenPostRunClean // only reached after a successful Run
)

// Model is the single root Bubble Tea model. All screen state lives here
// (rather than one model per screen) since navigation is a simple linear
// state machine and screens share context (ctx/cfg/pool) throughout.
type Model struct {
	ctx  context.Context
	cfg  *config.Config
	pool *runner.Pool

	screen screen
	width  int
	height int

	// Populated once at startup; read-only after that.
	instances []registry.Instance

	// Env screen state
	outputFilenameInput  textinput.Model
	chosenOutputFilename string // finalized once the env screen is left

	// Picker screen state
	cursor   int    // which row is currently highlighted
	selected []bool // parallel to instances; true = checked

	// Action screen state.
	actionChoice actionChoice // Run or Clean; defaults to Run

	// Clean-confirm screen state.
	removeRepo bool // whether to also remove the shared git clone

	// Execution screen state.
	execCh        chan tea.Msg // background goroutine streams log lines/completion into this
	execLog       []string
	execDone      bool
	execSucceeded int
	execFailed    int

	// Post-run-clean screen state.
	postRunStage postRunStage // which of the two questions is active; zero value = first question

	// Filled in by later steps, as each screen's real behavior is added.
}

// NewModel constructs the initial model, starting at the env screen.
func NewModel(ctx context.Context, cfg *config.Config, pool *runner.Pool) Model {
	ti := textinput.New()
	ti.Placeholder = cfg.OutputFile
	ti.Focus()
	ti.CharLimit = 128
	ti.Width = 40
 
	instances := registry.GetInstances()
 
	return Model{
		ctx:                 ctx,
		cfg:                 cfg,
		pool:                pool,
		screen:              screenEnv,
		instances:           instances,
		outputFilenameInput: ti,
		selected:            make([]bool, len(instances)),
	}
}

func (m Model) Init() tea.Cmd {
	return textinput.Blink
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "ctrl+c" {
		return m, tea.Quit
	}

	if size, ok := msg.(tea.WindowSizeMsg); ok {
		m.width = size.Width
		m.height = size.Height
		return m, nil
	}
 
	switch m.screen {
	case screenEnv:
		return m.updateEnvScreen(msg)
	case screenPicker:
		return m.updatePickerScreen(msg)
	case screenAction:
		return m.updateActionScreen(msg)
	case screenCleanConfirm:
		return m.updateCleanConfirmScreen(msg)
	case screenExecution:
		return m.updateExecutionScreen(msg)
	case screenPostRunClean:
		return m.updatePostRunCleanScreen(msg)
	}
	return m, nil
}

func (m Model) View() string {
	var content string
	switch m.screen {
	case screenEnv:
		content = m.viewEnvScreen()
	case screenPicker:
		content = m.viewPickerScreen()
	case screenAction:
		content = m.viewActionScreen()
	case screenCleanConfirm:
		content = m.viewCleanConfirmScreen()
	case screenExecution:
		content = m.viewExecutionScreen()
	case screenPostRunClean:
		content = m.viewPostRunCleanScreen()
	default:
		content = fmt.Sprintf("unknown screen: %d", m.screen)
	}
	return renderCentered(content, m.width, m.height)
}
