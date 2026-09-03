package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/fadhilkurnia/distro/internal/benchmarkhistory"
	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)

const maxLogLines = 500

type runStatus int

const (
	runIdle runStatus = iota
	runRunning
)

// logLine is one leveled progress or completion message, ready to render.
type logLine struct {
    level launcher.Level
    name  string // ex: "ailidani.paxi/paxos (master)"
    msg   string // ex: "(If not exists) Cloning ... repository..."
}

type runLogMsg logLine
type runFinishedMsg struct{}

type runModel struct {
	pool    *runner.Pool
	nodes   []config.Node
	sshCfg  config.SSHConfig
	baseCtx context.Context
	styles  *styles
 
	status     runStatus
	log        []logLine
	progressCh chan logLine
	cancel     context.CancelFunc
	showDebug  bool

	scrollOffset int
}

func newRunModel(ctx context.Context, pool *runner.Pool, nodes []config.Node, sshCfg config.SSHConfig, s *styles) runModel {
	return runModel{
		pool:    pool,
		nodes:   nodes,
		sshCfg:  sshCfg,
		baseCtx: ctx,
		styles:  s,
		status:  runIdle,
	}
}
 
func (m runModel) Init() tea.Cmd { return nil }
func (m runModel) Idle() bool    { return m.status == runIdle }
func (m runModel) Running() bool { return m.status == runRunning }

// Start kicks off the Build->Start->RunLatencyBenchmark->Stop sequence for
// every selected instance, sequentially, on a background goroutine. Progress
// lines and completion are streamed back via listenForProgress.
func (m runModel) Start(selected []registry.Instance, params launcher.LatencyParams, outputFilename string) (runModel, tea.Cmd) {
	if !m.Idle() || len(selected) == 0 {
		return m, nil
	}
 
	ctx, cancel := context.WithCancel(m.baseCtx)
	ch := make(chan logLine, 100)
 
	m.status = runRunning
	m.log = nil
	m.progressCh = ch
	m.cancel = cancel
	m.scrollOffset = 0
 
	if err := os.MkdirAll("benchmark", 0755); err != nil {
		ch <- logLine{level: launcher.LevelWarning, msg: fmt.Sprintf("could not create benchmark/ directory: %v", err)}
	}
	manifestPath := filepath.Join("benchmark", outputFilename)
 
	go runAllInstances(ctx, m.pool, m.nodes, m.sshCfg, selected, params, manifestPath, ch)
 
	return m, listenForProgress(ch)
}
 
func (m runModel) Cancel() (runModel, tea.Cmd) {
	if m.Running() && m.cancel != nil {
		m.cancel()
	}
	return m, nil
}
 
func (m runModel) Reset() runModel {
	m.status = runIdle
	m.log = nil
	m.progressCh = nil
	m.cancel = nil
	return m
}
 
func listenForProgress(ch <-chan logLine) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-ch
		if !ok {
			return runFinishedMsg{}
		}
		return runLogMsg(line)
	}
}
 
func runAllInstances(ctx context.Context, pool *runner.Pool, nodes []config.Node, sshCfg config.SSHConfig,
	selected []registry.Instance, params launcher.LatencyParams, manifestPath string, ch chan<- logLine) {
	defer close(ch)
 
	for _, inst := range selected {
		if ctx.Err() != nil {
			ch <- logLine{level: launcher.LevelWarning, msg: "run cancelled"}
			return
		}
 
		name := fmt.Sprintf("%s/%s (%s)", inst.ProjectName, inst.Specification.Protocol, inst.Version.Name)
		progress := launcher.NewProgress(func(level launcher.Level, msg string) {
			ch <- logLine{level: level, name: name, msg: msg}
		})
 
		l, err := registry.GetLauncher(inst)
		if err != nil {
			ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("registry error: %v", err)}
			continue
		}
 
		if err := l.Build(ctx, pool, nodes, sshCfg, progress); err != nil {
			ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("build failed: %v", err)}
			continue
		}
 
		startErr := l.Start(ctx, pool, nodes, progress)
		if startErr != nil {
			ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("start failed: %v (cleaning up)", startErr)}
		}

		if startErr == nil {
			resultPath, err := l.RunLatencyBenchmark(ctx, pool, nodes, params, progress)
			if err != nil {
				ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("benchmark failed: %v", err)}
			} else {
				entry := benchmarkhistory.LatencyBenchmarkEntry{
					Project:     inst.ProjectName,
					Version:     inst.Version.Name,
					CommitHash:  inst.Version.CommitHash,
					Protocol:    inst.Specification.Protocol,
					Language:    inst.Specification.Language,
					Consistency: inst.Specification.Consistency,
					Persistency: inst.Specification.Persistency,
					ResultPath:  resultPath,
					Timestamp:   time.Now(),
				}
				if err := benchmarkhistory.AppendLatency(manifestPath, entry); err != nil {
					ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("manifest write failed: %v", err)}
				}
			}
		}
 
		// Stop must run even if the overall ctx was cancelled — otherwise
		// cancelling a run would abandon nodes mid-process instead of
		// actually stopping them.
		stopCtx := context.Background()
		if err := l.Stop(stopCtx, pool, nodes, progress); err != nil {
			ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("stop failed: %v", err)}
		}
	}
 
	ch <- logLine{level: launcher.LevelInfo, msg: "=== run complete ==="}
}
 
func (m runModel) Update(msg tea.Msg) (runModel, tea.Cmd) {
	switch msg := msg.(type) {
	case runLogMsg:
		m.log = append(m.log, logLine(msg))
		if len(m.log) > maxLogLines {
			m.log = m.log[len(m.log)-maxLogLines:]
		}
		return m, listenForProgress(m.progressCh)
	case runFinishedMsg:
		m.status = runIdle
		return m, nil
	}
	return m, nil
}

func (m runModel) View(width, height int) string {
	var status string
	switch m.status {
	case runIdle:
		status = "Press enter to start the benchmark run."
	case runRunning:
		status = "Running..."
	}
 
	// Expand every visible log entry into its wrapped physical lines,
	// with continuation lines hanging-indented under where the message
	// text starts, not under the [LEVEL] tag.
	var lines []string
	for _, line := range m.log {
		if line.level == launcher.LevelDebug && !m.showDebug {
			continue
		}
 
		tag := "[" + line.level.String() + "]"
		if m.styles != nil {
			tag = m.styles.logStyle(line.level).Render(tag)
		}
		prefix := tag + " "
		if line.name != "" {
			prefix = tag + " [" + line.name + "] "
		}
		prefixWidth := lipgloss.Width(prefix)
 
		wrapWidth := width - prefixWidth
		if wrapWidth < 1 {
			wrapWidth = 1
		}
		wrapped := lipgloss.Wrap(line.msg, wrapWidth, "")
		for i, seg := range strings.Split(wrapped, "\n") {
			if i == 0 {
				lines = append(lines, prefix+seg)
			} else {
				lines = append(lines, strings.Repeat(" ", prefixWidth)+seg)
			}
		}
	}
 
	// Height budget for the log window: total height minus the status
	// line and the blank separator line above the log block.
	logHeight := height - lipgloss.Height(status) - 1
	if logHeight < 0 {
		logHeight = 0
	}
 
	// Windowing: scrollOffset is "how many lines back from the live
	// bottom". Clamp scrollOffset itself against the actual line count
	// on every render — past a certain point, further scrolling can't
	// reveal anything new, so it pins to the topmost lines rather than
	// (incorrectly) sliding past them into an empty view.
	maxOffset := len(lines) - logHeight
	if maxOffset < 0 {
		maxOffset = 0
	}
	offset := m.scrollOffset
	if offset > maxOffset {
		offset = maxOffset
	}
	end := len(lines) - offset
	if end > len(lines) {
		end = len(lines)
	}
	if end < 0 {
		end = 0
	}
	start := end - logHeight
	if start < 0 {
		start = 0
	}
	visible := lines[start:end]
 
	body := status
	if len(visible) > 0 {
		body += "\n\n" + strings.Join(visible, "\n")
	}
	return body
}
 
func (m runModel) ScrollUp() runModel {
	m.scrollOffset++
	return m
}
 
func (m runModel) ScrollDown() runModel {
	m.scrollOffset--
	if m.scrollOffset < 0 {
		m.scrollOffset = 0
	}
	return m
}

func (m runModel) HidesTabNav() bool {
	return false
}

func (m runModel) HidesArrowNav() bool {
	return false
}

func (m runModel) HidesLetterNav() bool {
	return m.Running()
}

func (m runModel) KeyBindings() []key.Binding {
	return []key.Binding{
		key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "start run")),
		key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "stop run")),
		key.NewBinding(key.WithKeys("c"), key.WithHelp("c", "clean state")),
		key.NewBinding(key.WithKeys("C"), key.WithHelp("C", "clean state (delete repo)")),
		key.NewBinding(key.WithKeys("ctrl+l"), key.WithHelp("ctrl+l", "clear logs")),
		key.NewBinding(key.WithKeys("."), key.WithHelp(".", "toggle debug logs")),
	}
}

func (m runModel) Clean(selected []registry.Instance, removeRepo bool) (runModel, tea.Cmd) {
	if m.Running() || len(selected) == 0 {
		return m, nil
	}

	ctx, cancel := context.WithCancel(m.baseCtx)
	ch := make(chan logLine, 100)

	m.status = runRunning
	m.log = nil
	m.progressCh = ch
	m.cancel = cancel

	go cleanAllInstances(ctx, m.pool, m.nodes, selected, removeRepo, ch)

	return m, listenForProgress(ch)
}

func cleanAllInstances(
	ctx context.Context, pool *runner.Pool, nodes []config.Node,
	selected []registry.Instance, removeRepo bool, ch chan<- logLine,
) {
	defer close(ch)

	for _, inst := range selected {
		if ctx.Err() != nil {
			ch <- logLine{level: launcher.LevelWarning, msg: "clean cancelled"}
			return
		}

		name := fmt.Sprintf("%s/%s (%s)", inst.ProjectName, inst.Specification.Protocol, inst.Version.Name)
		progress := launcher.NewProgress(func(level launcher.Level, msg string) {
			ch <- logLine{level: level, name: name, msg: msg}
		})

		l, err := registry.GetLauncher(inst)
		if err != nil {
			ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("registry error: %v", err)}
			continue
		}

		if err := l.Clean(ctx, pool, nodes, removeRepo, progress); err != nil {
			ch <- logLine{level: launcher.LevelError, name: name, msg: fmt.Sprintf("clean failed: %v", err)}
			continue
		}
	}

	ch <- logLine{level: launcher.LevelInfo, msg: "=== clean complete ==="}
}


func (m runModel) ClearLog() runModel {
	m.log = nil
	return m
}

func (m runModel) ToggleDebug() runModel {
	m.showDebug = !m.showDebug
	return m
}
