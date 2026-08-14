package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"

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

type runLogMsg string
type runFinishedMsg struct{}

type runModel struct {
	pool   *runner.Pool
	nodes  []config.Node
	client config.Node
	baseCtx context.Context

	status     runStatus
	log        []string
	progressCh chan string
	cancel     context.CancelFunc
}

func newRunModel(ctx context.Context, pool *runner.Pool, nodes []config.Node, client config.Node) runModel {
	return runModel{
		pool:    pool,
		nodes:   nodes,
		client:  client,
		baseCtx: ctx,
		status:  runIdle,
	}
}

func (m runModel) Init() tea.Cmd {
	return nil
}

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
	ch := make(chan string, 100)

	m.status = runRunning
	m.log = nil
	m.progressCh = ch
	m.cancel = cancel

	if err := os.MkdirAll("benchmark", 0755); err != nil {
		ch <- fmt.Sprintf("warning: could not create benchmark/ directory: %v", err)
	}
	manifestPath := filepath.Join("benchmark", outputFilename)

	go runAllInstances(ctx, m.pool, m.nodes, m.client, selected, params, manifestPath, ch)

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

func listenForProgress(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-ch
		if !ok {
			return runFinishedMsg{}
		}
		return runLogMsg(line)
	}
}

func runAllInstances(ctx context.Context, pool *runner.Pool, nodes []config.Node, client config.Node,
	selected []registry.Instance, params launcher.LatencyParams, manifestPath string, ch chan<- string) {
	defer close(ch)

	for _, inst := range selected {
		if ctx.Err() != nil {
			ch <- "run cancelled"
			return
		}

		name := fmt.Sprintf("%s/%s", inst.ProjectName, inst.Specification.Protocol)
		progress := func(s string) { ch <- fmt.Sprintf("[%s] %s", name, s) }

		l, err := registry.GetLauncher(inst)
		if err != nil {
			ch <- fmt.Sprintf("[%s] registry error: %v", name, err)
			continue
		}

		if err := l.Build(ctx, pool, nodes, progress); err != nil {
			ch <- fmt.Sprintf("[%s] build failed: %v", name, err)
			continue
		}

		startErr := l.Start(ctx, pool, nodes, progress)
		if startErr != nil {
			ch <- fmt.Sprintf("[%s] start failed: %v (cleaning up)", name, startErr)
		}

		if startErr == nil {
			resultPath, err := l.RunLatencyBenchmark(ctx, pool, client, params, progress)
			if err != nil {
				ch <- fmt.Sprintf("[%s] benchmark failed: %v", name, err)
			} else {
				entry := benchmarkhistory.Entry{
					Project:       inst.ProjectName,
					Version:       inst.Version.Name,
					CommitHash:    inst.Version.CommitHash,
					Protocol:      inst.Specification.Protocol,
					Language:      inst.Specification.Language,
					Consistency:   inst.Specification.Consistency,
					Persistency:   inst.Specification.Persistency,
					BenchmarkType: "latency",
					ResultPath:    resultPath,
					Timestamp:     time.Now(),
				}
				if err := benchmarkhistory.Append(manifestPath, entry); err != nil {
					ch <- fmt.Sprintf("[%s] manifest write failed: %v", name, err)
				}
			}
		}

		// Stop must run even if the overall ctx was cancelled — otherwise
		// cancelling a run would abandon nodes mid-process instead of
		// actually stopping them.
		stopCtx := context.Background()
		if err := l.Stop(stopCtx, pool, nodes, progress); err != nil {
			ch <- fmt.Sprintf("[%s] stop failed: %v", name, err)
		}
	}

	ch <- "=== run complete ==="
}

func (m runModel) Update(msg tea.Msg) (runModel, tea.Cmd) {
	switch msg := msg.(type) {
	case runLogMsg:
		m.log = append(m.log, string(msg))
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

func (m runModel) View() string {
	var status string
	switch m.status {
	case runIdle:
		status = "Press enter to start the benchmark run."
	case runRunning:
		status = "Running..."
	}

	body := status
	if len(m.log) > 0 {
		body += "\n\n"
		for _, line := range m.log {
			body += line + "\n"
		}
	}
	return "Run\n\n" + body
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
	}
}

func (m runModel) Clean(selected []registry.Instance, removeRepo bool) (runModel, tea.Cmd) {
	if m.Running() || len(selected) == 0 {
		return m, nil
	}

	ctx, cancel := context.WithCancel(m.baseCtx)
	ch := make(chan string, 100)

	m.status = runRunning
	m.log = nil
	m.progressCh = ch
	m.cancel = cancel

	go cleanAllInstances(ctx, m.pool, m.nodes, selected, removeRepo, ch)

	return m, listenForProgress(ch)
}

func cleanAllInstances(ctx context.Context, pool *runner.Pool, nodes []config.Node,
	selected []registry.Instance, removeRepo bool, ch chan<- string) {
	defer close(ch)

	for _, inst := range selected {
		if ctx.Err() != nil {
			ch <- "clean cancelled"
			return
		}

		name := fmt.Sprintf("%s/%s", inst.ProjectName, inst.Specification.Protocol)
		progress := func(s string) { ch <- fmt.Sprintf("[%s] %s", name, s) }

		l, err := registry.GetLauncher(inst)
		if err != nil {
			ch <- fmt.Sprintf("[%s] registry error: %v", name, err)
			continue
		}

		if err := l.Clean(ctx, pool, nodes, removeRepo, progress); err != nil {
			ch <- fmt.Sprintf("[%s] clean failed: %v", name, err)
			continue
		}
	}

	ch <- "=== clean complete ==="
}

func (m runModel) ClearLog() runModel {
	m.log = nil
	return m
}
