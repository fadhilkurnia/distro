package tui

import (
	"context"
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)

// logLineMsg is one line of progress, streamed from the background
// execution goroutine into the model via execCh.
type logLineMsg string

// execDoneMsg signals the entire sequential run (every selected instance)
// has finished.
type execDoneMsg struct {
	succeeded int
	failed    int
}

// selectedInstances returns the checked instances, in picker order.
func (m Model) selectedInstances() []registry.Instance {
	var out []registry.Instance
	for i, inst := range m.instances {
		if m.selected[i] {
			out = append(out, inst)
		}
	}
	return out
}

// startExecution creates the channel, launches the background goroutine
// that does the real work, and returns the Cmd that starts listening for
// messages from it. Called by the Action/Clean-confirm screens at the
// moment they transition into screenExecution.
func (m Model) startExecution() (Model, tea.Cmd) {
	ch := make(chan tea.Msg, 32)
	m.execCh = ch
	m.execLog = nil
	m.execDone = false
	m.execSucceeded = 0
	m.execFailed = 0

	params := launcher.LatencyParams{
		WarmupDuration: m.cfg.WarmupDuration,
		Duration:       m.cfg.Duration,
		WriteRatio:     m.cfg.WriteRatio,
	}

	go runExecution(m.ctx, m.pool, m.cfg.Nodes, m.cfg.Client, m.selectedInstances(), m.actionChoice, m.removeRepo, params, ch)

	return m, waitForExecMsg(ch)
}

// waitForExecMsg blocks on ch and returns whatever arrives next. Update
// re-issues this after every logLineMsg, which is what keeps the screen
// listening for further messages rather than only ever seeing the first
// one.
func waitForExecMsg(ch chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}

// runExecution processes every selected instance sequentially — full
// Build->Start->[latency benchmark]->Stop (or just Clean), one instance
// completely finished before the next begins. Runs on its own goroutine;
// only ever communicates back via ch, never touches the terminal directly
// (see launcher.Progress's doc comment for why).
func runExecution(
	ctx context.Context,
	pool *runner.Pool,
	nodes []config.Node,
	client config.Node,
	instances []registry.Instance,
	action actionChoice,
	removeRepo bool,
	params launcher.LatencyParams,
	ch chan tea.Msg,
) {
	succeeded, failed := 0, 0

	for _, inst := range instances {
		ch <- logLineMsg(fmt.Sprintf("Running benchmark for %s/%s/%s/%s/%s (%s)",
			inst.ProjectName,
			inst.Specification.Protocol,
			inst.Specification.Language,
			inst.Specification.Persistency,
			inst.Specification.Consistency,
			inst.Version.Name,
		))

		l, err := registry.GetLauncher(inst)
		if err != nil {
			ch <- logLineMsg(fmt.Sprintf("  ERROR: %v", err))
			failed++
			continue
		}

		progress := func(s string) { ch <- logLineMsg("  " + s) }

		if action == actionClean {
			ch <- logLineMsg("[Clean] Removing build output")
			if err := l.Clean(ctx, pool, nodes, removeRepo, progress); err != nil {
				ch <- logLineMsg(fmt.Sprintf("  CLEAN FAILED: %v", err))
				failed++
				continue
			}
			succeeded++
			continue
		}

		// action == actionRunOnly or actionRunWithLatency
		ch <- logLineMsg("[Build] Building project binary and setting up dependencies")
		if err := l.Build(ctx, pool, nodes, progress); err != nil {
			ch <- logLineMsg(fmt.Sprintf("  BUILD FAILED: %v", err))
			failed++
			continue // Start/latency/Stop never make sense without a successful Build
		}

		ch <- logLineMsg("[Start] launching on nodes")
		startErr := l.Start(ctx, pool, nodes, progress)
		if startErr != nil {
			ch <- logLineMsg(fmt.Sprintf("  START FAILED: %v", startErr))
		}

		var latencyErr error
		if startErr == nil && action == actionRunWithLatency {
			ch <- logLineMsg("[Latency Benchmark] Running k6 latency benchmark from client")
			latencyErr = l.RunLatencyBenchmark(ctx, pool, client, params, progress)
			if latencyErr != nil {
				ch <- logLineMsg(fmt.Sprintf("  LATENCY BENCHMARK FAILED: %v", latencyErr))
			}
		}

		// Stop always runs after Start, even if Start or the latency
		// benchmark failed — this is what avoids leaving an orphaned
		// process behind (the exact scenario that blocked a port on us
		// earlier).
		ch <- logLineMsg("[Stop] Killing protocols on nodes (binaries and logs are untouched)")
		if err := l.Stop(ctx, pool, nodes, progress); err != nil {
			ch <- logLineMsg(fmt.Sprintf("  STOP FAILED: %v", err))
			failed++
			continue
		}

		if startErr != nil || latencyErr != nil {
			failed++
		} else {
			succeeded++
		}
	}

	ch <- execDoneMsg{succeeded: succeeded, failed: failed}
}

func (m Model) updateExecutionScreen(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case logLineMsg:
		m.execLog = append(m.execLog, string(msg))
		return m, waitForExecMsg(m.execCh)
	case execDoneMsg:
		m.execSucceeded = msg.succeeded
		m.execFailed = msg.failed
		m.execDone = true
		if m.actionChoice != actionClean {
			m.screen = screenPostRunClean
		}
		// Clean run: stay here, showing the final summary.
		return m, nil
	}
	return m, nil
}

func (m Model) viewExecutionScreen() string {
	var b strings.Builder
	for _, line := range m.execLog {
		fmt.Fprintln(&b, line)
	}

	if m.execDone {
		fmt.Fprintf(&b, "\nDone: %d succeeded, %d failed.\n", m.execSucceeded, m.execFailed)
		if m.actionChoice == actionClean {
			fmt.Fprintf(&b, "ctrl+c to quit\n")
		}
	} else {
		fmt.Fprintf(&b, "\nrunning...\n")
	}

	return b.String()
}
