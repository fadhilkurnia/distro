package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
)

// Runner to handle scripts that runs locally (in the same machine as the driver)
type LocalRunner struct {
	// The protocol's base directory. Example:
	// - sut/ailidani.paxi
	// - fadhilkurnia.xdn
	workdir string
}

func NewLocalRunner(workdir string) *LocalRunner {
	return &LocalRunner{workdir: workdir}
}

// Create an *exec.Cmd:
// - What to run
// - Where to run it
// - What environments to give it
func (r *LocalRunner) buildCommand(ctx context.Context, script string, env map[string]string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "bash", script)
	cmd.Dir = r.workdir
	// Makes sure the command has PATH, HOME, etc
	cmd.Env = append(os.Environ(), mapToEnvSlice(env)...)
	return cmd
}

// Use this if you don't need live output
func (r *LocalRunner) Run(ctx context.Context, script string, env map[string]string) error {
	cmd := r.buildCommand(ctx, script, env)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("local: %s failed: %w", script, err)
	}
	return nil
}

// Use this if you need live output
func (r *LocalRunner) Stream(ctx context.Context, script string, env map[string]string) (*StreamHandle, error) {
	cmd := r.buildCommand(ctx, script, env)

	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("local: creating output pipe for %s: %w", script, err)
	}
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return nil, fmt.Errorf("local: starting %s: %w", script, err)
	}

	var (
		once    sync.Once
		waitErr error
	)
	done := make(chan struct{})

	go func() {
		waitErr = cmd.Wait() // Call Wait() once only, otherwise will panic
		pw.Close()           // unblocks any pending read on pr with io.EOF
		close(done)
	}()

	return &StreamHandle{
		Output: pr,
		wait: func() error {
			once.Do(func() { <-done })
			return waitErr
		},
	}, nil
}

// Note: will automatically create targetPath's parent directory 
// 	 if it doesn't exist yet.
func (r *LocalRunner) Copy(ctx context.Context, sourcePath, targetPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return fmt.Errorf("local: creating directory for %s: %w", targetPath, err)
	}

	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("local: opening %s: %w", sourcePath, err)
	}
	defer src.Close()

	dst, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("local: creating %s: %w", targetPath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("local: copying %s to %s: %w", sourcePath, targetPath, err)
	}
	return nil
}

func (r *LocalRunner) Host() string {
	return "127.0.0.1"
}

// Converts a map of environment variables into "KEY=VALUE" string slices
// Keys are sorted purely so output/behavior is deterministic across runs 
func mapToEnvSlice(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(env))
	for _, k := range keys {
		out = append(out, fmt.Sprintf("%s=%s", k, env[k]))
	}
	return out
}

// compile-time check that LocalRunner satisfies Runner
var _ Runner = (*LocalRunner)(nil)
