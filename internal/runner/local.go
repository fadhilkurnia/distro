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

// Runner to handle commands that run locally (in the same machine as the driver)
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
// - What to run (cmd, executed via "bash -c" since it's a 
// full command line - ex: `nix-shell shell.nix --run '...'`)
// - Where to run it
// - What environment to give it
func (r *LocalRunner) buildCommand(ctx context.Context, cmd string, env map[string]string) *exec.Cmd {
	c := exec.CommandContext(ctx, "bash", "-c", cmd)
	c.Dir = r.workdir
	// Makes sure the command has PATH, HOME, etc
	c.Env = append(os.Environ(), mapToEnvSlice(env)...)
	return c
}

// Use this if you don't need live output
func (r *LocalRunner) Run(ctx context.Context, cmd string, env map[string]string) error {
	c := r.buildCommand(ctx, cmd, env)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr

	if err := c.Run(); err != nil {
		return fmt.Errorf("local: %s failed: %w", cmd, err)
	}
	return nil
}

// Use this if you need live output
func (r *LocalRunner) Stream(ctx context.Context, cmd string, env map[string]string) (*StreamHandle, error) {
	c := r.buildCommand(ctx, cmd, env)

	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("local: creating output pipe for %s: %w", cmd, err)
	}
	c.Stdout = pw
	c.Stderr = pw

	if err := c.Start(); err != nil {
		pw.Close()
		pr.Close()
		return nil, fmt.Errorf("local: starting %s: %w", cmd, err)
	}

	var (
		once    sync.Once
		waitErr error
	)
	done := make(chan struct{})

	go func() {
		waitErr = c.Wait() // Call Wait() once only, otherwise will panic
		pw.Close()         // unblocks any pending read on pr with io.EOF
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
//	if it doesn't exist yet.
func (r *LocalRunner) Copy(ctx context.Context, sourcePath, targetPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Resolve targetPath relative to this Runner's workdir
	targetPath = filepath.Join(r.workdir, targetPath)

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

// Converts a map of environment variables into "KEY=VALUE" string slices.
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
