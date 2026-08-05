package runner

import (
	"context"
	"io"
)

// Stream to get a command's live output and the execution status
// Usage:
// 1. Read `Output` until it's exhausted
// 2. Call `Wait()` to get the command's execution status
// Note: `Wait()` blocks until the command is complete, so a launcher
// can call `Wait()` without waiting for `Output` to be exhausted
// and the launcher will wait until the command is complete
type StreamHandle struct {
	// The command's combined stdout/stderr.
	// Read until it returns io.EOF.
	Output io.Reader

	// Blocks until the command stops and returns execution status
	wait func() error
}

// Public function to call wait
func (s *StreamHandle) Wait() error {
	return s.wait()
}

// Runner is a handler to run commands on a node, then:
// - Get the command's live output (CLI logs)
// - Get the command's execution status (did the command succeed?)
// The protocol's launcher doesn't need to know if
// the command is being run locally or remotely using SSH.
//
// Every real protocol's Build/Start/Stop goes through internal/nix
// (which wraps a script inside a `nix-shell ... --run` command) rather
// than calling Runner directly. The thing being executed is
// always a fully-composed command by the time it reaches a Runner.
//
// Example usage:
//
//	runner, err := pool.For(node, workdir)
//	err = runner.Run(ctx, "nix-shell shell.nix --run './scripts/start.sh'", map[string]string{"NODE_ID": "1.2"})
//
type Runner interface {
	// Executes cmd (local or remote), waits for it to complete (blocking),
	// then returns the execution status
	// - cmd = a full command line to execute, e.g. via a shell
	// - env = environment variables the command needs
	// Returns nil on success
	Run(ctx context.Context, cmd string, env map[string]string) error

	// Executes cmd (local or remote), immediately returns
	// a StreamHandle so the live output is captured,
	// Then use `Wait()` to check for exec status (blocking)
	Stream(ctx context.Context, cmd string, env map[string]string) (*StreamHandle, error)

	// Copy files from sourcePath to targetPath target
	// Some protocols need certain config files and binaries to run correctly
	// - sourcePath is a relative path from the driver machine's workDir
	// - targetPath can be either local or remote
	Copy(ctx context.Context, sourcePath, targetPath string) error

	// The IP address of the node the Runner is handling
	// - LocalRunner = "127.0.0.1"
	// - SSHRunner = a public IP to the node
	// Intended for logging/TUI display, not for running commands with
	Host() string
}
