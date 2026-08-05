package runner

import (
	"context"
	"io"
)

// Stream to get a script's live output and the exeution status
// Usage:
// 1. Read `Output` until it's exhausted
// 2. Call `Wait()` to get the script's execution status
// Note: `Wait()` blocks until the script is complete, so a launcher 
// can call `Wait()` without waiting for `Output`  to be exhausted 
// and the launcher will wait until the script is complete
type StreamHandle struct {
	// The script's combined stdout/stderr.
	// Read until it returns io.EOF.
	Output io.Reader

	// Blocks until the script stops and return execution status
	wait func() error
}

// Public function to call wait
func (s *StreamHandle) Wait() error {
	return s.wait()
}

// Runner is a handler to run scripts on a node, then:
// - Get the script's live output (CLI logs)
// - Get the script's execution status (does the script succeed?)
// The protocol's launcher doesn't need to know if 
// the script is being run locally or remotely using SSH
// 
// Example usage:
// 	runner, err := pool.For(node)
//	err = runner.Run(ctx, "scripts/start.sh", map[string]string{"NODE_ID": "1.2"})
type Runner interface {
	// Executes a script (local or remote), wait for it to complete (blocking),
	// then return the execution status
	// - script = path/to/bash-script/file
	// - env = environment variables the bash script needs
	// Returns nil on success
	Run(ctx context.Context, script string, env map[string]string) error

	// Executes a script (local or remote), immediately returns 
	// a StreamHandle so the live output is captured,
	// Then use `Wait()` to check for exec status (blocking)
	Stream(ctx context.Context, script string, env map[string]string) (*StreamHandle, error)

	// Copy files from sourcePath to targetPath target
	// Some protocols need certain config files and binaries to run correctly
	// - sourcePath is a relative path from the driver machine's workDir
	// - targetPath can be either local or remote
	Copy(ctx context.Context, sourcePath, targetPath string) error

	// The IP address of the node the Runner is handling
	// - LocalRunner = "127.0.0.1"
	// - SSHRunner = a public IP to the node
	// Intended for loggin/TUI display, not for running scripts with
	Host() string
}
