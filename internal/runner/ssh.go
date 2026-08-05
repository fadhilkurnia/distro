package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/fadhilkurnia/distro/internal/shellquote"
)

// Runner to handle commands that run on a remote machine over SSH
type SSHRunner struct {
	// the node's public IP, returned by Host()
	host string

	// The protocol's directory in the remote machine, relative to that
	// node's home directory. Pool.For prepends "distro/" automatically.
	// Ex: passing "sut/ailidani.paxi" results in this field holding
	// "distro/sut/ailidani.paxi", landing at
	// ~/distro/sut/ailidani.paxi on the remote machine.
	workdir string

	client  *ssh.Client

	sftpMu sync.Mutex
	sftp   *sftp.Client // lazily created on first Copy, then reused
}

func NewSSHRunner(host, workdir string, client *ssh.Client) *SSHRunner {
	return &SSHRunner{host: host, workdir: workdir, client: client}
}

// Assembles cmd and environment variables into a plain string
// to be run through the SSH protocol.
// This is needed because SSH doesn't let a client hand arbitrary
// environment variables to the remote shell like in a local shell.
// Example:
// "cd sut/ailidani.paxi && NODE_ID=1.2 nix-shell shell.nix --run './scripts/start.sh'"
//	^-- workdir              ^-- env       ^-- cmd (already fully composed by the caller)
func (r *SSHRunner) buildCommand(cmd string, env map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "cd %s && ", shellquote.Quote(r.workdir))

	// Sorted purely for deterministic output (see mapToEnvSlice in
	// local.go for the same reasoning) - has no effect on the command.
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s ", k, shellquote.Quote(env[k]))
	}

	b.WriteString(cmd)
	return b.String()
}

// Use this if you don't need live output
func (r *SSHRunner) Run(ctx context.Context, cmd string, env map[string]string) error {
	session, err := r.client.NewSession()
	if err != nil {
		return fmt.Errorf("ssh[%s]: opening session: %w", r.host, err)
	}
	defer session.Close()

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr

	done := make(chan error, 1)
	go func() { done <- session.Run(r.buildCommand(cmd, env)) }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("ssh[%s]: %s failed: %w", r.host, cmd, err)
		}
		return nil
	case <-ctx.Done():
		// Best-effort abort: closing the session tells the remote side
		// to tear down, but unlike a local process (which Go can kill
		// outright via exec.CommandContext), there's no hard guarantee
		// the remote command stops instantly. It depends on the remote
		// shell/process handling the closed connection promptly.
		session.Close()
		return ctx.Err()
	}
}

// Use this if you need live output
func (r *SSHRunner) Stream(ctx context.Context, cmd string, env map[string]string) (*StreamHandle, error) {
	session, err := r.client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("ssh[%s]: opening session: %w", r.host, err)
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		session.Close()
		return nil, fmt.Errorf("ssh[%s]: creating output pipe for %s: %w", r.host, cmd, err)
	}
	session.Stdout = pw
	session.Stderr = pw

	if err := session.Start(r.buildCommand(cmd, env)); err != nil {
		pw.Close()
		pr.Close()
		session.Close()
		return nil, fmt.Errorf("ssh[%s]: starting %s: %w", r.host, cmd, err)
	}

	var (
		once    sync.Once
		waitErr error
	)
	done := make(chan struct{})

	go func() {
		waitErr = session.Wait() // Call Wait() once only, otherwise will panic
		pw.Close()               // unblocks any pending read on pr with io.EOF
		session.Close()
		close(done)
	}()

	// SSH session has no built-in ctx support, so if ctx is cancelled
	// before the command finishes on its own, this closes the session
	// to force it to stop.
	// If the command already finished, done is already closed and
	// this goroutine exits immediately without doing anything.
	go func() {
		select {
		case <-ctx.Done():
			session.Close()
		case <-done:
		}
	}()

	return &StreamHandle{
		Output: pr,
		wait: func() error {
			once.Do(func() { <-done })
			return waitErr
		},
	}, nil
}

// Create an SFTP client for the Runner to copy files with
func (r *SSHRunner) sftpClient() (*sftp.Client, error) {
	r.sftpMu.Lock()
	defer r.sftpMu.Unlock()

	if r.sftp != nil {
		return r.sftp, nil
	}
	client, err := sftp.NewClient(r.client)
	if err != nil {
		return nil, err
	}
	r.sftp = client
	return client, nil
}

// Note: will automatically create targetPath's parent directory
//	 if it doesn't exist yet.
func (r *SSHRunner) Copy(ctx context.Context, sourcePath, targetPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Resolve targetPath relative to this Runner's workdir, same as
	// Run/Stream already do for cmd's script paths.
	targetPath = path.Join(r.workdir, targetPath)

	sftpClient, err := r.sftpClient()
	if err != nil {
		return fmt.Errorf("ssh[%s]: opening sftp client: %w", r.host, err)
	}

	if err := sftpClient.MkdirAll(path.Dir(targetPath)); err != nil {
		return fmt.Errorf("ssh[%s]: creating remote directory for %s: %w", r.host, targetPath, err)
	}

	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("ssh[%s]: opening local file %s: %w", r.host, sourcePath, err)
	}
	defer src.Close()

	dst, err := sftpClient.Create(targetPath)
	if err != nil {
		return fmt.Errorf("ssh[%s]: creating remote file %s: %w", r.host, targetPath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("ssh[%s]: copying %s to %s: %w", r.host, sourcePath, targetPath, err)
	}
	return nil
}

func (r *SSHRunner) Host() string {
	return r.host
}

// compile-time check that SSHRunner satisfies Runner.
var _ Runner = (*SSHRunner)(nil)
