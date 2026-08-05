package runner

import (
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/fadhilkurnia/distro/internal/config"
)

// Pool is an object that tracks which runner is used for which node
// as well as all the SSH connections so they can be reused rather than
// creating new connections on every remote execution
//
// Launcher code never touches ssh.Client directly. It only calls
// pool.For(node, workdir) and gets back a Runner
type Pool struct {
	sshConfig *ssh.ClientConfig

	mu      sync.Mutex
	clients map[string]*ssh.Client // keyed by node.PublicIP
}

func NewPool(cfg config.SSHConfig) (*Pool, error) {
	keyBytes, err := os.ReadFile(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("pool: reading SSH key %s: %w", cfg.KeyPath, err)
	}

	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("pool: parsing SSH key %s: %w", cfg.KeyPath, err)
	}

	return &Pool{
		sshConfig: &ssh.ClientConfig{
			User: cfg.Username,
			Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
			// Setting to skip `known_hosts` so driver machine can 
			// connect straight to remote machine
			// TODO: consider using golang.org/x/crypto/ssh/knownhosts
			// 	 to handle this issue
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			Timeout:         10 * time.Second,
		},
		clients: make(map[string]*ssh.Client),
	}, nil
}

// Returns a Runner for the specified node
// Note: workdir is passed every call because Pool is shared across protocols
func (p *Pool) For(node config.Node, workdir string) (Runner, error) {
	if node.Local {
		return NewLocalRunner(workdir), nil
	}

	client, err := p.clientFor(node.PublicIP)
	if err != nil {
		return nil, err
	}
	return NewSSHRunner(node.PublicIP, workdir, client), nil
}

// Returns an existing SSH connection to host if one is already
// open, or dials a new one and caches it for reuse otherwise
//
// Known limitation: if a cached connection has silently died (e.g. the
// node rebooted, or a network blip dropped it) this will hand back a dead
// client rather than detecting and redialing.
// TODO: Handle dead connections
func (p *Pool) clientFor(host string) (*ssh.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if client, ok := p.clients[host]; ok {
		return client, nil
	}

	client, err := ssh.Dial("tcp", host+":22", p.sshConfig)
	if err != nil {
		return nil, fmt.Errorf("remote: dialing %s: %w", host, err)
	}
	p.clients[host] = client
	return client, nil
}

// Closes every pooled SSH connection. Call this once, when
// distrobench is shutting down (e.g. via defer in main.go).
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for host, client := range p.clients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("remote: closing connection to %s: %w", host, err)
		}
		delete(p.clients, host)
	}
	return firstErr
}
