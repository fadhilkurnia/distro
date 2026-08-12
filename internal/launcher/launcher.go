package launcher

import (
	"context"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/runner"
)

// A node's actual address, as assigned during Start
type NodeAddress struct {
	NodeID      string
	PublicIP    string
	PrivateIP   string
	PublicPort  int
	PrivatePort int
}

// k6 latency benchmark configurations (read once from .env)
type LatencyParams struct {
	WarmupDuration string  // e.g. "60s"
	Duration       string  // e.g. "180s"
	WriteRatio     float64 // e.g. 0.2
}

// A combination of protocol, language, consistency, persistency 
// the launcher for a project can run as
type Specification struct {
	Protocol    string // ex: "paxos", "epaxos"
	Language    string // ex: "Go", "Java"
	Consistency string // ex: "Linearizability", "Eventual"
	Persistency string // ex: "In-Memory", "On-Disk"
}

// Which commit version of the project is being benchmarked
type Version struct {
	Name       string // human readable label. Used in:
			  // <name>-build.sh, <name>-start.sh, <name>-stop.sh
	CommitHash string // full 40-character commit hash
}

// Reports a human-readable line of ongoing status from inside a
// Build/Start/Stop/Clean call. Similar to a logger
// The messages are sent directly to a background goroutine so 
// Bubble Tea (TUI) can render them to the terminal
type Progress func(message string)

// Describes what functions are required for all project Launcher
//
// Build/Start/Stop/Clean follow a fixed division of responsibility:
//   - Build: compile the binary, copy it to every node. Nothing runs yet.
//   - Start: generate the config, copy it to every node, launch the
//     process on every node.
//   - Stop: kill the running process on every node.
//   - Clean: removes build output (binary, generated config, logs) 
//     from every node, and optionally the shared repository clone
type Launcher interface {
	// ex: "ailidani.paxi"
	ProjectName() string

	// Selected Specification for this Launcher instance
	Specification() Specification

	// Selected Version for this Launcher instance
	Version() Version 

	// Each node's actual address (assigned only at Start)
	Addresses() []NodeAddress

	Build(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress Progress) error
	Start(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress Progress) error
	Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress Progress) error
	Clean(ctx context.Context, pool *runner.Pool, nodes []config.Node, removeRepo bool, progress Progress) error

	// **********************
	// Benchmark functions:
	// **********************

	// Runs a k6-based latency benchmark from client to all Adresses()
	RunLatencyBenchmark(ctx context.Context, pool *runner.Pool, client config.Node, params LatencyParams, progress Progress) error
}
