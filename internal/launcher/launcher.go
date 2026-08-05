package launcher

import (
	"context"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/runner"
)

// Describes a protocol's static properties for *this* benchmark round
// Note: The same protocol can have multiple benchmarks with different
// 	 settings (name, language, consistency, persistency) depending 
// 	 on the protocol's implementation
type Metadata struct {
	Name        string // ex: "paxos", "epaxos" (the specific protocol variant)
	Language    string // ex: "Go", "Java"
	Consistency string // ex: "Linearizability", "Eventual"
	Persistency string // ex: "In-Memory", "On-Disk"
}

// Describes what functions are required for all protocol Launcher
type Launcher interface {
	// Name of the project. Example:
	// - ailidani.paxi
	// - apache.zookeeper
	Name() string

	// Builds the protocol in each node:
	// - Copies data from driver (binaries, etc)
	// - Cloning git directory and compiling binary
	// - Running build.sh script
	Build(ctx context.Context, pool *runner.Pool, nodes []config.Node) error

	// Launches the protocol in each node:
	// - Generate & send config files for each nodes
	// - Running start.sh script
	Start(ctx context.Context, pool *runner.Pool, nodes []config.Node) error

	// Stops the protocol in each node:
	// - Stops start.sh script and kill protocol process PID
	// - Delete configs
	// - Clean up /tmp and state files (if exists)
	Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node) error

	// Returns this protocol's static description.
	Metadata() Metadata
}
