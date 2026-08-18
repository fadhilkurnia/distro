package launcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
	WarmupDuration  string  // ex: "60s"
	Duration        string  // ex: "180s"
	WriteRatio      float64 // ex: 0.2
	RequestWorkload int     // total requests/sec across all nodes combined
	OutputFilename  string  // (optional) falls back to an auto-generated name if empty
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

// Identity data every launcher needs: 
// - its registered name
// - its working directory convention
// - the repository URL it's built from
type ProjectMeta struct {
	Name    string // ex: "ailidani.paxi"
	WorkDir string // ex: "sut/ailidani.paxi"
	RepoURL string
}
 
// Satisfies the Launcher interface's ProjectName() method
func (m ProjectMeta) ProjectName() string { return m.Name }


//************************
// Logging and Progress
//************************
// Level identifies the severity of a Progress message.
type Level int
 
const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarning
	LevelError
)
 
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarning:
		return "WARNING"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Prints status log from inside a Build/Start/Stop/Clean/benchmark call.
// Used by bubbletea to show live log
type Progress struct {
	emit func(level Level, message string)
}
 
// NewProgress builds a Progress that forwards every leveled message to emit.
func NewProgress(emit func(level Level, message string)) Progress {
	return Progress{emit: emit}
}
 
// Don't print anything if the logger is set to nil
func (p Progress) log(level Level, msg string) {
	if p.emit == nil { return }
	p.emit(level, msg)
}
 
func (p Progress) Debug(format string, args ...any) { p.log(LevelDebug, fmt.Sprintf(format, args...)) }
func (p Progress) Info(format string, args ...any)  { p.log(LevelInfo, fmt.Sprintf(format, args...)) }
func (p Progress) Warning(format string, args ...any) { p.log(LevelWarning, fmt.Sprintf(format, args...)) }

// This function ONLY logs the error message. It doesn't return the error.
// Example usage:
//	if err != nil {
//	    errMsg := fmt.Errorf("getting runner for %s: %w", n.ID, err)
//	    progress.Error(errMsg)
//	    return errMsg
//	}
func (p Progress) Error(err error) { p.log(LevelError, err.Error()) }

//************************
// Launcher Interface
//************************

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

	Build(ctx context.Context, pool *runner.Pool, nodes []config.Node, sshCfg config.SSHConfig, progress Progress) error
	Start(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress Progress) error
	Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress Progress) error
	Clean(ctx context.Context, pool *runner.Pool, nodes []config.Node, removeRepo bool, progress Progress) error

	// **********************
	// Benchmark functions:
	// **********************

	// Runs a k6-based latency benchmark from client to all Adresses()
	// Returns the local path to the fetched result file on success
	RunLatencyBenchmark(ctx context.Context, pool *runner.Pool, nodes []config.Node, params LatencyParams, progress Progress) (string, error)
}


// **********************
// Helper Functions
// **********************
// Check if a binary file acually exists in a path
func CheckBinaryExists(ctx context.Context, r runner.Runner, relBinPath string) error {
	if err := r.Run(ctx, fmt.Sprintf("test -f %s", relBinPath), nil); err != nil {
		return fmt.Errorf("binary not found at %s, run Build first: %w", relBinPath, err)
	}
	return nil
}
 
// Get Runner (scoped to meta.WorkDir) for a launcher
func GetRunner(pool *runner.Pool, n config.Node, meta ProjectMeta, progress Progress) (runner.Runner, error) {
	progress.Debug("Getting launcher runner in %s (%s)...", n.ID, n.PublicIP)

	r, err := pool.For(n, meta.WorkDir)
	if err != nil {
		errMsg := fmt.Errorf("getting runner for %s: %w", n.ID, err)
		progress.Error(errMsg)
		return nil, errMsg
	}
	return r, nil
}

// Copies sshCfg.KeyPath to all nodes
// Assumption: sshCfg's public key is already authorized on every node
func DistributeSSHKey(ctx context.Context, r runner.Runner, sshCfg config.SSHConfig, remoteDir string, progress Progress) (string, error) {
	remoteKeyPath := remoteDir + "/id_distrobench"
 
	progress.Info("Sending %s to %s...", sshCfg.KeyPath, r.Host())
	if err := r.SendToNode(ctx, sshCfg.KeyPath, remoteKeyPath); err != nil {
		errMsg := fmt.Errorf("sending SSH key: %w", err)
		progress.Error(errMsg)
		return "", errMsg
	}
 
	if err := r.Run(ctx, fmt.Sprintf("chmod 600 %s", remoteKeyPath), nil); err != nil {
		errMsg := fmt.Errorf("chmod SSH key: %w", err)
		progress.Error(errMsg)
		return "", errMsg
	}
 
	return remoteKeyPath, nil
}
 
// Get latency benchmark output filename (in driver machine)
// Format:
// <protocol>:<language>:<consistency>:<persistency>:<duration>:w<writeRatio>.json
func LatencyBenchmarkResultFilename(spec Specification, params LatencyParams) string {
	if params.OutputFilename != "" { return params.OutputFilename }
	return fmt.Sprintf("%s:%s:%s:%s:%s:w%v.json",
		spec.Protocol, spec.Language, spec.Consistency, spec.Persistency, params.Duration, params.WriteRatio)
}
 
// Get benchmark result file from remote machine to driver machine.
// NOTE: Currently only supports latency benchmark output
// Local Directory Path:
// sut/<project>/.benchmarks/<benchmarkType>/<short>
func FetchBenchmarkResult(ctx context.Context, r runner.Runner, workdir, short, benchmarkType, relResultPath string, spec Specification, params LatencyParams, progress Progress) (string, error) {
	localResultDir := filepath.Join(workdir, ".benchmarks", benchmarkType, short)
	if err := os.MkdirAll(localResultDir, 0755); err != nil {
		errMsg := fmt.Errorf("creating %s: %w", localResultDir, err)
		progress.Error(errMsg)
		return "", errMsg
	}
	localResultPath := filepath.Join(localResultDir, LatencyBenchmarkResultFilename(spec, params))
 
	progress.Info("Fetching results from client...")
	if err := r.FetchFromNode(ctx, relResultPath, localResultPath); err != nil {
		errMsg := fmt.Errorf("fetching results: %w", err)
		progress.Error(errMsg)
		return "", errMsg
	}
	return localResultPath, nil
}
