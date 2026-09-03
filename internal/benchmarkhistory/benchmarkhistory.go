package benchmarkhistory

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Latency Benchmark Output
type LatencyBenchmarkEntry struct {
	Project     string    `json:"project"`
	Version     string    `json:"version"`
	CommitHash  string    `json:"commitHash"`
	Protocol    string    `json:"protocol"`
	Language    string    `json:"language"`
	Consistency string    `json:"consistency"`
	Persistency string    `json:"persistency"`
	ResultPath  string    `json:"resultPath"`
	Timestamp   time.Time `json:"timestamp"`
}

// Add New Peer Benchmark Output
//
// Timestamps are stored as one absolute anchor plus offsets in seconds
// from that anchor, not three separate absolute timestamps. The Go code
// that triggers the reconfiguration runs on the driver machine, while k6
// runs on the remote client node. Comparing absolute timestamps across
// two machines means trusting clock sync between them, and that can
// drift without warning. Offsets from one shared reference point remove
// that problem, and match what a latency versus time graph needs anyway,
// since k6's own per request output is relative to the same start point.
//
// The absolute anchor is kept too, so the wall clock time of any event
// can still be reconstructed if needed, for example to cross reference
// container logs from that same run.
type AddNewPeerBenchmarkEntry struct {
	Project        string   `json:"project"`
	Version        string   `json:"version"`
	CommitHash     string   `json:"commitHash"`
	Protocol       string   `json:"protocol"`
	Language       string   `json:"language"`
	Consistency    string   `json:"consistency"`
	Persistency    string   `json:"persistency"`
	ClientMode     string   `json:"clientMode"`     // "single" or "multi"
	InitialNodeIDs []string `json:"initialNodeIds"` // e.g. ["node1", "node2", "node3"]
	NewPeerNodeID  string   `json:"newPeerNodeId"`  // e.g. "node4"
	Outcome        string   `json:"outcome"`        // "success", "failed", "timed_out"

	BenchmarkStartedAt        time.Time `json:"benchmarkStartedAt"`
	AddNewPeerOffsetSec       *float64  `json:"addNewPeerOffsetSec,omitempty"`
	ControlPlaneDoneOffsetSec *float64  `json:"controlPlaneDoneOffsetSec,omitempty"`
	DataPlaneReadyOffsetSec   *float64  `json:"dataPlaneReadyOffsetSec,omitempty"`

	ResultPath string    `json:"resultPath"`
	Timestamp  time.Time `json:"timestamp"`
}

type file struct {
	LatencyRuns    []LatencyBenchmarkEntry    `json:"latencyRuns"`
	AddNewPeerRuns []AddNewPeerBenchmarkEntry `json:"addNewPeerRuns"`
}

// readFile reads path if it exists and returns its parsed contents. If
// path does not exist yet, it returns a zero value file with no error.
func readFile(path string) (file, error) {
	var f file
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return f, nil
		}
		return f, fmt.Errorf("manifest: reading %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return f, fmt.Errorf("manifest: parsing existing %s: %w", path, err)
	}
	return f, nil
}

// writeFile writes f back to path as indented JSON.
func writeFile(path string, f file) error {
	out, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest: encoding %s: %w", path, err)
	}
	if err := os.WriteFile(path, out, 0644); err != nil {
		return fmt.Errorf("manifest: writing %s: %w", path, err)
	}
	return nil
}

// AppendLatency reads path if it exists, appends entry to the latency
// runs, and writes the result back. If path does not exist yet, it is
// created fresh with just entry.
func AppendLatency(path string, entry LatencyBenchmarkEntry) error {
	f, err := readFile(path)
	if err != nil {
		return err
	}
	f.LatencyRuns = append(f.LatencyRuns, entry)
	return writeFile(path, f)
}

// AppendAddNewPeer reads path if it exists, appends entry to the add new
// peer runs, and writes the result back. If path does not exist yet, it
// is created fresh with just entry.
func AppendAddNewPeer(path string, entry AddNewPeerBenchmarkEntry) error {
	f, err := readFile(path)
	if err != nil {
		return err
	}
	f.AddNewPeerRuns = append(f.AddNewPeerRuns, entry)
	return writeFile(path, f)
}
