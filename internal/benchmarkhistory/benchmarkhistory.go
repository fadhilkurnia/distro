package benchmarkhistory

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Entry struct {
	Project       string    `json:"project"`
	Version       string    `json:"version"`
	CommitHash    string    `json:"commitHash"`
	Protocol      string    `json:"protocol"`
	Language      string    `json:"language"`
	Consistency   string    `json:"consistency"`
	Persistency   string    `json:"persistency"`
	BenchmarkType string    `json:"benchmarkType"`
	ResultPath    string    `json:"resultPath"`
	Timestamp     time.Time `json:"timestamp"`
}

type file struct {
	Runs []Entry `json:"runs"`
}

// Append reads path if it exists, appends entry, and writes the result
// back. If path doesn't exist yet, it's created fresh with just entry.
func Append(path string, entry Entry) error {
	var f file

	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &f); err != nil {
			return fmt.Errorf("manifest: parsing existing %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("manifest: reading %s: %w", path, err)
	}

	f.Runs = append(f.Runs, entry)

	out, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest: encoding %s: %w", path, err)
	}
	if err := os.WriteFile(path, out, 0644); err != nil {
		return fmt.Errorf("manifest: writing %s: %w", path, err)
	}
	return nil
}
