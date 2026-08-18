package k6

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/runner"
)

const (
	// filename of generated from `scripts/latency.js` template
	GeneratedScriptFilename = "latency-generated.js"
 
	// k6 latency benchmark output file in the remote machine
	// NOTE: this will be renamed when it is fetched to driver machine
	ResultFilename = "latency-result.json"
 
	// Placeholder inside `scripts/latency.js` template that will be replaced
	ScenariosPlaceholder = "/* SCENARIOS */"
)

// Generate the k6 latency benchmark scenarios for a protocol:
// - Warmup
// - Benchmark
// There will be 1 client for every node.
func BuildScenarios(addresses []launcher.NodeAddress) string {
	var b strings.Builder
	for _, a := range addresses {
		addr := fmt.Sprintf("%s:%d", a.PrivateIP, a.PublicPort)
		fmt.Fprintf(
			&b, `		%s_warmup: {
				executor: "constant-vus",
				vus: 1,
				duration: __ENV.WARMUP_DURATION,
				env: { ADDR: %q },
				exec: "warmup",
			},
			%s: {
				executor: "constant-vus",
				vus: 1,
				duration: __ENV.DURATION,
				startTime: __ENV.WARMUP_DURATION,
				env: { ADDR: %q },
				exec: "benchmark",
			},
			`, a.NodeID, addr, a.NodeID, addr,
		)
	}
	return b.String()
}

// Generate `.build/<commit-hash>/<GeneratedScriptFilename>` file
func GenerateLatencyScript(workdir, short, templatePath string, addresses []launcher.NodeAddress, requestWorkload int) (localPath string, requestInterval float64, err error) {
	if requestWorkload <= 0 {
		return "", 0, fmt.Errorf("RequestWorkload must be positive, got %d", requestWorkload)
	}
	requestInterval = float64(len(addresses)) / float64(requestWorkload)
 
	templateBytes, err := os.ReadFile(templatePath)
	if err != nil {
		return "", 0, fmt.Errorf("reading %s: %w", templatePath, err)
	}
 
	scenarios := BuildScenarios(addresses)
	generated := strings.Replace(string(templateBytes), ScenariosPlaceholder, scenarios, 1)
 
	localPath = filepath.Join(workdir, ".build", short, GeneratedScriptFilename)
	if err := os.WriteFile(localPath, []byte(generated), 0644); err != nil {
		return "", 0, fmt.Errorf("writing %s: %w", localPath, err)
	}
	return localPath, requestInterval, nil
}
 
// Run k6 benchmark using <genLocalPath>.js workload file
// Then fetch the result from the client machine to the driver machine
func RunAndFetchLatencyResult(ctx context.Context, r runner.Runner, genLocalPath, runScript, workdir, short string, params launcher.LatencyParams, requestInterval float64, spec launcher.Specification, progress launcher.Progress) (string, error) {
	relGenPath := fmt.Sprintf(".build/%s/%s", short, GeneratedScriptFilename)
	progress.Info("Sending %s to client...", relGenPath)
	if err := r.SendToNode(ctx, genLocalPath, relGenPath); err != nil {
		errMsg := fmt.Errorf("sending latency script to client: %w", err)
		progress.Error(errMsg)
		return "", errMsg
	}
 
	relResultPath := fmt.Sprintf(".build/%s/%s", short, ResultFilename)
	env := map[string]string{
		"SCRIPT_PATH":      relGenPath,
		"RESULT_PATH":      relResultPath,
		"WARMUP_DURATION":  params.WarmupDuration,
		"DURATION":         params.Duration,
		"WRITE_RATIO":      fmt.Sprintf("%v", params.WriteRatio),
		"REQUEST_INTERVAL": fmt.Sprintf("%v", requestInterval),
	}
 
	progress.Info("Running k6 latency with %s on client...", runScript)
	k6Err := nix.Run(ctx, r, runScript, env)
	if k6Err != nil {
		progress.Warning("k6 reported failure (threshold breach or error): %v", k6Err)
	}
 
	localResultPath, err := launcher.FetchBenchmarkResult(ctx, r, workdir, short, "latency", relResultPath, spec, params, progress)
	if err != nil {
		return "", err
	}
 
	if k6Err != nil {
		return localResultPath, fmt.Errorf("k6 reported failure (results still saved to %s): %w", localResultPath, k6Err)
	}
	return localResultPath, nil
}
