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

	// The Add New Peer benchmark runs warmup and benchmark as two
	// separate k6 invocations, not one script with an internal
	// startTime offset, so each phase needs its own generated script
	// and result file name to avoid the two runs colliding with each
	// other on the client node.
	GeneratedAddNewPeerWarmupScriptFilename    = "add-new-peer-warmup-generated.js"
	GeneratedAddNewPeerBenchmarkScriptFilename = "add-new-peer-benchmark-generated.js"
	AddNewPeerWarmupResultFilename             = "add-new-peer-warmup-result.json"
	AddNewPeerBenchmarkResultFilename          = "add-new-peer-benchmark-result.json"
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

// BuildAddNewPeerScenarios generates the k6 scenario block for one
// phase of the Add New Peer benchmark. Only initialAddresses are
// targeted, the new peer never receives traffic. clientMode is
// "single" or "multi", matching AddNewPeerParams.ClientMode. execName
// is the k6 exec function to call for this phase, "warmup" or
// "benchmark", matching the function names scripts/latency.js already
// defines.
func BuildAddNewPeerScenarios(initialAddresses []launcher.NodeAddress, clientMode, execName, durationEnvVar string) (string, error) {
	if len(initialAddresses) == 0 {
		return "", fmt.Errorf("initialAddresses must not be empty")
	}

	var targets []launcher.NodeAddress
	switch clientMode {
	case "single":
		targets = initialAddresses[:1]
	case "multi":
		targets = initialAddresses
	default:
		return "", fmt.Errorf("unknown client mode %q, expected \"single\" or \"multi\"", clientMode)
	}

	var b strings.Builder
	for _, a := range targets {
		addr := fmt.Sprintf("%s:%d", a.PrivateIP, a.PublicPort)
		fmt.Fprintf(
			&b, `		%s: {
				executor: "constant-vus",
				vus: 1,
				duration: __ENV.%s,
				env: { ADDR: %q },
				exec: %q,
			},
			`, a.NodeID, durationEnvVar, addr, execName,
		)
	}
	return b.String(), nil
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

// GenerateAddNewPeerScript generates one script file for one phase of
// the Add New Peer benchmark. filename picks which of the warmup or
// benchmark generated file names to write to, so the two phases never
// collide on the client node.
func GenerateAddNewPeerScript(workdir, short, templatePath string, initialAddresses []launcher.NodeAddress, requestWorkload int, clientMode, execName, durationEnvVar, filename string) (localPath string, requestInterval float64, err error) {
	if requestWorkload <= 0 {
		return "", 0, fmt.Errorf("RequestWorkload must be positive, got %d", requestWorkload)
	}

	numTargets := 1
	if clientMode == "multi" {
		numTargets = len(initialAddresses)
	}
	requestInterval = float64(numTargets) / float64(requestWorkload)

	templateBytes, err := os.ReadFile(templatePath)
	if err != nil {
		return "", 0, fmt.Errorf("reading %s: %w", templatePath, err)
	}

	scenarios, err := BuildAddNewPeerScenarios(initialAddresses, clientMode, execName, durationEnvVar)
	if err != nil {
		return "", 0, err
	}
	generated := strings.Replace(string(templateBytes), ScenariosPlaceholder, scenarios, 1)

	localPath = filepath.Join(workdir, ".build", short, filename)
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

// RunAddNewPeerWarmup runs the warmup phase of the Add New Peer
// benchmark on the client node and blocks until it finishes. The
// result is never fetched, since the warmup phase is setup, not part
// of what gets measured. A failure here should abort the whole
// benchmark, since a broken warmup means the deployment itself is
// suspect.
func RunAddNewPeerWarmup(ctx context.Context, r runner.Runner, genLocalPath, runScript, short string, params launcher.AddNewPeerParams, requestInterval float64, progress launcher.Progress) error {
	relGenPath := fmt.Sprintf(".build/%s/%s", short, GeneratedAddNewPeerWarmupScriptFilename)
	progress.Info("Sending %s to client...", relGenPath)
	if err := r.SendToNode(ctx, genLocalPath, relGenPath); err != nil {
		errMsg := fmt.Errorf("sending warmup script to client: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	relResultPath := fmt.Sprintf(".build/%s/%s", short, AddNewPeerWarmupResultFilename)
	env := map[string]string{
		"SCRIPT_PATH":      relGenPath,
		"RESULT_PATH":      relResultPath,
		"WARMUP_DURATION":  params.WarmupDuration,
		"WRITE_RATIO":      fmt.Sprintf("%v", params.WriteRatio),
		"REQUEST_INTERVAL": fmt.Sprintf("%v", requestInterval),
	}

	progress.Info("Running warmup phase on client...")
	if err := nix.Run(ctx, r, runScript, env); err != nil {
		errMsg := fmt.Errorf("warmup phase failed: %w", err)
		progress.Error(errMsg)
		return errMsg
	}
	return nil
}

// StartAddNewPeerBenchmarkPhase starts the benchmark phase of the Add
// New Peer benchmark on the client node and returns right away,
// without waiting for it to finish. The caller uses the returned
// StreamHandle to wait for completion later, after it has triggered
// the reconfiguration and waited for both signals, since k6 keeps
// running for its full configured duration regardless of what happens
// with the reconfiguration itself.
func StartAddNewPeerBenchmarkPhase(ctx context.Context, r runner.Runner, genLocalPath, runScript, short string, params launcher.AddNewPeerParams, requestInterval float64, progress launcher.Progress) (*runner.StreamHandle, string, error) {
	relGenPath := fmt.Sprintf(".build/%s/%s", short, GeneratedAddNewPeerBenchmarkScriptFilename)
	progress.Info("Sending %s to client...", relGenPath)
	if err := r.SendToNode(ctx, genLocalPath, relGenPath); err != nil {
		errMsg := fmt.Errorf("sending benchmark script to client: %w", err)
		progress.Error(errMsg)
		return nil, "", errMsg
	}

	relResultPath := fmt.Sprintf(".build/%s/%s", short, AddNewPeerBenchmarkResultFilename)
	env := map[string]string{
		"SCRIPT_PATH":      relGenPath,
		"RESULT_PATH":      relResultPath,
		"DURATION":         params.Duration,
		"WRITE_RATIO":      fmt.Sprintf("%v", params.WriteRatio),
		"REQUEST_INTERVAL": fmt.Sprintf("%v", requestInterval),
	}

	progress.Info("Starting benchmark phase on client...")
	stream, err := nix.Stream(ctx, r, runScript, env)
	if err != nil {
		errMsg := fmt.Errorf("starting benchmark phase: %w", err)
		progress.Error(errMsg)
		return nil, "", errMsg
	}
	return stream, relResultPath, nil
}

// FetchAddNewPeerResult blocks until the benchmark phase process
// finishes, then fetches its result file from the client node to the
// driver machine. k6 reporting a failure, a threshold breach or a
// request error, is logged as a warning, not returned as an error,
// since the result is kept either way. Watching what happens to
// latency when the reconfiguration itself is slow or broken is the
// point of this benchmark, not something to hide by discarding the
// result.
func FetchAddNewPeerResult(ctx context.Context, r runner.Runner, stream *runner.StreamHandle, relResultPath, workdir, short string, spec launcher.Specification, params launcher.AddNewPeerParams, progress launcher.Progress) (string, error) {
	k6Err := stream.Wait()
	if k6Err != nil {
		progress.Warning("k6 reported failure (threshold breach or error): %v", k6Err)
	}

	localResultDir := filepath.Join(workdir, ".benchmarks", "addnewpeer", short)
	if err := os.MkdirAll(localResultDir, 0755); err != nil {
		errMsg := fmt.Errorf("creating %s: %w", localResultDir, err)
		progress.Error(errMsg)
		return "", errMsg
	}

	filename := params.OutputFilename
	if filename == "" {
		filename = fmt.Sprintf("%s:%s:%s:%s:%s:w%v.json",
			spec.Protocol, spec.Language, spec.Consistency, spec.Persistency, params.Duration, params.WriteRatio)
	}
	localResultPath := filepath.Join(localResultDir, filename)

	progress.Info("Fetching results from client...")
	if err := r.FetchFromNode(ctx, relResultPath, localResultPath); err != nil {
		errMsg := fmt.Errorf("fetching results: %w", err)
		progress.Error(errMsg)
		return "", errMsg
	}

	if k6Err != nil {
		return localResultPath, fmt.Errorf("k6 reported failure (results still saved to %s): %w", localResultPath, k6Err)
	}
	return localResultPath, nil
}
