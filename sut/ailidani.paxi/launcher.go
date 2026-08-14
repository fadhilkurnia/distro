package paxi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/gitrepo"
	"github.com/fadhilkurnia/distro/internal/k6"
	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)

const projectName = "ailidani.paxi"

// This project's working directory. Convention: sut/<project-name>
const workdir = "sut/" + projectName

const repoURL = "https://github.com/ailidani/paxi.git"

// Paxi's curated list of benchmarkable revisions
var Versions = []launcher.Version{
	{Name: "master", CommitHash: "6823d0b0fb1690a906391bcd5b4e0b01486ea2bd"},
	// Add more entries here, ex:
	// {Name: "post-optimization", CommitHash: "<full 40-char hash>"},
}

// Specs is Paxi's full catalog of supported algorithms, ported directly
// from the Python PROTOCOLS table.
var Specs = []launcher.Specification{
	{Protocol: "paxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "epaxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "sdpaxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "wpaxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "abd", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "chain", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "vpaxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "wankeeper", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "kpaxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "paxos_groups", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "dynamo", 		Language: "Go", Consistency: "Eventual", 	Persistency: "In-Memory"},
	{Protocol: "blockchain", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "m2paxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
	{Protocol: "hpaxos", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory"},
}

func init() {
	registry.AddProject(registry.Project{
		Name:       projectName,
		Repository: repoURL,
		NewLauncher: func(spec launcher.Specification, version launcher.Version) launcher.Launcher {
			return &PaxiLauncher{spec: spec, version: version}
		},
		Specifications: Specs,
		Versions:       Versions,
	})
}

type PaxiLauncher struct {
	spec      launcher.Specification
	version   launcher.Version
	addresses []launcher.NodeAddress
}

func (l *PaxiLauncher) ProjectName() string                   { return projectName }
func (l *PaxiLauncher) Specification() launcher.Specification { return l.spec }
func (l *PaxiLauncher) Version() launcher.Version             { return l.version }
func (l *PaxiLauncher) Addresses() []launcher.NodeAddress     { return l.addresses }

// used whenever a caller passes a nil Progress
func noopProgress(string) {}

func (l *PaxiLauncher) Build(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	if progress == nil {
		progress = noopProgress
	}
	short := gitrepo.ShortHash(l.version.CommitHash)
 
	repoDir := filepath.Join(workdir, "repo")
	if err := gitrepo.EnsureCloned(repoDir, repoURL); err != nil {
		return fmt.Errorf("paxi: %w", err)
	}
	if err := gitrepo.Checkout(repoDir, l.version.CommitHash); err != nil {
		return fmt.Errorf("paxi: %w", err)
	}
 
	localBinPath := filepath.Join(workdir, ".build", short, "bin", "server")
	if !gitrepo.FileExists(localBinPath) {
		progress("Executing build.sh to compile binaries...")
		local := runner.NewLocalRunner(workdir)
		script := gitrepo.ResolveOverride(workdir, "scripts/build.sh", l.version.Name)
		env := map[string]string{"HASH": short}
		if err := nix.Run(ctx, local, script, env); err != nil {
			return fmt.Errorf("paxi: build failed: %w", err)
		}
		if !gitrepo.FileExists(localBinPath) {
			return fmt.Errorf("paxi: build.sh completed but %s was not produced", localBinPath)
		}
	} else {
		progress("Binary already built, skipping compile...")
	}
 
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	for _, n := range nodes {
		progress(fmt.Sprintf("Sending binaries to %s (%s)...", n.ID, n.PublicIP))
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}
		if err := r.SendToNode(ctx, localBinPath, relBinPath); err != nil {
			return fmt.Errorf("paxi: copying binary to %s: %w", n.ID, err)
		}
	}
	return nil
}

func computePortMap(nodes []config.Node) []launcher.NodeAddress {
	nextPublicPort := map[string]int{}
	nextPrivatePort := map[string]int{}
	out := make([]launcher.NodeAddress, len(nodes))
 
	for i, n := range nodes {
		pubPort, ok := nextPublicPort[n.PublicIP]
		if !ok {
			pubPort = 3000
		}
		nextPublicPort[n.PublicIP] = pubPort + 1
 
		privPort, ok := nextPrivatePort[n.PrivateIP]
		if !ok {
			privPort = 2000
		}
		nextPrivatePort[n.PrivateIP] = privPort + 1
 
		out[i] = launcher.NodeAddress{
			NodeID:      n.ID,
			PublicIP:    n.PublicIP,
			PrivateIP:   n.PrivateIP,
			PublicPort:  pubPort,
			PrivatePort: privPort,
		}
	}
	return out
}

func buildConfigJSON(templatePath string, addresses []launcher.NodeAddress) ([]byte, error) {
	raw, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("paxi: reading template %s: %w", templatePath, err)
	}
 
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("paxi: parsing template %s: %w", templatePath, err)
	}
 
	address := make(map[string]string, len(addresses))
	httpAddress := make(map[string]string, len(addresses))
	for i, a := range addresses {
		id := fmt.Sprintf("1.%d", i+1)
		address[id] = fmt.Sprintf("tcp://%s:%d", a.PrivateIP, a.PrivatePort)
		httpAddress[id] = fmt.Sprintf("http://%s:%d", a.PublicIP, a.PublicPort)
	}
	data["address"] = address
	data["http_address"] = httpAddress
 
	out, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("paxi: encoding generated config: %w", err)
	}
	return out, nil
}

func (l *PaxiLauncher) Start(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	if progress == nil {
		progress = noopProgress
	}
	short := gitrepo.ShortHash(l.version.CommitHash)
 
	templateRel := gitrepo.ResolveOverride(workdir, "template.json", l.version.Name)
	templateAbs := filepath.Join(workdir, templateRel)
 
	addresses := computePortMap(nodes)
	configJSON, err := buildConfigJSON(templateAbs, addresses)
	if err != nil {
		return err
	}
 
	localConfigPath := filepath.Join(workdir, ".build", short, "run_config.json")
	if err := os.WriteFile(localConfigPath, configJSON, 0644); err != nil {
		return fmt.Errorf("paxi: writing %s: %w", localConfigPath, err)
	}
 
	relConfigPath := fmt.Sprintf(".build/%s/run_config.json", short)
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	script := gitrepo.ResolveOverride(workdir, "scripts/start.sh", l.version.Name)
 
	for i, n := range nodes {
		progress(fmt.Sprintf("Starting protocol in %s (%s)...", n.ID, n.PublicIP))
 
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}
 
		if err := r.Run(ctx, fmt.Sprintf("test -f %s", relBinPath), nil); err != nil {
			return fmt.Errorf("paxi: binary not found on %s, run Build first: %w", n.ID, err)
		}
 
		if err := r.SendToNode(ctx, localConfigPath, relConfigPath); err != nil {
			return fmt.Errorf("paxi: copying config to %s: %w", n.ID, err)
		}
 
		env := map[string]string{
			"HASH":      short,
			"NODE_ID":   fmt.Sprintf("1.%d", i+1),
			"ALGORITHM": l.spec.Protocol, // Paxi's own -algorithm flag name
		}
 
		if err := nix.Run(ctx, r, script, env); err != nil {
			return fmt.Errorf("paxi: starting on %s: %w", n.ID, err)
		}
	}
 
	l.addresses = addresses
	return nil
}

func (l *PaxiLauncher) Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	if progress == nil {
		progress = noopProgress
	}
	short := gitrepo.ShortHash(l.version.CommitHash)
	script := gitrepo.ResolveOverride(workdir, "scripts/stop.sh", l.version.Name)
 
	for _, n := range nodes {
		progress(fmt.Sprintf("Stopping protocol in %s (%s)...", n.ID, n.PublicIP))
 
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}
 
		env := map[string]string{"HASH": short}
		if err := nix.Run(ctx, r, script, env); err != nil {
			return fmt.Errorf("paxi: stopping on %s: %w", n.ID, err)
		}
	}
	return nil
}
 
func (l *PaxiLauncher) Clean(ctx context.Context, pool *runner.Pool, nodes []config.Node, removeRepo bool, progress launcher.Progress) error {
	if progress == nil {
		progress = noopProgress
	}
	short := gitrepo.ShortHash(l.version.CommitHash)
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	relVersionDir := fmt.Sprintf(".build/%s", short)
 
	for _, n := range nodes {
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}
 
		checkCmd := fmt.Sprintf(`! (ps aux | grep %s | grep -v grep > /dev/null)`, relBinPath)
		if err := r.Run(ctx, checkCmd, nil); err != nil {
			return fmt.Errorf("paxi: process still running on %s, stop it first", n.ID)
		}
	}
 
	for _, n := range nodes {
		progress(fmt.Sprintf("Cleaning %s (%s)...", n.ID, n.PublicIP))
 
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}
 
		if err := r.Run(ctx, fmt.Sprintf("rm -rf %s", relVersionDir), nil); err != nil {
			return fmt.Errorf("paxi: cleaning %s on %s: %w", relVersionDir, n.ID, err)
		}
	}
 
	localVersionDir := filepath.Join(workdir, ".build", short)
	if err := os.RemoveAll(localVersionDir); err != nil {
		return fmt.Errorf("paxi: removing local %s: %w", localVersionDir, err)
	}
 
	if removeRepo {
		progress("Removing repo...")
		repoDir := filepath.Join(workdir, "repo")
		if err := os.RemoveAll(repoDir); err != nil {
			return fmt.Errorf("paxi: removing %s: %w", repoDir, err)
		}
	}
 
	return nil
}

// Generates a k6 script from scripts/latency.js plus a scenarios block 
// built from Addresses() via the shared k6 package, sends it to client, runs k6
// there via scripts/run-latency.sh, and fetches the resulting summary
// JSON back to this Version's .benchmarks/latency directory
func (l *PaxiLauncher) RunLatencyBenchmark(ctx context.Context, pool *runner.Pool, client config.Node, params launcher.LatencyParams, progress launcher.Progress) error {
	if progress == nil {
		progress = noopProgress
	}
	if len(l.addresses) == 0 {
		return fmt.Errorf("paxi: no addresses recorded, run Start first")
	}
	short := gitrepo.ShortHash(l.version.CommitHash)
 
	scriptRel := gitrepo.ResolveOverride(workdir, "scripts/latency.js", l.version.Name)
	scriptAbs := filepath.Join(workdir, scriptRel)
	templateBytes, err := os.ReadFile(scriptAbs)
	if err != nil {
		return fmt.Errorf("paxi: reading %s: %w", scriptAbs, err)
	}
 
	scenarios := k6.BuildScenarios(l.addresses)
	generated := strings.Replace(string(templateBytes), "/* SCENARIOS */", scenarios, 1)
 
	genLocalPath := filepath.Join(workdir, ".build", short, "latency-generated.js")
	if err := os.WriteFile(genLocalPath, []byte(generated), 0644); err != nil {
		return fmt.Errorf("paxi: writing %s: %w", genLocalPath, err)
	}
 
	r, err := pool.For(client, workdir)
	if err != nil {
		return fmt.Errorf("paxi: getting runner for client: %w", err)
	}
 
	relGenPath := fmt.Sprintf(".build/%s/latency-generated.js", short)
	progress(fmt.Sprintf("Sending benchmark script to client (%s)...", client.PublicIP))
	if err := r.SendToNode(ctx, genLocalPath, relGenPath); err != nil {
		return fmt.Errorf("paxi: sending latency script to client: %w", err)
	}

	relResultPath := fmt.Sprintf(".build/%s/latency-result.json", short)
	progress(fmt.Sprintf("Running k6 latency benchmark on client (%s)...", client.PublicIP))
	runScript := gitrepo.ResolveOverride(workdir, "scripts/run-latency.sh", l.version.Name)
	env := map[string]string{
		"SCRIPT_PATH":     relGenPath,
		"RESULT_PATH":     relResultPath,
		"WARMUP_DURATION": params.WarmupDuration,
		"DURATION":        params.Duration,
		"WRITE_RATIO":     fmt.Sprintf("%v", params.WriteRatio),
	}

	k6Err := nix.Run(ctx, r, runScript, env)
	if k6Err != nil {
		progress(fmt.Sprintf("  k6 reported failure (threshold breach or error): %v", k6Err))
	}

	localResultDir := filepath.Join(workdir, ".benchmarks", "latency", short)
	if err := os.MkdirAll(localResultDir, 0755); err != nil {
		return fmt.Errorf("paxi: creating %s: %w", localResultDir, err)
	}
	filename := fmt.Sprintf("%s-%s-%s-%s-%s-w%v.json",
		l.spec.Protocol, l.spec.Language, l.spec.Consistency, l.spec.Persistency, params.Duration, params.WriteRatio)
	localResultPath := filepath.Join(localResultDir, filename)

	progress(fmt.Sprintf("Fetching results from client (%s)...", client.PublicIP))
	if err := r.FetchFromNode(ctx, relResultPath, localResultPath); err != nil {
		return fmt.Errorf("paxi: fetching latency results: %w", err)
	}

	if k6Err != nil {
		return fmt.Errorf("paxi: k6 reported failure (results still saved to %s): %w", localResultPath, k6Err)
	}
	return nil
}

// compile-time check that PaxiLauncher satisfies Launcher
var _ launcher.Launcher = (*PaxiLauncher)(nil)
