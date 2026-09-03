package paxi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/gitrepo"
	"github.com/fadhilkurnia/distro/internal/k6"
	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)

var meta = launcher.ProjectMeta{
	Name:    "ailidani.paxi",
	WorkDir: "sut/ailidani.paxi",
	RepoURL: "https://github.com/ailidani/paxi.git",
}

// Paxi's curated list of benchmarkable revisions
var Versions = []launcher.Version{
	{Name: "master", CommitHash: "6823d0b0fb1690a906391bcd5b4e0b01486ea2bd"},
	// Add more entries here, ex:
	// {Name: "post-optimization", CommitHash: "<full 40-char hash>"},
}

// Paxi's full catalog of supported algorithms
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
		Name:       meta.Name,
		Repository: meta.RepoURL,
		NewLauncher: func(spec launcher.Specification, version launcher.Version) launcher.Launcher {
			return &PaxiLauncher{ProjectMeta: meta, spec: spec, version: version}
		},
		Specifications: Specs,
		Versions:       Versions,
	})
}

type PaxiLauncher struct {
	launcher.ProjectMeta // gives ProjectName(), l.WorkDir, l.RepoURL
	spec                 launcher.Specification
	version              launcher.Version
	addresses            []launcher.NodeAddress
}

func (l *PaxiLauncher) Specification() launcher.Specification { return l.spec }
func (l *PaxiLauncher) Version() launcher.Version             { return l.version }
func (l *PaxiLauncher) Addresses() []launcher.NodeAddress     { return l.addresses }

func (l *PaxiLauncher) Build(ctx context.Context, pool *runner.Pool, nodes []config.Node, sshCfg config.SSHConfig, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	short := gitrepo.ShortHash(l.version.CommitHash)
	repoDir := filepath.Join(l.WorkDir, "repo")

	progress.Debug("(If not exists) Cloning %s repository...", l.RepoURL)
	if err := gitrepo.EnsureCloned(repoDir, l.RepoURL); err != nil {
		progress.Error(err)
		return err
	}

	progress.Debug("Checking out to %s", l.version.CommitHash)
	if err := gitrepo.Checkout(repoDir, l.version.CommitHash); err != nil {
		progress.Error(err)
		return err
	}

	localBinPath := filepath.Join(l.WorkDir, ".build", short, "bin", "server")
	if !gitrepo.FileExists(localBinPath) {
		local := runner.NewLocalRunner(l.WorkDir)
		script := gitrepo.ResolveOverride(l.WorkDir, "scripts/build.sh", l.version.Name)
		env := map[string]string{"HASH": short}

		progress.Info("Executing %s to compile binaries...", script)
		if err := nix.Run(ctx, local, script, env); err != nil {
			errMsg := fmt.Errorf("build failed: %w", err)
			progress.Error(errMsg)
			return errMsg
		}

		if !gitrepo.FileExists(localBinPath) {
			errMsg := fmt.Errorf("%s completed but %s was not produced", script, localBinPath)
			progress.Error(errMsg)
			return errMsg
		}
	} else {
		progress.Info("%s already built, skipping compile", localBinPath)
	}
 
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	for _, n := range replicas {
		if err := ctx.Err(); err != nil { return err }

		r, err := launcher.GetRunner(pool, n, l.ProjectMeta, progress)
		if err != nil { return err }

		progress.Info("Sending %s to %s (%s): %s", localBinPath, n.ID, n.PublicIP, relBinPath)
		if err := r.SendToNode(ctx, localBinPath, relBinPath); err != nil {
			errMsg := fmt.Errorf("Copying binary to %s: %w", n.ID, err)
			progress.Error(errMsg)
			return errMsg
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
		return nil, fmt.Errorf("reading template %s: %w", templatePath, err)
	}
 
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("parsing template %s: %w", templatePath, err)
	}
 
	address := make(map[string]string, len(addresses))
	httpAddress := make(map[string]string, len(addresses))
	for i, a := range addresses {
		id := fmt.Sprintf("1.%d", i+1)
		address[id] = fmt.Sprintf("tcp://%s:%d", a.PrivateIP, a.PrivatePort)
		httpAddress[id] = fmt.Sprintf("http://%s:%d", a.PrivateIP, a.PublicPort)
	}
	data["address"] = address
	data["http_address"] = httpAddress
 
	out, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encoding generated config: %w", err)
	}
	return out, nil
}

func (l *PaxiLauncher) Start(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	short := gitrepo.ShortHash(l.version.CommitHash)
 
	templateRel := gitrepo.ResolveOverride(l.WorkDir, "template.json", l.version.Name)
	templateAbs := filepath.Join(l.WorkDir, templateRel)
 
	addresses := computePortMap(nodes)
	progress.Debug("Generating config from %s", templateRel)
	configJSON, err := buildConfigJSON(templateAbs, addresses)
	if err != nil {
		progress.Error(err)
		return err
	}
 
	localConfigPath := filepath.Join(l.WorkDir, ".build", short, "run_config.json")
	progress.Debug("Writing generated config as %s", localConfigPath)
	if err := os.WriteFile(localConfigPath, configJSON, 0644); err != nil {
		errMsg := fmt.Errorf("Writing %s: %w", localConfigPath, err)
		progress.Error(errMsg)
		return errMsg
	}
 
	relConfigPath := fmt.Sprintf(".build/%s/run_config.json", short)
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	script := gitrepo.ResolveOverride(l.WorkDir, "scripts/start.sh", l.version.Name)
 
	for i, n := range replicas {
		r, err := launcher.GetRunner(pool, n, l.ProjectMeta, progress)
		if err != nil { return err }

		progress.Debug("Check if %s exists in %s (%s)", relBinPath, n.ID, n.PublicIP)
		if err := launcher.CheckBinaryExists(ctx, r, relBinPath); err != nil {
			progress.Error(err)
			return err
		}

		progress.Debug("Sending %s to %s (%s): %s", localConfigPath, n.ID, n.PublicIP, relConfigPath)
		if err := r.SendToNode(ctx, localConfigPath, relConfigPath); err != nil {
			errMsg := fmt.Errorf("Copying config to %s: %w", n.ID, err)
			progress.Error(errMsg)
			return errMsg
		}

		env := map[string]string{
			"HASH":      short,
			"NODE_ID":   fmt.Sprintf("1.%d", i+1),
			"ALGORITHM": l.spec.Protocol, // Paxi's own -algorithm flag name
		}

		progress.Info("Executing %s to start protocol in %s (%s)...", script, n.ID, n.PublicIP)
		if err := nix.Run(ctx, r, script, env); err != nil {
			errMsg := fmt.Errorf("starting on %s: %w", n.ID, err)
			progress.Error(errMsg)
			return errMsg
		}
	}
 
	l.addresses = addresses
	return nil
}

func (l *PaxiLauncher) Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	short := gitrepo.ShortHash(l.version.CommitHash)
	script := gitrepo.ResolveOverride(l.WorkDir, "scripts/stop.sh", l.version.Name)
 
	for _, n := range replicas {
		r, err := launcher.GetRunner(pool, n, l.ProjectMeta, progress)
		if err != nil { return err }
 
		env := map[string]string{"HASH": short}

		progress.Info("Stopping protocol in %s (%s)...", n.ID, n.PublicIP)
		if err := nix.Run(ctx, r, script, env); err != nil {
			errMsg := fmt.Errorf("Stopping on %s: %w", n.ID, err)
			progress.Error(errMsg)
			return errMsg
		}
	}
	return nil
}

// Paxi does not support adding a new peer to a running cluster. Its
// config is loaded once at startup with no way to change membership
// while it runs.
func (l *PaxiLauncher) SupportsAddNewPeer() bool { return false }

func (l *PaxiLauncher) SupportsMultiClientMode() bool { return true }

func (l *PaxiLauncher) AddNewPeer(ctx context.Context, pool *runner.Pool, nodes []config.Node, newPeer config.Node, progress launcher.Progress) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, fmt.Errorf("%s does not support adding a new peer", meta.Name)
}

func (l *PaxiLauncher) AwaitDataPlaneReady(ctx context.Context, target config.Node, pollInterval time.Duration, progress launcher.Progress) (time.Time, error) {
	return time.Time{}, fmt.Errorf("%s does not support adding a new peer", meta.Name)
}

func (l *PaxiLauncher) RunAddNewPeerBenchmark(ctx context.Context, pool *runner.Pool, nodes []config.Node, newPeer config.Node, params launcher.AddNewPeerParams, progress launcher.Progress) (string, error) {
	return "", fmt.Errorf("%s does not support adding a new peer", meta.Name)
}
 
func (l *PaxiLauncher) Clean(ctx context.Context, pool *runner.Pool, nodes []config.Node, removeRepo bool, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	short := gitrepo.ShortHash(l.version.CommitHash)
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	relVersionDir := fmt.Sprintf(".build/%s", short)
 
	for _, n := range replicas {
		r, err := launcher.GetRunner(pool, n, l.ProjectMeta, progress)
		if err != nil { return err }
 
		progress.Debug("Checking if %s is still running in %s (%s)", relBinPath, n.ID, n.PublicIP)
		checkCmd := fmt.Sprintf(`! (ps aux | grep %s | grep -v grep > /dev/null)`, relBinPath)
		if err := r.Run(ctx, checkCmd, nil); err != nil {
			errMsg := fmt.Errorf("process still running on %s, stop it first", n.ID)
			progress.Error(errMsg)
			return errMsg
		}
	}
 
	for _, n := range replicas {
		r, err := launcher.GetRunner(pool, n, l.ProjectMeta, progress)
		if err != nil { return err }
 
		progress.Info("Cleaning %s (%s)...", n.ID, n.PublicIP)
		if err := r.Run(ctx, fmt.Sprintf("rm -rf %s", relVersionDir), nil); err != nil {
			errMsg := fmt.Errorf("cleaning %s on %s: %w", relVersionDir, n.ID, err)
			progress.Error(errMsg)
			return errMsg
		}
	}
 
	localVersionDir := filepath.Join(l.WorkDir, ".build", short)
	progress.Info("Removing all local %s", localVersionDir)
	if err := os.RemoveAll(localVersionDir); err != nil {
		errMsg := fmt.Errorf("removing local %s: %w", localVersionDir, err)
		progress.Error(errMsg)
		return errMsg
	}
 
	if removeRepo {
		repoDir := filepath.Join(l.WorkDir, "repo")
		progress.Info("Removing %s repo...", repoDir)
		if err := os.RemoveAll(repoDir); err != nil {
			errMsg := fmt.Errorf("removing %s: %w", repoDir, err)
			progress.Error(errMsg)
			return errMsg
		}
	}
 
	return nil
}

// Generates a k6 script from scripts/latency.js plus a scenarios block 
// built from Addresses() via the shared k6 package, sends it to client, runs k6
// there via scripts/run-latency.sh, and fetches the resulting summary
// JSON back to this Version's .benchmarks/latency directory
func (l *PaxiLauncher) RunLatencyBenchmark(ctx context.Context, pool *runner.Pool, nodes []config.Node, params launcher.LatencyParams, progress launcher.Progress) (string, error) {
	client := config.ClientNode(nodes)
	if len(l.addresses) == 0 {
		errMsg := fmt.Errorf("no addresses recorded, run Start first")
		progress.Error(errMsg)
		return "", errMsg
	}
	short := gitrepo.ShortHash(l.version.CommitHash)

	scriptRel := gitrepo.ResolveOverride(l.WorkDir, "scripts/latency.js", l.version.Name)
	scriptAbs := filepath.Join(l.WorkDir, scriptRel)

	genLocalPath, requestInterval, err := k6.GenerateLatencyScript(l.WorkDir, short, scriptAbs, l.addresses, params.RequestWorkload)
	if err != nil {
		progress.Error(err)
		return "", err
	}

	r, err := launcher.GetRunner(pool, client, l.ProjectMeta, progress)
	if err != nil { return "", err }

	runScript := gitrepo.ResolveOverride(l.WorkDir, "scripts/run-latency.sh", l.version.Name)
	return k6.RunAndFetchLatencyResult(ctx, r, genLocalPath, runScript, l.WorkDir, short, params, requestInterval, l.spec, progress)
}

// compile-time check that PaxiLauncher satisfies Launcher
var _ launcher.Launcher = (*PaxiLauncher)(nil)
