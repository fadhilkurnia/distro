package paxi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/gitrepo"
	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)


func init() {
	registry.Register("ailidani.paxi",
		func() launcher.Launcher { return &PaxiLauncher{} },
		Variants,
		Versions,
	)
}


const workdir = "sut/ailidani.paxi"
const repoURL = "https://github.com/ailidani/paxi.git"
// Curated list of benchmarkable commit versions
// Note: add an entry here for every commit worth comparing against
var Versions = []launcher.Version{
	{Name: "baseline", Ref: "6823d0b0fb1690a906391bcd5b4e0b01486ea2bd"},
	// Add more entries here, ex:
	// {Name: "post-optimization", Ref: "<full 40-char hash>"},
}


// Paxi's full catalog of supported algorithms
var Variants = []launcher.Variant{
	{Name: "paxos", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "epaxos",  	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "sdpaxos", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "wpaxos", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "abd", 		Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "chain", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "vpaxos", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "wankeeper", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "kpaxos", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "paxos_groups", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "dynamo", 	Language: "Go", Consistency: "Eventual", 	Persistency: "In-Memory", Repository: repoURL},
	{Name: "blockchain", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "m2paxos", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
	{Name: "hpaxos", 	Language: "Go", Consistency: "Linearizability", Persistency: "In-Memory", Repository: repoURL},
}


type PaxiLauncher struct {
	launcher.VariantVersion
}


func (l *PaxiLauncher) Name() string { return "ailidani.paxi" }



// Compiles the binary for this launcher's resolved Version (if not already 
// built locally) and copies it to every node. Nothing is started.
func (l *PaxiLauncher) Build(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	_, version, err := l.Resolve(Variants, Versions)
	if err != nil {
		return fmt.Errorf("paxi: %w", err)
	}
	short := gitrepo.ShortHash(version.Ref)
 
	repoDir := filepath.Join(workdir, "repo")
	if err := gitrepo.EnsureCloned(repoDir, repoURL); err != nil {
		return fmt.Errorf("paxi: %w", err)
	}

	if err := gitrepo.Checkout(repoDir, version.Ref); err != nil {
		return fmt.Errorf("paxi: %w", err)
	}
 
	localBinPath := filepath.Join(workdir, ".build", short, "bin", "server")
	if !launcher.FileExists(localBinPath) {
		local := runner.NewLocalRunner(workdir)
		script := launcher.ResolveOverride(workdir, "scripts/build.sh", version.Name)
		env := map[string]string{"HASH": short}
		if err := nix.Run(ctx, local, script, env); err != nil {
			return fmt.Errorf("paxi: build failed: %w", err)
		}

		if !launcher.FileExists(localBinPath) {
			return fmt.Errorf("paxi: build.sh completed but %s was not produced", localBinPath)
		}
	}
 
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	for _, n := range nodes {
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}

		if err := r.Copy(ctx, localBinPath, relBinPath); err != nil {
			return fmt.Errorf("paxi: copying binary to %s: %w", n.ID, err)
		}
	}

	return nil
}

 
// portMapEntry mirrors the Python PaxiLauncher.map_ip_port() output: a
// per-node public/private IP+port assignment.
type portMapEntry struct {
	PublicIP    string
	PrivateIP   string
	PublicPort  int
	PrivatePort int
}
 
// computePortMap ports map_ip_port(): public ports start at 3000 per
// distinct public IP, private ports start at 2000 per distinct private
// IP, each incrementing whenever that same IP appears again (the case
// where multiple nodes share one machine).
func computePortMap(nodes []config.Node) []portMapEntry {
	nextPublicPort := map[string]int{}
	nextPrivatePort := map[string]int{}
	out := make([]portMapEntry, len(nodes))
 
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
 
		out[i] = portMapEntry{
			PublicIP:    n.PublicIP,
			PrivateIP:   n.PrivateIP,
			PublicPort:  pubPort,
			PrivatePort: privPort,
		}
	}
	return out
}
 

// buildConfigJSON ports generate_config(): reads the template, fills in
// "address"/"http_address" per node using Paxi's "1.<n>" node ID
// convention, and returns the result ready to write to disk.
func buildConfigJSON(templatePath string, portMap []portMapEntry) ([]byte, error) {
	raw, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("paxi: reading template %s: %w", templatePath, err)
	}
 
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("paxi: parsing template %s: %w", templatePath, err)
	}
 
	address := make(map[string]string, len(portMap))
	httpAddress := make(map[string]string, len(portMap))
	for i, p := range portMap {
		id := fmt.Sprintf("1.%d", i+1)
		address[id] = fmt.Sprintf("tcp://%s:%d", p.PrivateIP, p.PrivatePort)
		httpAddress[id] = fmt.Sprintf("http://%s:%d", p.PublicIP, p.PublicPort)
	}
	data["address"] = address
	data["http_address"] = httpAddress
 
	out, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("paxi: encoding generated config: %w", err)
	}
	return out, nil
}
 
// Generates run_config.json, copies it to every node, then launches the 
// process on every node. Requires Build to have already placed a binary 
// on each node
func (l *PaxiLauncher) Start(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	variant, version, err := l.Resolve(Variants, Versions)
	if err != nil {
		return fmt.Errorf("paxi: %w", err)
	}
	short := gitrepo.ShortHash(version.Ref)
 
	templateRel := launcher.ResolveOverride(workdir, "template.json", version.Name)
	templateAbs := filepath.Join(workdir, templateRel)
 
	portMap := computePortMap(nodes)
	configJSON, err := buildConfigJSON(templateAbs, portMap)
	if err != nil {
		return err
	}
 
	localConfigPath := filepath.Join(workdir, ".build", short, "run_config.json")
	if err := os.WriteFile(localConfigPath, configJSON, 0644); err != nil {
		return fmt.Errorf("paxi: writing %s: %w", localConfigPath, err)
	}
 
	relConfigPath := fmt.Sprintf(".build/%s/run_config.json", short)
	relBinPath := fmt.Sprintf(".build/%s/bin/server", short)
	script := launcher.ResolveOverride(workdir, "scripts/start.sh", version.Name)
 
	for i, n := range nodes {
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}
 
		// Cheap existence check so a missing Build produces a clear
		// error here, rather than a confusing failure inside start.sh.
		if err := r.Run(ctx, fmt.Sprintf("test -f %s", relBinPath), nil); err != nil {
			return fmt.Errorf("paxi: binary not found on %s, run Build first: %w", n.ID, err)
		}
 
		if err := r.Copy(ctx, localConfigPath, relConfigPath); err != nil {
			return fmt.Errorf("paxi: copying config to %s: %w", n.ID, err)
		}
 
		env := map[string]string{
			"HASH":      short,
			"NODE_ID":   fmt.Sprintf("1.%d", i+1),
			"ALGORITHM": variant.Name,
		}
		if err := nix.Run(ctx, r, script, env); err != nil {
			return fmt.Errorf("paxi: starting on %s: %w", n.ID, err)
		}
	}
	return nil
}


// Kills the running process on every node and deletes the generated
// config (both remote copies, via stop.sh, and the local copy). The
// binary is left in place.
func (l *PaxiLauncher) Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	_, version, err := l.Resolve(Variants, Versions)
	if err != nil {
		return fmt.Errorf("paxi: %w", err)
	}
	short := gitrepo.ShortHash(version.Ref)
	script := launcher.ResolveOverride(workdir, "scripts/stop.sh", version.Name)
 
	for _, n := range nodes {
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("paxi: getting runner for %s: %w", n.ID, err)
		}
		env := map[string]string{"HASH": short}
		if err := nix.Run(ctx, r, script, env); err != nil {
			return fmt.Errorf("paxi: stopping on %s: %w", n.ID, err)
		}
	}
 
	localConfigPath := filepath.Join(workdir, ".build", short, "run_config.json")
	if err := os.Remove(localConfigPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("paxi: removing local config %s: %w", localConfigPath, err)
	}
	return nil
}


// compile-time check that PaxiLauncher satisfies Launcher.
var _ launcher.Launcher = (*PaxiLauncher)(nil)
