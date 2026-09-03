package xdn

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/gitrepo"
	"github.com/fadhilkurnia/distro/internal/k6"
	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)

const serviceName = "bookcatalog"

// reconfiguratorPort is fixed since there's only ever one reconfigurator,
// unlike replica ports which are assigned dynamically per node.
const reconfiguratorPort = 3000

const gigapaxosPortBase = 2000
const httpPortBase = 2300

var meta = launcher.ProjectMeta{
	Name:    "fadhilkurnia.xdn",
	WorkDir: "sut/fadhilkurnia.xdn",
	RepoURL: "https://github.com/fadhilkurnia/xdn",
}

var Versions = []launcher.Version{
	{Name: "main", CommitHash: "0000ea27c860d67ff85ff8848875a4cb920858f8"},
}

var Specs = []launcher.Specification{
	{Protocol: "gigapaxos", Language: "Java", Consistency: "Linearizability", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "Sequential", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "Causal", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "PRAM", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "Eventual", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "MonotonicReads", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "MonotonicWrites", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "ReadYourWrites", Persistency: "On-Disk"},
	{Protocol: "gigapaxos", Language: "Java", Consistency: "WritesFollowReads", Persistency: "On-Disk"},
	// TODO: add primary-backup protocol variants once that coordinator config exists
}

// scripts/services/<service-name>-<consistency>.yaml naming convention.
var consistencyFileSuffix = map[string]string{
	"Linearizability":   "linearizability",
	"Sequential":        "sequential",
	"Causal":            "causal",
	"PRAM":              "pram",
	"Eventual":          "eventual",
	"MonotonicReads":    "monotonic_reads",
	"MonotonicWrites":   "monotonic_writes",
	"ReadYourWrites":    "read_your_writes",
	"WritesFollowReads": "writes_follow_reads",
}

func init() {
	registry.AddProject(registry.Project{
		Name:       meta.Name,
		Repository: meta.RepoURL,
		NewLauncher: func(spec launcher.Specification, version launcher.Version) launcher.Launcher {
			return &XDNLauncher{ProjectMeta: meta, spec: spec, version: version}
		},
		Specifications: Specs,
		Versions:       Versions,
	})
}

type XDNLauncher struct {
	launcher.ProjectMeta
	spec      launcher.Specification
	version   launcher.Version
	addresses []launcher.NodeAddress
}

func (l *XDNLauncher) Specification() launcher.Specification { return l.spec }
func (l *XDNLauncher) Version() launcher.Version             { return l.version }
func (l *XDNLauncher) Addresses() []launcher.NodeAddress     { return l.addresses }

func remoteSSHKeyPath(workDir string) string {
	return workDir + "/id_distrobench"
}

// Get path for service YAML file for the chosen consistency model
func (l *XDNLauncher) serviceYAMLRelPath() (string, error) {
	suffix, ok := consistencyFileSuffix[l.spec.Consistency]
	if !ok {
		return "", fmt.Errorf("no yaml filename mapping for consistency %q", l.spec.Consistency)
	}
	return fmt.Sprintf("scripts/services/%s-%s.yaml", serviceName, suffix), nil
}

// Sends all files inside localDir to remoteDir
func sendDir(ctx context.Context, r runner.Runner, localDir, remoteDir string) error {
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return fmt.Errorf("reading %s: %w", localDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		local := filepath.Join(localDir, e.Name())
		remote := remoteDir + "/" + e.Name()
		if err := r.SendToNode(ctx, local, remote); err != nil {
			return fmt.Errorf("sending %s: %w", e.Name(), err)
		}
	}
	return nil
}

func computePortMap(replicas []config.Node) []launcher.NodeAddress {
	nextGigapaxosPort := map[string]int{}
	nextHTTPPort := map[string]int{}
	out := make([]launcher.NodeAddress, len(replicas))

	for i, n := range replicas {
		gp, ok := nextGigapaxosPort[n.PrivateIP]
		if !ok {
			gp = gigapaxosPortBase
		}
		nextGigapaxosPort[n.PrivateIP] = gp + 1

		hp, ok := nextHTTPPort[n.PrivateIP]
		if !ok {
			hp = httpPortBase
		}
		nextHTTPPort[n.PrivateIP] = hp + 1

		out[i] = launcher.NodeAddress{
			NodeID:      n.ID,
			PublicIP:    n.PrivateIP,
			PublicPort:  hp,
			PrivateIP:   n.PrivateIP,
			PrivatePort: gp,
		}
	}
	return out
}

// Generates custom gigapaxos.xdn.properties for XDN
func buildRunConfigProperties(templatePath string, addresses []launcher.NodeAddress, client config.Node) ([]byte, error) {
	raw, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("reading template %s: %w", templatePath, err)
	}

	var b strings.Builder
	b.Write(raw)
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		b.WriteString("\n")
	}

	b.WriteString(fmt.Sprintf("\nDEFAULT_NUM_REPLICAS=%d\n\n", len(addresses)))
	for i, a := range addresses {
		b.WriteString(fmt.Sprintf("active.AR%d=%s:%d\n", i, a.PrivateIP, a.PrivatePort))
	}
	b.WriteString(fmt.Sprintf("\nreconfigurator.RC0=%s:%d\n", client.PrivateIP, reconfiguratorPort))

	return []byte(b.String()), nil
}

// Distribute SSH key to client and check Docker Swarm & FUSE
func (l *XDNLauncher) ensureClusterProvisioned(ctx context.Context, pool *runner.Pool, nodes []config.Node, sshCfg config.SSHConfig, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	if len(replicas) == 0 {
		err := fmt.Errorf("no replica nodes configured")
		progress.Error(err)
		return err
	}
	client := config.ClientNode(nodes)
	control := replicas[0]

	allLocal := true
	for _, n := range append(replicas, client) {
		if !n.Local {
			allLocal = false
			break
		}
	}
	if allLocal {
		progress.Info("All nodes are local. Skipping cluster provisioning (single-machine test)")
		return nil
	}

	r, err := launcher.GetRunner(pool, control, l.ProjectMeta, progress)
	if err != nil { return err }

	remoteKeyPath, err := launcher.DistributeSSHKey(ctx, r, sshCfg, l.WorkDir, progress)
	if err != nil { return err }

	replicaAddrs := make([]string, len(replicas))
	for i, n := range replicas {
		replicaAddrs[i] = n.PrivateIP
	}

	script := gitrepo.ResolveOverride(l.WorkDir, "scripts/ensure-provisioned.sh", l.version.Name)
	env := map[string]string{
		"SSH_KEY_PATH":  remoteKeyPath,
		"SSH_USERNAME":  sshCfg.Username,
		"REPLICA_ADDRS": strings.Join(replicaAddrs, ","),
		"CLIENT_ADDR":   client.PrivateIP,
	}

	progress.Info("Checking for Docker, Swarm, FUSE...")
	if err := nix.Run(ctx, r, script, env); err != nil {
		errMsg := fmt.Errorf("ensuring cluster provisioned: %w", err)
		progress.Error(errMsg)
		return errMsg
	}
	return nil
}

func (l *XDNLauncher) Build(ctx context.Context, pool *runner.Pool, nodes []config.Node, sshCfg config.SSHConfig, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	if len(replicas) == 0 {
		err := fmt.Errorf("no replica nodes configured")
		progress.Error(err)
		return err
	}
	client := config.ClientNode(nodes)
	control := replicas[0]

	if err := l.ensureClusterProvisioned(ctx, pool, nodes, sshCfg, progress); err != nil {
		return err
	}

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

	localCLIPath := filepath.Join(repoDir, "bin", "xdn-linux-amd64")
	if !gitrepo.FileExists(localCLIPath) {
		local := runner.NewLocalRunner(l.WorkDir)
		script := gitrepo.ResolveOverride(l.WorkDir, "scripts/build.sh", l.version.Name)

		progress.Info("Executing %s to compile binaries...", script)
		if err := nix.Run(ctx, local, script, nil); err != nil {
			errMsg := fmt.Errorf("build failed: %w", err)
			progress.Error(errMsg)
			return errMsg
		}

		if !gitrepo.FileExists(localCLIPath) {
			errMsg := fmt.Errorf("build.sh completed but %s was not produced", localCLIPath)
			progress.Error(errMsg)
			return errMsg
		}
	} else {
		progress.Info("Binaries already built, skipping compile...")
	}

	// Sending jars/, conf/, xdn-cli to first replica
	rControl, err := launcher.GetRunner(pool, control, l.ProjectMeta, progress)
	if err != nil { return err }

	progress.Info("Sending jars/ to %s (%s)...", control.ID, control.PublicIP)
	if err := sendDir(ctx, rControl, filepath.Join(repoDir, "jars"), "repo/jars"); err != nil {
		errMsg := fmt.Errorf("sending jars: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	progress.Info("Sending conf/ to %s (%s)...", control.ID, control.PublicIP)
	if err := sendDir(ctx, rControl, filepath.Join(repoDir, "conf"), "repo/conf"); err != nil {
		errMsg := fmt.Errorf("sending conf: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	progress.Info("Sending %s to %s (%s)...", localCLIPath, control.ID, control.PublicIP)
	if err := rControl.SendToNode(ctx, localCLIPath, "repo/bin/xdn-linux-amd64"); err != nil {
		errMsg := fmt.Errorf("sending CLI to control node: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	// Sends xdn-cli + yaml file to client node
	rClient, err := launcher.GetRunner(pool, client, l.ProjectMeta, progress)
	if err != nil { return err }

	progress.Info("Sending %s to %s (%s)...", localCLIPath, client.ID, client.PublicIP)
	if err := rClient.SendToNode(ctx, localCLIPath, "bin/xdn-linux-amd64"); err != nil {
		errMsg := fmt.Errorf("sending CLI to client: %w", err)
		progress.Error(errMsg)
		return errMsg
	}
	if err := rClient.Run(ctx, "chmod +x bin/xdn-linux-amd64", nil); err != nil {
		errMsg := fmt.Errorf("marking CLI executable on client: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	yamlRel, err := l.serviceYAMLRelPath()
	if err != nil {
		progress.Error(err)
		return err
	}
	yamlOverrideRel := gitrepo.ResolveOverride(l.WorkDir, yamlRel, l.version.Name)
	yamlAbs := filepath.Join(l.WorkDir, yamlOverrideRel)

	progress.Debug("Checking if %s exists", yamlAbs)
	if !gitrepo.FileExists(yamlAbs) {
		errMsg := fmt.Errorf("service yaml not found for consistency %q: %s (create it under scripts/services/)", l.spec.Consistency, yamlAbs)
		progress.Error(errMsg)
		return errMsg
	}

	progress.Info("Sending %s to %s (%s)...", yamlAbs, client.ID, client.PublicIP)
	if err := rClient.SendToNode(ctx, yamlAbs, yamlRel); err != nil {
		errMsg := fmt.Errorf("sending service yaml to client: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	return nil
}

func (l *XDNLauncher) Start(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	if len(replicas) == 0 {
		err := fmt.Errorf("no replica nodes configured")
		progress.Error(err)
		return err
	}
	client := config.ClientNode(nodes)
	control := replicas[0]
	short := gitrepo.ShortHash(l.version.CommitHash)

	addresses := computePortMap(replicas)

	templateRel := gitrepo.ResolveOverride(l.WorkDir, "template.properties", l.version.Name)
	templateAbs := filepath.Join(l.WorkDir, templateRel)


	progress.Debug("Generating config from %s", templateRel)
	propsBytes, err := buildRunConfigProperties(templateAbs, addresses, client)
	if err != nil {
		progress.Error(err)
		return err
	}

	localConfigPath := filepath.Join(l.WorkDir, ".build", short, "run_config.properties")
	progress.Info("Writing generated config as %s", localConfigPath)
	if err := os.MkdirAll(filepath.Dir(localConfigPath), 0755); err != nil {
		errMsg := fmt.Errorf("creating %s: %w", filepath.Dir(localConfigPath), err)
		progress.Error(errMsg)
		return errMsg
	}
	if err := os.WriteFile(localConfigPath, propsBytes, 0644); err != nil {
		errMsg := fmt.Errorf("writing %s: %w", localConfigPath, err)
		progress.Error(errMsg)
		return errMsg
	}

	r, err := launcher.GetRunner(pool, control, l.ProjectMeta, progress)
	if err != nil { return err }

	relConfigPath := fmt.Sprintf(".build/%s/run_config.properties", short)
	progress.Info("Sending %s to %s (%s)...", relConfigPath, control.ID, control.PublicIP)
	if err := r.SendToNode(ctx, localConfigPath, relConfigPath); err != nil {
		errMsg := fmt.Errorf("copying config to control node: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	script := gitrepo.ResolveOverride(l.WorkDir, "scripts/start-control.sh", l.version.Name)
	env := map[string]string{
		"CONFIG_PATH":  relConfigPath,
		"SSH_KEY_PATH": remoteSSHKeyPath(l.WorkDir),
	}

	progress.Info("Starting XDN cluster from %s (%s) (gpServer.sh distributes to the rest itself)...", control.ID, control.PublicIP)
	if err := nix.Run(ctx, r, script, env); err != nil {
		errMsg := fmt.Errorf("starting XDN cluster: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	progress.Info("Waiting for cluster to settle (10s)...")
	time.Sleep(10 * time.Second)

	rClient, err := launcher.GetRunner(pool, client, l.ProjectMeta, progress)
	if err != nil { return err }

	yamlRel, err := l.serviceYAMLRelPath()
	if err != nil {
		progress.Error(err)
		return err
	}

	launchScript := gitrepo.ResolveOverride(l.WorkDir, "scripts/launch-service.sh", l.version.Name)
	launchEnv := map[string]string{
		"SERVICE_NAME":      serviceName,
		"SERVICE_YAML_PATH": yamlRel,
		"XDN_CONTROL_PLANE": control.PrivateIP,
	}

	progress.Info("Launching %s service to %s (%s)...", serviceName, client.ID, client.PublicIP)
	if err := nix.Run(ctx, rClient, launchScript, launchEnv); err != nil {
		errMsg := fmt.Errorf("launching service: %w", err)
		progress.Error(errMsg)
		return errMsg
	}

	l.addresses = addresses
	return nil
}

func (l *XDNLauncher) Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	if len(replicas) == 0 {
		err := fmt.Errorf("no replica nodes configured")
		progress.Error(err)
		return err
	}

	control := replicas[0]
	short := gitrepo.ShortHash(l.version.CommitHash)
	relConfigPath := fmt.Sprintf(".build/%s/run_config.properties", short)

	r, err := launcher.GetRunner(pool, control, l.ProjectMeta, progress)
	if err != nil { return err }

	script := gitrepo.ResolveOverride(l.WorkDir, "scripts/stop-control.sh", l.version.Name)
	env := map[string]string{
		"CONFIG_PATH":  relConfigPath,
		"SSH_KEY_PATH": remoteSSHKeyPath(l.WorkDir),
	}

	progress.Info("Stopping XDN cluster from %s (%s) (gpServer.sh reaches the rest itself)...", control.ID, control.PublicIP)
	if err := nix.Run(ctx, r, script, env); err != nil {
		errMsg := fmt.Errorf("stopping XDN cluster: %w", err)
		progress.Error(errMsg)
		return errMsg
	}
	return nil
}

func (l *XDNLauncher) SupportsAddNewPeer() bool { return true }

func (l *XDNLauncher) SupportsMultiClientMode() bool { return true }

// arLabel returns the gigapaxos AR label XDN assigned to node during
// Start, based on its position in l.addresses. Start and every add new
// peer call must be given the exact same node slice, in the exact same
// order, or this lookup silently points at the wrong replica.
func (l *XDNLauncher) arLabel(node config.Node) (string, error) {
	for i, a := range l.addresses {
		if a.NodeID == node.ID {
			return fmt.Sprintf("AR%d", i), nil
		}
	}
	return "", fmt.Errorf("node %s not found in recorded addresses, run Start first", node.ID)
}

func (l *XDNLauncher) AddNewPeer(ctx context.Context, pool *runner.Pool, nodes []config.Node, newPeer config.Node, progress launcher.Progress) (time.Time, time.Time, error) {
	if len(l.addresses) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("no addresses recorded, run Start first")
	}

	replicas := config.ReplicaNodes(nodes)
	control := replicas[0]
	coordinator, err := l.arLabel(control)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	currentARs := make([]string, 0, len(replicas)+1)
	for _, n := range replicas {
		ar, err := l.arLabel(n)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		currentARs = append(currentARs, ar)
	}
	newPeerAR, err := l.arLabel(newPeer)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	desiredARs := append(currentARs, newPeerAR)

	nodesJSON, err := json.Marshal(desiredARs)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("encoding placement nodes: %w", err)
	}
	body := fmt.Sprintf(`{"NODES":%s,"COORDINATOR":%q}`, nodesJSON, coordinator)

	client := config.ClientNode(nodes)
	r, err := launcher.GetRunner(pool, client, l.ProjectMeta, progress)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	script := gitrepo.ResolveOverride(l.WorkDir, "scripts/set-placement.sh", l.version.Name)
	env := map[string]string{
		"XDN_CONTROL_PLANE": control.PrivateIP,
		"SERVICE_NAME":      serviceName,
		"PLACEMENT_BODY":    body,
	}

	progress.Info("Adding %s (%s) to the cluster...", newPeer.ID, newPeerAR)
	triggeredAt := time.Now()
	output, runErr := nix.RunWithOutput(ctx, r, script, env)
	controlPlaneDoneAt := time.Now()

	if runErr != nil {
		errMsg := fmt.Errorf("sending placement request: %w\noutput:\n%s", runErr, output)
		progress.Error(errMsg)
		return triggeredAt, time.Time{}, errMsg
	}
	if err := checkPlacementResponse(output); err != nil {
		progress.Error(err)
		return triggeredAt, controlPlaneDoneAt, err
	}

	return triggeredAt, controlPlaneDoneAt, nil
}

// placementResponse is the JSON body XDN's control plane returns from
// a placement PUT. A 200 status alone does not mean the change actually
// applied, the control plane reports that separately in this body.
type placementResponse struct {
	Failed          bool   `json:"FAILED"`
	ResponseMessage string `json:"RESPONSE_MESSAGE"`
}

// checkPlacementResponse reads what set-placement.sh printed and
// confirms the control plane actually accepted the request, not just
// that curl itself ran without error. output is the JSON body followed
// by a trailing HTTP_STATUS line, added by set-placement.sh so a curl
// level failure can be told apart from an HTTP level one.
func checkPlacementResponse(output string) error {
	body, _, found := strings.Cut(output, "\nHTTP_STATUS:")
	if !found {
		return fmt.Errorf("unexpected response format:\n%s", output)
	}

	var resp placementResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return fmt.Errorf("parsing placement response: %w\nbody:\n%s", err, body)
	}
	if resp.Failed {
		return fmt.Errorf("control plane rejected placement request: %s", resp.ResponseMessage)
	}
	return nil
}


// TODO: replace with the real implementation in a later commit.
func (l *XDNLauncher) AwaitDataPlaneReady(ctx context.Context, target config.Node, pollInterval time.Duration, progress launcher.Progress) (time.Time, error) {
	return time.Time{}, fmt.Errorf("not yet implemented")
}

// TODO: replace with the real implementation in a later commit.
func (l *XDNLauncher) RunAddNewPeerBenchmark(ctx context.Context, pool *runner.Pool, nodes []config.Node, newPeer config.Node, params launcher.AddNewPeerParams, progress launcher.Progress) (string, error) {
	return "", fmt.Errorf("not yet implemented")
}

func (l *XDNLauncher) Clean(ctx context.Context, pool *runner.Pool, nodes []config.Node, removeRepo bool, progress launcher.Progress) error {
	replicas := config.ReplicaNodes(nodes)
	if len(replicas) == 0 {
		err := fmt.Errorf("no replica nodes configured")
		progress.Error(err)
		return err
	}

	client := config.ClientNode(nodes)
	control := replicas[0]
	short := gitrepo.ShortHash(l.version.CommitHash)
	relVersionDir := fmt.Sprintf(".build/%s", short)

	for _, n := range []config.Node{control, client} {
		progress.Info("Cleaning %s (%s)...", n.ID, n.PublicIP)
		r, err := launcher.GetRunner(pool, n, l.ProjectMeta, progress)
		if err != nil {
			return err
		}
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

func (l *XDNLauncher) RunLatencyBenchmark(ctx context.Context, pool *runner.Pool, nodes []config.Node, params launcher.LatencyParams, progress launcher.Progress) (string, error) {
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
	if err != nil {
		return "", err
	}

	runScript := gitrepo.ResolveOverride(l.WorkDir, "scripts/run-latency.sh", l.version.Name)
	return k6.RunAndFetchLatencyResult(ctx, r, genLocalPath, runScript, l.WorkDir, short, params, requestInterval, l.spec, progress)
}

// compile-time check that XDNLauncher satisfies Launcher
var _ launcher.Launcher = (*XDNLauncher)(nil)
