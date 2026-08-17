package testutil

import (
	"context"
	"fmt"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/launcher"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"
)

var meta = launcher.ProjectMeta{
	Name:    "dummy",
	WorkDir: "internal/testutil",
	RepoURL: "",
}

func init() {
	registry.AddProject(registry.Project{
		Name:       meta.Name,
		Repository: meta.RepoURL,
		NewLauncher: func(spec launcher.Specification, version launcher.Version) launcher.Launcher {
			return &DummyLauncher{ProjectMeta: meta}
		},
		Specifications: nil, // empty catalog — contributes zero rows to GetInstances
		Versions:       nil,
	})
}

// DummyLauncher does nothing real: Build is a no-op, Start and Stop just
// run testdata/echo.sh on every node via that node's Runner.
type DummyLauncher struct {
	launcher.ProjectMeta // gives ProjectName(), d.WorkDir, d.RepoURL
}

func (d *DummyLauncher) Specification() launcher.Specification { return launcher.Specification{} }
func (d *DummyLauncher) Version() launcher.Version             { return launcher.Version{} }
func (d *DummyLauncher) Addresses() []launcher.NodeAddress     { return nil }

func (d *DummyLauncher) Build(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	return nil
}

func (d *DummyLauncher) Start(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	return d.runOnEach(ctx, pool, nodes, progress, "starting")
}

func (d *DummyLauncher) Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress) error {
	return d.runOnEach(ctx, pool, nodes, progress, "stopping")
}

func (d *DummyLauncher) Clean(ctx context.Context, pool *runner.Pool, nodes []config.Node, removeRepo bool, progress launcher.Progress) error {
	return nil
}

func (d *DummyLauncher) runOnEach(ctx context.Context, pool *runner.Pool, nodes []config.Node, progress launcher.Progress, verb string) error {
	for _, n := range nodes {
		progress.Info("%s %s (%s)...", verb, n.ID, n.PublicIP)

		r, err := launcher.GetRunner(pool, n, d.ProjectMeta, progress)
		if err != nil {
			return err
		}

		msg := fmt.Sprintf("%s %s (%s)", verb, n.ID, r.Host())
		env := map[string]string{"MESSAGE": msg}
		if err := nix.Run(ctx, r, "testdata/echo.sh", env); err != nil {
			errMsg := fmt.Errorf("%s on %s: %w", verb, n.ID, err)
			progress.Error(errMsg)
			return errMsg
		}
	}
	return nil
}

func (d *DummyLauncher) RunLatencyBenchmark(context.Context, *runner.Pool, config.Node, launcher.LatencyParams, launcher.Progress) (string, error) {
	return "", nil
}

// compile-time check that DummyLauncher satisfies Launcher
var _ launcher.Launcher = (*DummyLauncher)(nil)
