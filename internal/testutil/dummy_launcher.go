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

const projectName = "dummy"
const workdir = "internal/testutil"

func init() {
	registry.AddProject(registry.Project{
		Name:       projectName,
		Repository: "",
		NewLauncher: func(spec launcher.Specification, version launcher.Version) launcher.Launcher {
			return &DummyLauncher{}
		},
		Specifications: nil, // empty catalog — contributes zero rows to GetInstances
		Versions:       nil,
	})
}

// DummyLauncher does nothing real: Build is a no-op, Start and Stop just
// run testdata/echo.sh on every node via that node's Runner.
type DummyLauncher struct{}

func (d *DummyLauncher) ProjectName() string                   { return projectName }
func (d *DummyLauncher) Specification() launcher.Specification { return launcher.Specification{} }
func (d *DummyLauncher) Version() launcher.Version             { return launcher.Version{} }
func (d *DummyLauncher) Addresses() []launcher.NodeAddress { return nil }

func noopProgress(string) {}

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
	if progress == nil { progress = noopProgress}

	for _, n := range nodes {
		progress(fmt.Sprintf("%s %s (%s)...", verb, n.ID, n.PublicIP))
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("dummy: getting runner for %s: %w", n.ID, err)
		}

		msg := fmt.Sprintf("%s %s (%s)", verb, n.ID, r.Host())
		env := map[string]string{"MESSAGE": msg}
		if err := nix.Run(ctx, r, "testdata/echo.sh", env); err != nil {
			return fmt.Errorf("dummy: %s on %s: %w", verb, n.ID, err)
		}
	}
	return nil
}

func (d *DummyLauncher) RunLatencyBenchmark(ctx context.Context, pool *runner.Pool, client config.Node, params launcher.LatencyParams, progress launcher.Progress) error {
	return nil
}
