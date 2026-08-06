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

func (d *DummyLauncher) Build(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	return nil
}

func (d *DummyLauncher) Start(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	return d.runOnEach(ctx, pool, nodes, "starting")
}

func (d *DummyLauncher) Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	return d.runOnEach(ctx, pool, nodes, "stopping")
}

func (d *DummyLauncher) Clean(ctx context.Context, pool *runner.Pool, nodes []config.Node, removeRepo bool) error {
	return nil
}

func (d *DummyLauncher) runOnEach(ctx context.Context, pool *runner.Pool, nodes []config.Node, verb string) error {
	for _, n := range nodes {
		r, err := pool.For(n, workdir)
		if err != nil {
			return fmt.Errorf("%s: getting runner for %s: %w", projectName, n.ID, err)
		}

		msg := fmt.Sprintf("%s %s (%s)", verb, n.ID, r.Host())
		env := map[string]string{"MESSAGE": msg}
		if err := nix.Run(ctx, r, "testdata/echo.sh", env); err != nil {
			return fmt.Errorf("%s: %s on %s: %w", projectName, verb, n.ID, err)
		}
	}
	return nil
}
