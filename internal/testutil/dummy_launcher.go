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

// Registers "dummy" for this Launcher inside the Registry
// Note: DummyLauncher does nothing real. Build is a no-op, Start and Stop
// just run testdata/echo.sh (via nix.Run, wrapped in this package's own
// shell.nix) on every node via that node's Runner. It exists purely to
// check that config -> Pool -> Runner -> nix -> registry all fit
// together correctly before any real protocol is ported.
func init() {
	registry.Register("dummy",
		func() launcher.Launcher { return &DummyLauncher{} },
		nil, // no Variants. dummy has nothing meaningful to catalog
		nil, // no Versions. same
	)
}

// workdir is this package's own directory, matching the sut/<protocol>
// convention every real protocol will follow: shell.nix and scripts/
// (here, testdata/) both live directly under it.
const workdir = "internal/testutil"

type DummyLauncher struct{}

func (d *DummyLauncher) Name() string { return "dummy" }

func (d *DummyLauncher) Build(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	// Nothing to build. This is what a protocol with no build step
	// (or one already built) would look like.
	return nil
}

func (d *DummyLauncher) Start(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	return d.runOnEach(ctx, pool, nodes, "starting")
}

func (d *DummyLauncher) Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node) error {
	return d.runOnEach(ctx, pool, nodes, "stopping")
}

func (d *DummyLauncher) runOnEach(ctx context.Context, pool *runner.Pool, nodes []config.Node, verb string) error {
	for _, n := range nodes {
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

func (d *DummyLauncher) Metadata() launcher.Metadata {
	return launcher.Metadata{
		Name:        "dummy",
		Language:    "Bash",
		Consistency: "N/A",
		Persistency: "N/A",
	}
}
