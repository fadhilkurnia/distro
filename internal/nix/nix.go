package nix

import (
	"context"
	"fmt"
	"strings"

	"github.com/fadhilkurnia/distro/internal/runner"
	"github.com/fadhilkurnia/distro/internal/shellquote"
)

// Name of nix shell file.
// Ex: sut/ailidani.paxi/shell.nix
const shellFile = "shell.nix"

// Returns script after it is wrapped inside nix-shell.
// Uses impure nix to inherit ambient environment variables.
// Ex: `nix-shell shell.nix --run '...'`
func wrap(script string) string {
	invocation := script
	if !strings.HasPrefix(script, "/") && !strings.HasPrefix(script, "./") {
		invocation = "./" + script
	}
	inner := fmt.Sprintf("chmod +x %s && %s", shellquote.Quote(script), invocation)

	return fmt.Sprintf("nix-shell %s --run %s", shellquote.Quote(shellFile), shellquote.Quote(inner))
}

func Run(ctx context.Context, r runner.Runner, script string, env map[string]string) error {
	return r.Run(ctx, wrap(script), env)
}

func Stream(ctx context.Context, r runner.Runner, script string, env map[string]string) (*runner.StreamHandle, error) {
	return r.Stream(ctx, wrap(script), env)
}

// Check if the target node machine has nix-shell.
// Returns non-nil error if nix-shell isn't found.
func CheckAvailable(ctx context.Context, r runner.Runner) error {
	if err := r.Run(ctx, "command -v nix-shell", nil); err != nil {
		return fmt.Errorf("nix: nix-shell not available on %s: %w", r.Host(), err)
	}
	return nil
}
