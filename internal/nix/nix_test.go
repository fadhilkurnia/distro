package nix

import (
	"os/exec"
	"strings"
	"testing"
)

func TestWrapShape(t *testing.T) {
	cases := []struct {
		script string
		want   string
	}{
		{
			script: "scripts/build.sh",
			want:   `nix-shell 'shell.nix' --run 'chmod +x '\''scripts/build.sh'\'' && ./scripts/build.sh'`,
		},
		{
			script: "./scripts/start.sh",
			want:   `nix-shell 'shell.nix' --run 'chmod +x '\''./scripts/start.sh'\'' && ./scripts/start.sh'`,
		},
	}

	for _, c := range cases {
		if got := wrap(c.script); got != c.want {
			t.Errorf("wrap(%q) =\n  %s\nwant:\n  %s", c.script, got, c.want)
		}
	}
}

func TestWrapParsesAsOneRunArgument(t *testing.T) {
	composed := wrap("scripts/build.sh")

	fakeNixShell := `nix-shell() { for a in "$@"; do echo "ARG:[$a]"; done; }`
	script := fakeNixShell + "\n" + composed

	out, err := exec.Command("bash", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("running composed command: %v (output: %s)", err, out)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var args []string
	for _, l := range lines {
		if strings.HasPrefix(l, "ARG:[") {
			args = append(args, strings.TrimSuffix(strings.TrimPrefix(l, "ARG:["), "]"))
		}
	}

	wantArgs := []string{
		"shell.nix",
		"--run",
		"chmod +x 'scripts/build.sh' && ./scripts/build.sh",
	}

	if len(args) != len(wantArgs) {
		t.Fatalf("got %d args %q, want %d args %q", len(args), args, len(wantArgs), wantArgs)
	}
	for i, want := range wantArgs {
		if args[i] != want {
			t.Errorf("arg[%d] = %q, want %q", i, args[i], want)
		}
	}
}
