package shellquote

import (
	"os/exec"
	"testing"
)

func TestQuoteRoundTrip(t *testing.T) {
	cases := []string{
		"simple",
		"has spaces",
		"has'a'quote",
		"'leading and trailing quotes'",
		"",
		"/tmp/run 1/config.json",
		`mix of "double" and 'single' quotes`,
	}

	for _, want := range cases {
		cmd := exec.Command("sh", "-c", "printf '%s' "+Quote(want))
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Quote(%q): shell rejected output: %v (output: %s)", want, err, out)
		}
		if got := string(out); got != want {
			t.Errorf("Quote(%q) round-tripped to %q, want %q", want, got, want)
		}
	}
}
