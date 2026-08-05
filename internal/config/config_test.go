package config

import "testing"

func TestLoad(t *testing.T) {
	t.Setenv("NUM_OF_NODES", "2")
	t.Setenv("PUBLIC_IP1", "127.0.0.1")
	t.Setenv("PRIVATE_IP1", "127.0.0.1")
	t.Setenv("PUBLIC_IP2", "10.0.0.5")
	t.Setenv("PRIVATE_IP2", "192.168.1.5")
	t.Setenv("SSH_KEY", "testdata/fake_key")
	t.Setenv("REMOTE_USERNAME", "ubuntu")
	t.Setenv("CLIENT_IP", "10.0.0.5")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if len(cfg.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(cfg.Nodes))
	}

	if !cfg.Nodes[0].Local {
		t.Errorf("expected node1 (127.0.0.1) to be Local")
	}

	if cfg.Nodes[1].Local {
		t.Errorf("expected node2 (10.0.0.5) to not be Local")
	}

	if cfg.OutputFile != "data.local.json" {
		t.Errorf("expected default OutputFile, got %q", cfg.OutputFile)
	}
}
