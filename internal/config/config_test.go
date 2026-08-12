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
	t.Setenv("CLIENT_IP", "10.0.0.9")
	t.Setenv("CLIENT_PRIVATE_IP", "192.168.1.9")

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

	if cfg.Client.PublicIP != "10.0.0.9" || cfg.Client.PrivateIP != "192.168.1.9" {
		t.Errorf("expected Client 10.0.0.9/192.168.1.9, got %+v", cfg.Client)
	}
	if cfg.Client.Local {
		t.Errorf("expected Client (10.0.0.9) to not be Local")
	}

	if cfg.WarmupDuration != "60s" {
		t.Errorf("expected default WarmupDuration 60s, got %q", cfg.WarmupDuration)
	}
	if cfg.Duration != "180s" {
		t.Errorf("expected default Duration 180s, got %q", cfg.Duration)
	}
	if cfg.WriteRatio != 0.2 {
		t.Errorf("expected default WriteRatio 0.2, got %v", cfg.WriteRatio)
	}
}

func TestLoadMissingClientPrivateIP(t *testing.T) {
	t.Setenv("NUM_OF_NODES", "1")
	t.Setenv("PUBLIC_IP1", "127.0.0.1")
	t.Setenv("PRIVATE_IP1", "127.0.0.1")
	t.Setenv("SSH_KEY", "testdata/fake_key")
	t.Setenv("REMOTE_USERNAME", "ubuntu")
	t.Setenv("CLIENT_IP", "10.0.0.9")
	// CLIENT_PRIVATE_IP deliberately left unset

	if _, err := Load(); err == nil {
		t.Fatalf("expected an error when CLIENT_PRIVATE_IP is missing, got nil")
	}
}
