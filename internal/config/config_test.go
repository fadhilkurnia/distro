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
	t.Setenv("CLIENT_PUBLIC_IP", "10.0.0.9")
	t.Setenv("CLIENT_PRIVATE_IP", "192.168.1.9")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	// Nodes now holds 2 replicas + 1 client = 3 entries.
	if len(cfg.Nodes) != 3 {
		t.Fatalf("expected 3 nodes (2 replicas + client), got %d", len(cfg.Nodes))
	}

	replicas := ReplicaNodes(cfg.Nodes)
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replica nodes, got %d", len(replicas))
	}

	if !replicas[0].Local {
		t.Errorf("expected node1 (127.0.0.1) to be Local")
	}
	if replicas[1].Local {
		t.Errorf("expected node2 (10.0.0.5) to not be Local")
	}
	for _, n := range replicas {
		if n.Type != NodeTypeReplica {
			t.Errorf("expected replica node %s to have Type NodeTypeReplica", n.ID)
		}
	}

	if cfg.OutputFile != "data.local.json" {
		t.Errorf("expected default OutputFile, got %q", cfg.OutputFile)
	}

	client := ClientNode(cfg.Nodes)
	if client.PublicIP != "10.0.0.9" || client.PrivateIP != "192.168.1.9" {
		t.Errorf("expected Client 10.0.0.9/192.168.1.9, got %+v", client)
	}
	if client.Local {
		t.Errorf("expected Client (10.0.0.9) to not be Local")
	}
	if client.Type != NodeTypeClient {
		t.Errorf("expected Client node to have Type NodeTypeClient")
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
	t.Setenv("CLIENT_PUBLIC_IP", "10.0.0.9")
	// CLIENT_PRIVATE_IP deliberately left unset

	if _, err := Load(); err == nil {
		t.Fatalf("expected an error when CLIENT_PRIVATE_IP is missing, got nil")
	}
}

func TestValidateRequiresExactlyOneClient(t *testing.T) {
	cfg := &Config{
		Nodes: []Node{
			{ID: "node1", PublicIP: "1.1.1.1", PrivateIP: "10.0.0.1", Type: NodeTypeReplica},
		},
		SSH: SSHConfig{KeyPath: "/tmp/key", Username: "ubuntu"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error when no client node is present, got nil")
	}
}
