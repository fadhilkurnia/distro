package main

import (
	"context"
	"log"

	"github.com/joho/godotenv"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"

	// Blank-imported so its init() runs and registers itself into the
	// registry. main.go blank-imports each sut/<protocol> package, and
	// adding a new protocol never requires touching the registry itself.
	_ "github.com/fadhilkurnia/distro/internal/testutil"
	_ "github.com/fadhilkurnia/distro/sut/ailidani.paxi"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("no .env file loaded (%v) - relying on already-set environment variables", err)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("loaded config: %d node(s), client=%s, output=%s", len(cfg.Nodes), cfg.ClientIP, cfg.OutputFile)

	pool, err := runner.NewPool(cfg.SSH)
	if err != nil {
		log.Fatalf("runner: %v", err)
	}
	defer func() {
		if err := pool.Close(); err != nil {
			log.Printf("runner: error closing connections: %v", err)
		}
	}()

	ctx := context.Background()

	for _, n := range cfg.Nodes {
		r, err := pool.For(n, ".")
		if err != nil {
			log.Fatalf("runner: %v", err)
		}
		if err := nix.CheckAvailable(ctx, r); err != nil {
			log.Fatalf("nix: %v", err)
		}
	}
	log.Printf("nix-shell available on all %d node(s)", len(cfg.Nodes))

	// Manual test: find the paxos/baseline instance from the full
	// catalog and construct a Launcher for it. This is standing in for
	// what the TUI's picker will do once it exists.
	var chosen *registry.Instance
	for _, inst := range registry.GetInstances() {
		if inst.ProjectName == "ailidani.paxi" && inst.Specification.Protocol == "paxos" && inst.Version.Name == "baseline" {
			chosen = &inst
			break
		}
	}
	if chosen == nil {
		log.Fatalf("no matching instance found in catalog")
	}

	l, err := registry.GetLauncher(*chosen)
	if err != nil {
		log.Fatalf("registry: %v", err)
	}

	log.Printf("[%s] building...", l.ProjectName())
	if err := l.Build(ctx, pool, cfg.Nodes); err != nil {
		log.Fatalf("build failed: %v", err)
	}

	log.Printf("[%s] starting...", l.ProjectName())
	if err := l.Start(ctx, pool, cfg.Nodes); err != nil {
		log.Fatalf("start failed: %v", err)
	}

	log.Printf("[%s] stopping...", l.ProjectName())
	if err := l.Stop(ctx, pool, cfg.Nodes); err != nil {
		log.Fatalf("stop failed: %v", err)
	}

	log.Println("done")
}
