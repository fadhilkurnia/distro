package main

import (
	"context"
	"log"

	"github.com/joho/godotenv"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/registry"
	"github.com/fadhilkurnia/distro/internal/runner"

	// Blank-imported so its init() runs and registers "dummy" into the
	// registry. main.go blank-imports each sut/<protocol> package, and
	// adding a new protocol never requires touching the registry itself
	_ "github.com/fadhilkurnia/distro/internal/testutil"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Printf("no .env file loaded (%v) - relying on already-set environment variables", err)
	}

	// Load config from .env and validates the variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("loaded config: %d node(s), client=%s, output=%s", len(cfg.Nodes), cfg.ClientIP, cfg.OutputFile)

	// Create Runner pool to all nodes
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

	// Check if every node has nix-shell available.
	// A node missing this will cause hard failure in distrobench
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

	// Test dummy
	l, err := registry.Get("dummy")
	if err != nil {
		log.Fatalf("registry: %v", err)
	}

	log.Printf("[%s] building...", l.Name())
	if err := l.Build(ctx, pool, cfg.Nodes); err != nil {
		log.Fatalf("build failed: %v", err)
	}

	log.Printf("[%s] starting...", l.Name())
	if err := l.Start(ctx, pool, cfg.Nodes); err != nil {
		log.Fatalf("start failed: %v", err)
	}

	log.Printf("[%s] stopping...", l.Name())
	if err := l.Stop(ctx, pool, cfg.Nodes); err != nil {
		log.Fatalf("stop failed: %v", err)
	}

	log.Println("done")
}
