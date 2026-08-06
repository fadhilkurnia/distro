package main

import (
	"context"
	"log"

	"github.com/joho/godotenv"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/nix"
	"github.com/fadhilkurnia/distro/internal/runner"
	"github.com/fadhilkurnia/distro/internal/tui"

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

	if err := tui.Run(ctx, cfg, pool); err != nil {
		log.Fatalf("tui: %v", err)
	}
}
