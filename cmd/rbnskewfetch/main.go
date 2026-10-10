// Program rbnskewfetch writes the configured absolute-skew selection to JSON.
// Fetch, parsing and persistence failures must be visible to operator automation.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"dxcluster/config"
	"dxcluster/skew"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath = flag.String("config", "data/config", "Path to the cluster configuration directory")
		outputPath = flag.String("out", filepath.FromSlash("data/skm_correction/rbnskew.json"), "Destination JSON file")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	count, err := skew.FetchAndWrite(ctx, cfg.Skew.URL, cfg.Skew.MinAbsSkew, *outputPath)
	if err != nil {
		return fmt.Errorf("failed to fetch skew table: %w", err)
	}

	if _, err := fmt.Fprintf(os.Stdout, "Wrote %d skew entries to %s\n", count, *outputPath); err != nil {
		return fmt.Errorf("failed to report skew table: %w", err)
	}
	return nil
}
