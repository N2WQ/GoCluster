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
	var (
		configPath = flag.String("config", "data/config", "Path to the cluster configuration file or directory")
		outputPath = flag.String("out", filepath.FromSlash("data/skm_correction/rbnskew.json"), "Destination JSON file")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	count, err := skew.FetchAndWrite(ctx, cfg.Skew.URL, cfg.Skew.MinSpots, *outputPath)
	if err != nil {
		log.Fatalf("failed to fetch skew table: %v", err)
	}

	fmt.Fprintf(os.Stdout, "Wrote %d skew entries to %s\n", count, *outputPath)
}
