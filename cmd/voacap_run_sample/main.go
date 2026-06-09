package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"dxcluster/internal/voacap"
)

func main() {
	home := flag.String("home", voacap.DefaultVOACAPHome, "VOACAP home directory")
	deckPath := flag.String("deck", "", "VOACAP deck path")
	outputName := flag.String("out-name", "gocluster_voacap_sample.out", "VOACAP output file name in the run directory")
	timeout := flag.Duration("timeout", 30*time.Second, "VOACAP process timeout")
	flag.Parse()

	if err := run(*home, *deckPath, *outputName, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(home string, deckPath string, outputName string, timeout time.Duration) error {
	if deckPath == "" {
		deckPath = filepath.Join("internal", "voacap", "testdata", "voacapx.dat")
	}
	deck, err := os.ReadFile(deckPath)
	if err != nil {
		return fmt.Errorf("read deck %s: %w", deckPath, err)
	}
	result, err := voacap.NewRunner(home).Run(context.Background(), voacap.RunRequest{
		Deck:       deck,
		OutputName: outputName,
		Timeout:    timeout,
	})
	if err != nil {
		return err
	}
	fmt.Printf("VOACAP completed in %.0fms\n", result.Elapsed.Seconds()*1000)
	fmt.Printf("Output: %s\n", result.OutputPath)
	fmt.Printf("Bytes: %d\n", len(result.Output))
	return nil
}
