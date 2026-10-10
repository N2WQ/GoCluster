// Program prop_report publishes daily observations and optional narrative.
// Hard generation/reporting failures reach automation; request fallback stays
// inside Generate, which owns the existing JSON-then-Markdown partial effects.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"dxcluster/internal/propreport"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	dateFlag := flag.String("date", "", "Date to analyze (YYYY-MM-DD, defaults to today UTC)")
	logPathFlag := flag.String("log", "", "Path to archived daily propagation log (defaults to data/logs/propagation/<DD-Mon-YYYY>.log)")
	jsonOutFlag := flag.String("json-out", "", "Output JSON summary path (defaults to data/reports/prop-YYYY-MM-DD.json)")
	reportOutFlag := flag.String("report-out", "", "Output report path (defaults to data/reports/prop-YYYY-MM-DD.md)")
	configDirFlag := flag.String("config-dir", filepath.Join("data", "config"), "Config directory for model context")
	pathConfigFlag := flag.String("path-config", "", "Deprecated compatibility alias for -config-dir; accepts a config directory or path_reliability.yaml")
	openAIConfigFlag := flag.String("openai-config", filepath.Join("data", "config", "openai.yaml"), "OpenAI config for narrative generation")
	noLLMFlag := flag.Bool("no-llm", false, "Disable OpenAI narrative generation")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.LUTC)

	date := time.Now().UTC()
	if *dateFlag != "" {
		parsed, err := time.Parse("2006-01-02", *dateFlag)
		if err != nil {
			return fmt.Errorf("invalid date %q: %w", *dateFlag, err)
		}
		date = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := propreport.Generate(ctx, propreport.Options{
		Date:             date,
		LogPath:          *logPathFlag,
		JSONOut:          *jsonOutFlag,
		ReportOut:        *reportOutFlag,
		ConfigDir:        *configDirFlag,
		PathConfigPath:   *pathConfigFlag,
		OpenAIConfigPath: *openAIConfigFlag,
		NoLLM:            *noLLMFlag,
		Logger:           log.Default(),
	})
	if err != nil {
		return fmt.Errorf("prop report generation failed: %w", err)
	}

	_, jsonReportErr := fmt.Fprintf(os.Stdout, "Wrote JSON summary: %s\n", result.JSONPath)
	_, reportErr := fmt.Fprintf(os.Stdout, "Wrote report: %s\n", result.ReportPath)
	if err := errors.Join(jsonReportErr, reportErr); err != nil {
		return fmt.Errorf("prop report output failed: %w", err)
	}
	return nil
}
