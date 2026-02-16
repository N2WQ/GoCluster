package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"dxcluster/internal/propreport"
)

func main() {
	dateFlag := flag.String("date", "", "Date to analyze (YYYY-MM-DD, defaults to today UTC)")
	logPathFlag := flag.String("log", "", "Path to daily log (defaults to data/logs/<DD-Mon-YYYY>.log)")
	jsonOutFlag := flag.String("json-out", "", "Output JSON summary path (defaults to data/reports/prop-YYYY-MM-DD.json)")
	reportOutFlag := flag.String("report-out", "", "Output report path (defaults to data/reports/prop-YYYY-MM-DD.md)")
	pathConfigFlag := flag.String("path-config", filepath.Join("data", "config", "path_reliability.yaml"), "Path reliability config for model context")
	openAIConfigFlag := flag.String("openai-config", filepath.Join("data", "config", "openai.yaml"), "OpenAI config for narrative generation")
	noLLMFlag := flag.Bool("no-llm", false, "Disable OpenAI narrative generation")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.LUTC)

	date := time.Now().UTC()
	if *dateFlag != "" {
		parsed, err := time.Parse("2006-01-02", *dateFlag)
		if err != nil {
			log.Fatalf("Invalid date %q: %v", *dateFlag, err)
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
		PathConfigPath:   *pathConfigFlag,
		OpenAIConfigPath: *openAIConfigFlag,
		NoLLM:            *noLLMFlag,
		Logger:           log.Default(),
	})
	if err != nil {
		log.Printf("prop report generation failed: %v", err)
		return
	}

	fmt.Printf("Wrote JSON summary: %s\n", result.JSONPath)
	fmt.Printf("Wrote report: %s\n", result.ReportPath)
}
