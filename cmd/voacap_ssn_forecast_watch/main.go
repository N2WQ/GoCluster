package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/internal/voacap"
)

const maxSunspotReportBytes = 1 << 20

type fetchResult struct {
	body         []byte
	notModified  bool
	etag         string
	lastModified string
}

func main() {
	configPath := flag.String("config", filepath.Join("data", "config", "voacap_experiment.yaml"), "VOACAP experiment YAML config path")
	once := flag.Bool("once", false, "fetch once and exit")
	flag.Parse()

	if err := run(*configPath, *once); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(configPath string, once bool) error {
	cfg, err := voacap.LoadExperimentConfig(configPath)
	if err != nil {
		return fmt.Errorf("load experiment config: %w", err)
	}
	st, err := loadState(cfg.StatePath)
	if err != nil {
		return err
	}

	printHeader()
	for {
		pollErr := poll(cfg, &st)
		if pollErr != nil {
			fmt.Fprintf(os.Stderr, "%s error: %v\n", time.Now().UTC().Format(time.RFC3339), pollErr)
		}
		if once {
			return pollErr
		}
		time.Sleep(cfg.SSNFetchInterval())
	}
}

func poll(cfg voacap.ExperimentConfig, st *voacap.ForecastState) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.SSNRequestTimeout())
	defer cancel()

	result, err := fetch(ctx, voacap.NOAASunspotReportURL, st.ETag, st.LastModified)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if result.etag != "" {
		st.ETag = result.etag
	}
	if result.lastModified != "" {
		st.LastModified = result.lastModified
	}

	if result.notModified {
		decision, err := voacap.ApplyFetchUnchanged(st, cfg)
		if err != nil {
			return err
		}
		return completeDecision(cfg, st, decision, now)
	}

	series, err := voacap.ParseNOAASunspotReport(result.body)
	if err != nil {
		return err
	}
	if len(series) == 0 {
		return errors.New("no usable NOAA sunspot observations")
	}

	decision, err := voacap.ApplySunspotObservation(st, series[len(series)-1], cfg)
	if err != nil {
		return err
	}
	return completeDecision(cfg, st, decision, now)
}

func completeDecision(cfg voacap.ExperimentConfig, st *voacap.ForecastState, decision voacap.ForecastDecision, now time.Time) error {
	outputPath := ""
	if decision.ForecastRequired {
		forecastSSN, err := voacap.RoundedSunspotSSN(decision.EWMA)
		if err != nil {
			return err
		}
		result, err := runForecast(context.Background(), cfg, forecastSSN, now)
		outputPath = result.OutputPath
		if err != nil {
			failure := voacap.MarkForecastFailure(st, err, now)
			printDecision(now, failure, *st, outputPath)
			if saveErr := saveState(cfg.StatePath, *st); saveErr != nil {
				return errors.Join(err, fmt.Errorf("save forecast failure state: %w", saveErr))
			}
			return err
		}
		predictions, err := voacap.ParsePredictions(result.Output)
		if err != nil {
			failure := voacap.MarkForecastFailure(st, err, now)
			printDecision(now, failure, *st, outputPath)
			if saveErr := saveState(cfg.StatePath, *st); saveErr != nil {
				return errors.Join(err, fmt.Errorf("save forecast parse failure state: %w", saveErr))
			}
			return fmt.Errorf("parse VOACAP predictions: %w", err)
		}
		success, err := voacap.MarkForecastSuccess(st, float64(forecastSSN), outputPath, forecastOutputSize(outputPath), now)
		if err != nil {
			return err
		}
		printDecision(now, success, *st, outputPath)
		printPredictionHeader()
		printPredictions(predictions)
	} else {
		printDecision(now, decision, *st, outputPath)
	}
	return saveState(cfg.StatePath, *st)
}

func runForecast(ctx context.Context, cfg voacap.ExperimentConfig, smoothedSSN int, now time.Time) (voacap.RunResult, error) {
	deck, err := voacap.BuildExperimentDeck(cfg, float64(smoothedSSN), now)
	if err != nil {
		return voacap.RunResult{}, err
	}
	outputName := fmt.Sprintf("%s_%s.out", cfg.OutputNamePrefix, now.UTC().Format("20060102T150405Z"))
	result, err := voacap.NewRunner(cfg.VOACAPHome).Run(ctx, voacap.RunRequest{
		Deck:       deck,
		OutputName: outputName,
		Timeout:    cfg.VOACAPTimeout(),
	})
	return result, err
}

func fetch(ctx context.Context, url string, etag string, lastModified string) (fetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fetchResult{}, err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastModified != "" {
		req.Header.Set("If-Modified-Since", lastModified)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fetchResult{}, err
	}
	defer resp.Body.Close()
	result := fetchResult{
		etag:         resp.Header.Get("ETag"),
		lastModified: resp.Header.Get("Last-Modified"),
	}
	if resp.StatusCode == http.StatusNotModified {
		result.notModified = true
		return result, nil
	}
	if resp.StatusCode != http.StatusOK {
		return fetchResult{}, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSunspotReportBytes+1))
	if err != nil {
		return fetchResult{}, err
	}
	if len(body) > maxSunspotReportBytes {
		return fetchResult{}, fmt.Errorf("response from %s exceeds %d bytes", url, maxSunspotReportBytes)
	}
	result.body = body
	return result, nil
}

func loadState(path string) (voacap.ForecastState, error) {
	if strings.TrimSpace(path) == "" {
		return voacap.ForecastState{}, nil
	}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return voacap.ForecastState{}, nil
	}
	if err != nil {
		return voacap.ForecastState{}, err
	}
	var st voacap.ForecastState
	if err := json.Unmarshal(body, &st); err != nil {
		return voacap.ForecastState{}, err
	}
	return st, nil
}

func saveState(path string, st voacap.ForecastState) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), 0o644)
}

func forecastOutputSize(path string) int {
	if strings.TrimSpace(path) == "" {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

func printHeader() {
	fmt.Println("fetched_at_utc observed_at_utc raw_ssn ewma_ssn delta transition marker forecast_output")
}

func printPredictionHeader() {
	fmt.Println("prediction hour_utc frequency_mhz voacap_snr_dbhz ft8_snr_db reliability")
}

func printPredictions(records []voacap.PredictionRecord) {
	for _, record := range records {
		reliability := "-"
		if record.HasReliability {
			reliability = fmt.Sprintf("%.2f", record.Reliability)
		}
		fmt.Printf("prediction %02d %.1f %d %d %s\n",
			record.HourUTC,
			record.FrequencyMHz,
			record.VOACAPSNRDBHz,
			record.FT8SNRDB,
			reliability,
		)
	}
}

func printDecision(now time.Time, decision voacap.ForecastDecision, st voacap.ForecastState, outputPath string) {
	observedAt := "-"
	if !st.LastObservedAtUTC.IsZero() {
		observedAt = st.LastObservedAtUTC.Format(time.RFC3339)
	}
	rawSSN := "-"
	if st.LastRawSSN != 0 {
		rawSSN = fmt.Sprintf("%d", st.LastRawSSN)
	}
	marker := ""
	if decision.ForecastRequired ||
		decision.Transition == voacap.ForecastTransitionSuccess ||
		decision.Transition == voacap.ForecastTransitionFailure {
		marker = "*"
	}
	if outputPath == "" {
		outputPath = "-"
	}
	roundedSSN := 0
	if st.EWMAInitialized {
		if rounded, err := voacap.RoundedSunspotSSN(st.EWMA); err == nil {
			roundedSSN = rounded
		}
	}
	fmt.Printf("%s %s %s %d %.4f %s %s %s\n",
		now.Format(time.RFC3339),
		observedAt,
		rawSSN,
		roundedSSN,
		decision.Delta,
		decision.Transition,
		marker,
		outputPath,
	)
}
