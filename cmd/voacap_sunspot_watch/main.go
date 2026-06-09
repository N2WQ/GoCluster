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
	"time"

	"dxcluster/internal/voacap"
)

const maxSunspotReportBytes = 1 << 20

type state struct {
	ETag                     string    `json:"etag,omitempty"`
	LastModified             string    `json:"last_modified,omitempty"`
	LastObservedAtUTC        time.Time `json:"last_observed_at_utc,omitempty"`
	LastRawSSN               int       `json:"last_raw_ssn,omitempty"`
	EWMA                     float64   `json:"ewma,omitempty"`
	EWMAInitialized          bool      `json:"ewma_initialized,omitempty"`
	LastVOACAPSSN            float64   `json:"last_voacap_ssn,omitempty"`
	LastVOACAPSSNInitialized bool      `json:"last_voacap_ssn_initialized,omitempty"`
}

type fetchResult struct {
	body         []byte
	notModified  bool
	etag         string
	lastModified string
}

func main() {
	url := flag.String("url", voacap.NOAASunspotReportURL, "NOAA sunspot_report.json URL")
	interval := flag.Duration("interval", 30*time.Minute, "fetch interval")
	halfLife := flag.Duration("half-life", 8*time.Hour, "EWMA half-life")
	threshold := flag.Float64("threshold", 0.12, "relative VOACAP recompute threshold")
	timeout := flag.Duration("timeout", 30*time.Second, "HTTP request timeout")
	statePath := flag.String("state", filepath.Join(".tmp", "voacap-ssn-watch-state.json"), "state file path, or empty to disable persistence")
	once := flag.Bool("once", false, "fetch once and exit")
	flag.Parse()

	if err := run(*url, *interval, *halfLife, *threshold, *timeout, *statePath, *once); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(url string, interval time.Duration, halfLife time.Duration, threshold float64, timeout time.Duration, statePath string, once bool) error {
	if interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	if halfLife <= 0 {
		return fmt.Errorf("half-life must be positive")
	}
	if threshold < 0 {
		return fmt.Errorf("threshold must be non-negative")
	}
	if timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	st, err := loadState(statePath)
	if err != nil {
		return err
	}
	printHeader()
	for {
		if err := poll(url, timeout, halfLife, threshold, statePath, &st); err != nil {
			fmt.Fprintf(os.Stderr, "%s error: %v\n", time.Now().UTC().Format(time.RFC3339), err)
		}
		if once {
			return nil
		}
		time.Sleep(interval)
	}
}

func poll(url string, timeout time.Duration, halfLife time.Duration, threshold float64, statePath string, st *state) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := fetch(ctx, url, st.ETag, st.LastModified)
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
		printUnchanged(now, *st)
		return saveState(statePath, *st)
	}

	series, err := voacap.ParseNOAASunspotReport(result.body)
	if err != nil {
		return err
	}
	if len(series) == 0 {
		return errors.New("no usable NOAA sunspot observations")
	}
	latest := series[len(series)-1]
	if st.EWMAInitialized && !latest.ObservedAtUTC.After(st.LastObservedAtUTC) {
		printUnchanged(now, *st)
		return saveState(statePath, *st)
	}

	ewma, err := voacap.UpdateSunspotEWMA(st.EWMA, st.LastObservedAtUTC, st.EWMAInitialized, latest, halfLife)
	if err != nil {
		return err
	}
	roundedSSN, err := voacap.RoundedSunspotSSN(ewma.Average)
	if err != nil {
		return err
	}
	trigger, delta, err := voacap.ShouldRecomputeVOACAP(float64(roundedSSN), st.LastVOACAPSSN, st.LastVOACAPSSNInitialized, threshold)
	if err != nil {
		return err
	}

	marker := ""
	if trigger {
		marker = "*"
		st.LastVOACAPSSN = float64(roundedSSN)
		st.LastVOACAPSSNInitialized = true
	}
	st.LastObservedAtUTC = latest.ObservedAtUTC
	st.LastRawSSN = latest.RawWolfEstimate
	st.EWMA = ewma.Average
	st.EWMAInitialized = ewma.Initialized

	printSample(now, latest, roundedSSN, delta, marker, "new")
	return saveState(statePath, *st)
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

func loadState(path string) (state, error) {
	if path == "" {
		return state{}, nil
	}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return state{}, nil
	}
	if err != nil {
		return state{}, err
	}
	var st state
	if err := json.Unmarshal(body, &st); err != nil {
		return state{}, err
	}
	return st, nil
}

func saveState(path string, st state) error {
	if path == "" {
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

func printHeader() {
	fmt.Println("fetched_at_utc observed_at_utc raw_ssn ewma_ssn delta recompute status")
}

func printSample(now time.Time, observation voacap.SunspotObservation, ewmaSSN int, delta float64, marker string, status string) {
	fmt.Printf("%s %s %d %d %.4f %s %s\n",
		now.Format(time.RFC3339),
		observation.ObservedAtUTC.Format(time.RFC3339),
		observation.RawWolfEstimate,
		ewmaSSN,
		delta,
		marker,
		status,
	)
}

func printUnchanged(now time.Time, st state) {
	observedAt := "-"
	if !st.LastObservedAtUTC.IsZero() {
		observedAt = st.LastObservedAtUTC.Format(time.RFC3339)
	}
	rawSSN := "-"
	if st.LastRawSSN != 0 {
		rawSSN = fmt.Sprintf("%d", st.LastRawSSN)
	}
	roundedSSN := 0
	if st.EWMAInitialized {
		if rounded, err := voacap.RoundedSunspotSSN(st.EWMA); err == nil {
			roundedSSN = rounded
		}
	}
	fmt.Printf("%s %s %s %d 0.0000  unchanged\n",
		now.Format(time.RFC3339),
		observedAt,
		rawSSN,
		roundedSSN,
	)
}
