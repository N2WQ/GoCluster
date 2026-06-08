package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"dxcluster/internal/voacap"
)

const maxSunspotReportBytes = 1 << 20

type output struct {
	GeneratedAtUTC                string  `json:"generated_at_utc"`
	Source                        string  `json:"source"`
	LatestObservedAtUTC           string  `json:"latest_observed_at_utc"`
	LatestObservatory             string  `json:"latest_observatory"`
	LatestStation                 string  `json:"latest_station"`
	LatestRawWolfEstimate         int     `json:"latest_raw_wolf_sunspot_estimate"`
	MovingAverageWindowHours      float64 `json:"moving_average_window_hours"`
	MovingAverageObservationCount int     `json:"moving_average_observation_count"`
	MovingAverage                 float64 `json:"moving_average"`
	Note                          string  `json:"note"`
}

func main() {
	url := flag.String("url", voacap.NOAASunspotReportURL, "NOAA sunspot_report.json URL")
	window := flag.Duration("window", 8*time.Hour, "moving-average window")
	timeout := flag.Duration("timeout", 30*time.Second, "HTTP request timeout")
	flag.Parse()

	if err := run(os.Stdout, *url, *window, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(out io.Writer, url string, window time.Duration, timeout time.Duration) error {
	if window <= 0 {
		return fmt.Errorf("window must be positive")
	}
	if timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	body, err := fetch(ctx, url)
	if err != nil {
		return err
	}
	latest, ok, err := voacap.LatestNOAASunspotAverage(body, window)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no usable NOAA sunspot observations")
	}

	payload := output{
		GeneratedAtUTC:                time.Now().UTC().Format(time.RFC3339),
		Source:                        url,
		LatestObservedAtUTC:           latest.Observation.ObservedAtUTC.Format(time.RFC3339),
		LatestObservatory:             latest.Observation.Observatory,
		LatestStation:                 latest.Observation.Station,
		LatestRawWolfEstimate:         latest.Observation.RawWolfEstimate,
		MovingAverageWindowHours:      latest.Window.Hours(),
		MovingAverageObservationCount: latest.ObservationCount,
		MovingAverage:                 latest.Average,
		Note:                          "Derived as 10 * reported active-region count + reported spot count per observatory report; useful as a near-real-time estimate, not an official corrected SSN.",
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(payload)
}

func fetch(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSunspotReportBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxSunspotReportBytes {
		return nil, fmt.Errorf("response from %s exceeds %d bytes", url, maxSunspotReportBytes)
	}
	return body, nil
}
