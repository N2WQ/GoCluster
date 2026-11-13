package pathreliability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/internal/voacap"
	"github.com/cockroachdb/pebble"
)

func TestVOACAPSunspotSourceWorkerPipeline(t *testing.T) { testSunspotSourceWorkerPipeline(t, "") }
func TestVOACAPSunspotSourceNativePipeline(t *testing.T) {
	home := os.Getenv("GOCLUSTER_SSN_V6_NATIVE_HOME")
	if home == "" {
		t.Skip("set GOCLUSTER_SSN_V6_NATIVE_HOME to an isolated native installation")
	}
	// Refuse the conventional live installation even if an opt-in is mistyped.
	resolved, err := filepath.Abs(home)
	if err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(filepath.Clean(resolved), filepath.Clean(voacap.DefaultVOACAPHome)) {
		t.Fatal("native source test requires an isolated installation")
	}
	testSunspotSourceWorkerPipeline(t, resolved)
}
func testSunspotSourceWorkerPipeline(t *testing.T, home string) {
	t.Helper()
	var representation atomic.Value
	first := `{"time_tag":"2026-06-08T20:00:00","Station":1,"Observatory":"HOL","Obsid":307,"Numspot":90,"Report_Status":2,"Type":"spt"}`
	next := `{"time_tag":"2026-06-10T20:00:00","Station":1,"Observatory":"HOL","Obsid":307,"Numspot":92,"Report_Status":3,"Type":"spt"}`
	representation.Store("[" + first + "]")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, representation.Load()) }))
	defer server.Close()
	monitor, err := voacap.NewSunspotMonitor(voacap.SunspotMonitorConfig{URL: server.URL, FetchInterval: 30 * time.Minute, RequestTimeout: time.Second, EWMAHalfLife: 48 * time.Hour, StaleAfter: 48 * time.Hour, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 10, 21, 10, 0, 0, time.UTC)
	if err = monitor.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.ForecastHours = 24
	cfg.VOACAPFallback.CacheTTLSeconds = 86400
	cfg.VOACAPFallback.DelaySeconds = 0
	if home != "" {
		cfg.VOACAPFallback.VOACAPHome = home
		cfg.VOACAPFallback.VOACAPTimeoutSeconds = 120
	}
	real := NewVOACAPRunnerClosedForecaster(cfg.VOACAPFallback)
	type evidence struct {
		job  VOACAPClosedJob
		deck string
	}
	observed := make(chan evidence, 2)
	forecaster := fakeClosedForecaster{fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
		if home != "" {
			result, err := real.ForecastClosed(ctx, job)
			if err != nil {
				return result, err
			}
			deck, err := os.ReadFile(filepath.Join(home, "run", voacap.DefaultVOACAPInputName))
			if err != nil {
				return result, err
			}
			observed <- evidence{job, string(deck)}
			return result, nil
		}
		deck, err := real.buildDeck(job, voacapDeckReceive)
		if err != nil {
			return VOACAPClosedForecast{}, err
		}
		observed <- evidence{job, string(deck)}
		return v4FullForecast(job, 24), nil
	}}
	f := newTestClosedFallback(t, cfg, forecaster, monitor)
	f.now = func() time.Time { return now }
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	if _, err = f.AttachForecastCacheStore(store, now); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.Start(ctx)
	defer func() { cancel(); f.Wait() }()
	req := testClosedRequest()
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckQueued {
		t.Fatalf("first source job %+v", got)
	}
	waitForSourceWorker(t, f, 1)
	firstJob := <-observed
	if firstJob.job.SSN != 100 || !strings.Contains(firstJob.deck, "SUNSPOT    100.") {
		t.Fatalf("seed deck: %+v %s", firstJob.job, firstJob.deck)
	}
	representation.Store("[" + next + "," + first + "]")
	if err = monitor.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	input, ok := monitor.CurrentSolarInput(now)
	if !ok || input.SSN != 101 || input.Provenance.LatestValue != 102 || input.Provenance.SmoothedValue != 101 {
		t.Fatalf("literal48h input %+v", input)
	}
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 100 {
		t.Fatalf("previous source forecast bridge %+v", got)
	}
	waitForSourceWorker(t, f, 2)
	secondJob := <-observed
	if secondJob.job.SSN != 101 || secondJob.job.SolarInput != input || !strings.Contains(secondJob.deck, "SUNSPOT    101.") {
		t.Fatalf("one-unit update did not reach actual deck: %+v %s", secondJob.job, secondJob.deck)
	}
	loaded, stats, err := store.loadCurrent(now, cfg.VOACAPFallback)
	if err != nil || stats.Loaded != 1 || len(loaded) != 1 {
		t.Fatalf("persisted current source %v %+v", err, stats)
	}
	entry := loaded[0].entry
	if entry.metadata.solar != input || entry.forecast.WindowStartUTC != now.Truncate(time.Hour) || entry.expiresNS != now.Truncate(time.Hour).Add(24*time.Hour).UnixNano() {
		t.Fatalf("source provenance/expiry lost %+v", entry)
	}
	t.Logf("feed -> monitor -> worker -> %sdeck100 then101 -> generation2 persisted count provenance;24-hour expiry=%s", map[bool]string{true: "native ", false: ""}[home != ""], time.Unix(0, entry.expiresNS).UTC())
}
func waitForSourceWorker(t *testing.T, f *VOACAPClosedFallback, want uint64) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if f.Snapshot().WorkerDurationCount >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("worker timed out: %+v %+v", f.Snapshot(), f.StatsSnapshot())
		case <-tick.C:
		}
	}
}

func TestVOACAPSunspotSourceKeepsLegacyForecastAndExpiry(t *testing.T) {
	now := time.Date(2026, 6, 10, 21, 10, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.ForecastHours = 24
	cfg.VOACAPFallback.CacheTTLSeconds = 86400
	cfg.VOACAPFallback.DelaySeconds = 0
	provider := &v4SolarProvider{}
	provider.set(100, false)
	f := newTestClosedFallback(t, cfg, fakeClosedForecaster{fn: func(context.Context, VOACAPClosedJob) (VOACAPClosedForecast, error) {
		t.Error("same SSN or missing compatible input scheduled a job")
		return VOACAPClosedForecast{}, nil
	}}, provider)
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	req := testClosedRequest()
	key := f.cacheKey(req)
	entry := v4Entry(f, req, now.Truncate(time.Hour), now.Add(-time.Minute), 100)
	record := diskRecordFromForecastCache(key, entry)
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]json.RawMessage
	if err = json.Unmarshal(wire["metadata"], &metadata); err != nil {
		t.Fatal(err)
	}
	// Frozen old generation2 provenance has truthful SFU keys and an actual SSN
	// independent of the new runtime source. No migration resets its expiry.
	metadata["solar_input"] = json.RawMessage(`{"ssn":100,"provenance":{"recent_observed_unix":1780948800,"mean_observed_unix":1780948800,"input_updated_unix":1780952400,"ewma_half_life_seconds":172800,"latest_noon_flux_sfu":140,"recent_ewma_flux_sfu":130,"published_mean_flux_sfu":110,"blended_flux_sfu":120,"recent_weight":0.5,"source_id":1,"units_id":1,"conversion_id":1,"flags":7}}`)
	wire["metadata"], err = json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.db.Set(voacapForecastCacheKeyBytes(key), encoded, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if stats, err := f.AttachForecastCacheStore(store, now); err != nil || stats.Loaded != 1 {
		t.Fatalf("legacy restore %+v %v", stats, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.Start(ctx)
	defer func() { cancel(); f.Wait() }()
	for _, known := range []bool{false, true} {
		provider.mu.Lock()
		provider.known = known
		provider.input = voacap.SolarInputSnapshot{SSN: 100, Provenance: voacap.SolarInputProvenance{RecentObservedUnix: now.Unix(), InputUpdatedUnix: now.Unix(), EWMAHalfLifeSeconds: 172800, LatestValue: 100, SmoothedValue: 100, SourceID: 2, UnitsID: 2, ConversionID: 2, Flags: 1}}
		provider.mu.Unlock()
		got := f.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 100 {
			t.Fatalf("source transition withdrew forecast %+v", got)
		}
		if f.statsQueued.Load() != 0 || f.Snapshot().InflightEntries != 0 {
			t.Fatal("same SSN source transition queued work")
		}
	}
	f.mu.Lock()
	restored := f.cache[key]
	f.mu.Unlock()
	if restored.expiresNS != entry.expiresNS || restored.metadata.solar.Provenance.SourceID != 1 || restored.metadata.solar.Provenance.EffectiveValue != 120 {
		t.Fatalf("legacy origin mutated %+v", restored)
	}
	cancel()
	f.Wait()
	if got := f.CheckForecastDetailed(req, time.Unix(0, entry.expiresNS)); got.Status == VOACAPForecastCheckReady {
		t.Fatal("legacy forecast served at original expiry")
	}
}
