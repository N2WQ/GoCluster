package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/pathreliability"
)

func TestClusterRuntimePropPriorFeedsDisabledAllocatesNothing(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rt := &clusterRuntime{
		cfg: &config.Config{
			PathReliability: pathreliability.Config{
				PropPriorFeeds: &pathreliability.PropPriorFeedsConfig{
					Enabled:                   false,
					Hp30PrimaryURL:            srv.URL + "/gfz",
					Hp30FallbackURL:           srv.URL + "/kp",
					SFIPrimaryURL:             srv.URL + "/f107",
					SFIFallbackURL:            srv.URL + "/gfz",
					RTSWMagURL:                srv.URL + "/mag",
					RTSWWindURL:               srv.URL + "/wind",
					DstPrimaryURL:             srv.URL + "/kyoto",
					DstFallbackURL:            srv.URL + "/geospace",
					Hp30PollIntervalSeconds:   600,
					SFIPollIntervalSeconds:    1800,
					RTSWPollIntervalSeconds:   300,
					DstPollIntervalSeconds:    900,
					RequestTimeoutSeconds:     5,
					UserAgent:                 "GoCluster-Test/1.0",
					MaxBodyBytesStandard:      1048576,
					MaxBodyBytesRTSW:          8388608,
					MaxBodyBytesGeospace:      4194304,
					Hp30StaleAfterSeconds:     5400,
					SFIStaleAfterSeconds:      129600,
					DstStaleAfterSeconds:      10800,
					FailoverConsecutiveErrors: 3,
				},
			},
		},
	}
	if !rt.loadPropPriorFeedsConfig() {
		t.Fatalf("disabled prop_prior_feeds failed load")
	}
	if rt.propPriorPoller != nil || rt.propPriorWeather != nil {
		t.Fatalf("disabled prop_prior_feeds must leave store and poller nil")
	}
	rt.ctx, rt.cancel = context.WithCancel(context.Background())
	rt.close()
	if hits.Load() != 0 {
		t.Fatalf("disabled prop_prior_feeds made %d requests", hits.Load())
	}
}

func TestClusterRuntimePropPriorFeedsEnabledStartsAndStopsCleanly(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/gfz":
			_, _ = w.Write([]byte(`{"datetime":["2026-11-27T15:00:00Z"],"Hp30":[1.333]}`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()

	rt := &clusterRuntime{
		cfg: &config.Config{
			PathReliability: pathreliability.Config{
				PropPriorFeeds: &pathreliability.PropPriorFeedsConfig{
					Enabled:                   true,
					Hp30PrimaryURL:            srv.URL + "/gfz",
					Hp30FallbackURL:           srv.URL + "/kp",
					SFIPrimaryURL:             srv.URL + "/f107",
					SFIFallbackURL:            srv.URL + "/gfz",
					RTSWMagURL:                srv.URL + "/mag",
					RTSWWindURL:               srv.URL + "/wind",
					DstPrimaryURL:             srv.URL + "/kyoto",
					DstFallbackURL:            srv.URL + "/geospace",
					Hp30PollIntervalSeconds:   600,
					SFIPollIntervalSeconds:    1800,
					RTSWPollIntervalSeconds:   300,
					DstPollIntervalSeconds:    900,
					RequestTimeoutSeconds:     5,
					UserAgent:                 "GoCluster-Test/1.0",
					MaxBodyBytesStandard:      1048576,
					MaxBodyBytesRTSW:          8388608,
					MaxBodyBytesGeospace:      4194304,
					Hp30StaleAfterSeconds:     5400,
					SFIStaleAfterSeconds:      129600,
					DstStaleAfterSeconds:      10800,
					FailoverConsecutiveErrors: 3,
				},
			},
		},
	}
	if !rt.loadPropPriorFeedsConfig() {
		t.Fatalf("enabled prop_prior_feeds failed load: %v", rt.startupErr)
	}
	if rt.propPriorPoller == nil || rt.propPriorWeather == nil {
		t.Fatalf("enabled prop_prior_feeds did not initialize store and poller")
	}
	rt.ctx, rt.cancel = context.WithCancel(context.Background())
	rt.propPriorPoller.Start(rt.ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && hits.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if hits.Load() == 0 {
		t.Fatalf("enabled prop_prior_feeds made no HTTP polls")
	}
	rt.close()
}

func TestClusterRuntimePropPriorFallbackQuietDefaultsAndOverview(t *testing.T) {
	pathCfg := pathreliability.DefaultConfig()
	pathCfg.VOACAPFallback.Enabled = false
	fbCfg := pathreliability.DefaultPropPriorFallbackConfig()
	pathCfg.PropPriorFallback = &fbCfg
	pathCfg.PropPriorFeeds = nil

	rt := &clusterRuntime{
		cfg: &config.Config{
			PathReliability: pathCfg,
		},
	}
	if !rt.loadPropPriorFeedsConfig() {
		t.Fatalf("loadPropPriorFeedsConfig failed: %v", rt.startupErr)
	}
	if !rt.loadPathReliabilityConfig() {
		t.Fatalf("loadPathReliabilityConfig failed: %v", rt.startupErr)
	}
	if rt.propPriorPoller != nil {
		t.Fatalf("expected nil poller when prop_prior_feeds.enabled=false")
	}
	if rt.propPriorFallback == nil || rt.propPriorWeather == nil {
		t.Fatalf("expected propPriorFallback and quiet-default weather store")
	}
	if rt.activePathClosedFallback() == nil {
		t.Fatalf("activePathClosedFallback returned nil")
	}

	lines := formatPathLinesWithPropPrior(nil, nil, rt.propPriorFallback, time.Date(2026, 6, 15, 16, 0, 0, 0, time.UTC), 0, false)
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "Prop prior cache") || !strings.Contains(joined, "Prop prior weather") || !strings.Contains(joined, "quiet-defaults") {
		t.Fatalf("Overview missing Prop prior lines:\n%s", joined)
	}
	if strings.Contains(joined, "VOACAP cache") {
		t.Fatalf("Overview should omit VOACAP cache line when Prop prior is active:\n%s", joined)
	}
}

func TestClusterRuntimePropPriorFallbackAndPollerConcurrentRace(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch r.URL.Path {
		case "/gfz":
			_, _ = w.Write([]byte(`{"datetime":["2026-11-27T15:00:00Z"],"Hp30":[2.333]}`))
		case "/f107":
			_, _ = w.Write([]byte(`[{"time_tag":"2026-11-26T20:00:00","flux":155.0,"reporting_schedule":"Noon"}]`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()

	pathCfg := pathreliability.DefaultConfig()
	pathCfg.VOACAPFallback.Enabled = false
	fbCfg := pathreliability.DefaultPropPriorFallbackConfig()
	pathCfg.PropPriorFallback = &fbCfg
	pathCfg.PropPriorFeeds = &pathreliability.PropPriorFeedsConfig{
		Enabled:                   true,
		Hp30PrimaryURL:            srv.URL + "/gfz",
		Hp30FallbackURL:           srv.URL + "/kp",
		SFIPrimaryURL:             srv.URL + "/f107",
		SFIFallbackURL:            srv.URL + "/gfz",
		RTSWMagURL:                srv.URL + "/mag",
		RTSWWindURL:               srv.URL + "/wind",
		DstPrimaryURL:             srv.URL + "/kyoto",
		DstFallbackURL:            srv.URL + "/geospace",
		Hp30PollIntervalSeconds:   600,
		SFIPollIntervalSeconds:    1800,
		RTSWPollIntervalSeconds:   300,
		DstPollIntervalSeconds:    900,
		RequestTimeoutSeconds:     5,
		UserAgent:                 "GoCluster-Test/1.0",
		MaxBodyBytesStandard:      1048576,
		MaxBodyBytesRTSW:          8388608,
		MaxBodyBytesGeospace:      4194304,
		Hp30StaleAfterSeconds:     5400,
		SFIStaleAfterSeconds:      129600,
		DstStaleAfterSeconds:      10800,
		FailoverConsecutiveErrors: 3,
	}

	rt := &clusterRuntime{cfg: &config.Config{PathReliability: pathCfg}}
	if !rt.loadPropPriorFeedsConfig() || !rt.loadPathReliabilityConfig() {
		t.Fatalf("runtime load failed: %v", rt.startupErr)
	}
	rt.ctx, rt.cancel = context.WithCancel(context.Background())
	rt.propPriorPoller.Start(rt.ctx)

	var wg sync.WaitGroup
	now := time.Date(2026, 11, 27, 16, 0, 0, 0, time.UTC)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				chk := rt.propPriorFallback.CheckForecastDetailed(pathreliability.VOACAPClosedRequest{
					UserCell: 101, DXCell: 202, UserGrid: "FN20", DXGrid: "JO22", Band: "20m", Mode: "CW",
				}, now)
				if chk.Status != pathreliability.VOACAPForecastCheckReady {
					t.Errorf("unexpected status %s", chk.Status)
					return
				}
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && hits.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	rt.close()
}
