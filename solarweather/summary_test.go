package solarweather

import (
	"testing"
	"time"
)

func TestSummaryBothActive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.normalize()
	m := NewManager(cfg, nil)
	now := time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC)

	r3 := findRLevelIndex(cfg, "R3")
	g2 := findGLevelIndex(cfg, "G2")
	if r3 < 0 || g2 < 0 {
		t.Fatalf("expected default levels")
	}

	m.mu.Lock()
	m.state.RActiveUntil = make([]time.Time, len(cfg.rLevels))
	m.state.RActiveUntil[r3] = now.Add(10 * time.Minute)
	m.state.GOESTime = now
	m.state.GOESUpdatedAt = now
	m.state.Kp = cfg.gLevels[g2].MinKp
	m.state.KpTime = now
	m.state.KpUpdatedAt = now
	m.mu.Unlock()

	got := m.Summary(now)
	want := "SOLAR R3 G2 impacting 80m/60m/40m/30m/20m/17m/15m/12m/10m"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestManagerEnabledIsImmutableAvailability(t *testing.T) {
	var absent *Manager
	if absent.Enabled() || absent.Summary(time.Time{}) != "" {
		t.Fatal("nil manager reported available or produced a summary")
	}
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled unobserved"}[enabled], func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Enabled = enabled
			m := NewManager(cfg, nil)
			cfg.Enabled = !enabled
			now := time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC)
			snapshot := m.Snapshot(now)
			if m.Enabled() != enabled || !snapshot.GOESStale || !snapshot.KpStale || m.Summary(now) != "" {
				t.Fatal("availability followed caller configuration or empty/unobserved conditions")
			}
		})
	}
}

func TestSummaryFreshCalm(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	m := NewManager(cfg, nil)
	now := time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC)
	// Real owner updates supply fresh observations below every configured active
	// threshold. An unpolled manager's stale initial state is not this oracle.
	m.updateGOES(now, []goesSample{{Time: now, Flux: 1e-6}})
	m.updateKp(now, 0, now)
	snapshot := m.Snapshot(now)
	if !m.Enabled() || snapshot.GOESStale || snapshot.KpStale || snapshot.ActiveRLevel != -1 || snapshot.ActiveGLevel != -1 {
		t.Fatalf("calm fixture is stale, unavailable or active: %+v", snapshot)
	}
	if got := m.Summary(now); got != "" {
		t.Fatalf("fresh calm conditions produced a summary: %q", got)
	}
}

func TestSummaryDisabledWithFreshActiveObservation(t *testing.T) {
	m := NewManager(DefaultConfig(), nil)
	now := time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC)
	m.updateKp(now, 8, now)
	snapshot := m.Snapshot(now)
	if snapshot.KpStale || snapshot.ActiveGLevel < 0 {
		t.Fatal("disabled-manager control lacks a fresh active observation")
	}
	if m.Enabled() || m.Summary(now) != "" {
		t.Fatal("disabled manager emitted the otherwise active observation")
	}
}
