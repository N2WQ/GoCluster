package main

import (
	"testing"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestCloneSpotForBroadcastPreservesHasReportAndReport(t *testing.T) {
	src := spot.NewSpot("GM4YXI", "EA1TX", 144360.0, "MSK144")
	src.Report = 0
	src.HasReport = true

	clone := cloneSpotForBroadcast(src)
	if clone == nil {
		t.Fatalf("expected clone spot, got nil")
	}
	if clone == src {
		t.Fatalf("expected clone to be a new instance")
	}
	if clone.Report != src.Report {
		t.Fatalf("expected Report=%d, got %d", src.Report, clone.Report)
	}
	if clone.HasReport != src.HasReport {
		t.Fatalf("expected HasReport=%v, got %v", src.HasReport, clone.HasReport)
	}
}

func TestCloneSpotForBroadcastPreservesMissingReport(t *testing.T) {
	src := spot.NewSpot("GM4YXI", "EA1TX", 144360.0, "MSK144")
	src.Report = 99
	src.HasReport = false

	clone := cloneSpotForBroadcast(src)
	if clone == nil {
		t.Fatalf("expected clone spot, got nil")
	}
	if clone.HasReport {
		t.Fatalf("expected HasReport=false, got true with Report=%d", clone.Report)
	}
	if clone.Report != src.Report {
		t.Fatalf("expected Report=%d, got %d", src.Report, clone.Report)
	}
}

func TestGridDBCheckOnMissEnabled_DefaultsTrue(t *testing.T) {
	t.Setenv(envGridDBCheckOnMiss, "")

	got, source := gridDBCheckOnMissEnabled(&config.Config{})
	if !got {
		t.Fatalf("expected default grid DB check to be enabled, got %v (source=%s)", got, source)
	}
}

func TestGridDBCheckOnMissEnabled_ConfigFalse(t *testing.T) {
	t.Setenv(envGridDBCheckOnMiss, "")
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(false)}

	got, source := gridDBCheckOnMissEnabled(cfg)
	if got {
		t.Fatalf("expected grid DB check to be disabled by config, got %v (source=%s)", got, source)
	}
}

func TestGridDBCheckOnMissEnabled_EnvOverridesConfig(t *testing.T) {
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(true)}
	t.Setenv(envGridDBCheckOnMiss, "false")

	got, source := gridDBCheckOnMissEnabled(cfg)
	if got {
		t.Fatalf("expected env override to disable grid DB check, got %v (source=%s)", got, source)
	}
	if source != envGridDBCheckOnMiss {
		t.Fatalf("expected source=%q, got %q", envGridDBCheckOnMiss, source)
	}
}

func TestGridDBCheckOnMissEnabled_InvalidEnvIgnored(t *testing.T) {
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(false)}
	t.Setenv(envGridDBCheckOnMiss, "notabool")

	got, _ := gridDBCheckOnMissEnabled(cfg)
	if got {
		t.Fatalf("expected invalid env override to be ignored, got %v", got)
	}
}

func TestGridDBCheckOnMissEnabled_UsesLoadedFromWhenSet(t *testing.T) {
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(true), LoadedFrom: "data/config"}
	t.Setenv(envGridDBCheckOnMiss, "")

	_, source := gridDBCheckOnMissEnabled(cfg)
	if source != "data/config" {
		t.Fatalf("expected source=data/config, got %s", source)
	}
}

func TestCollapseSSIDForBroadcast(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"N2WQ-1-#", "N2WQ-#"},
		{"N2WQ-#", "N2WQ-#"},
		{"N2WQ-1", "N2WQ"},
		{"N2WQ-12", "N2WQ"},
		{"N2WQ-TEST", "N2WQ-TEST"},
		{"N2WQ-1/P", "N2WQ-1/P"},
		{"", ""},
	}

	for _, tc := range cases {
		got := collapseSSIDForBroadcast(tc.input)
		if got != tc.want {
			t.Fatalf("collapseSSIDForBroadcast(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func boolPtr(v bool) *bool {
	b := v
	return &b
}
