package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/dxsummit"
	"dxcluster/pskreporter"
	"dxcluster/spot"
)

func TestSourceStatsLabel(t *testing.T) {
	cases := []struct {
		name string
		spot *spot.Spot
		want string
	}{
		{"manual", &spot.Spot{SourceType: spot.SourceManual}, "HUMAN"},
		{"rbn-digital", &spot.Spot{SourceType: spot.SourceRBN, SourceNode: "RBN-DIGITAL"}, "RBN-DIGITAL"},
		{"rbn", &spot.Spot{SourceType: spot.SourceRBN, SourceNode: "RBN"}, "RBN"},
		{"ft8", &spot.Spot{SourceType: spot.SourceFT8}, "RBN-FT"},
		{"psk", &spot.Spot{SourceType: spot.SourcePSKReporter}, "PSKREPORTER"},
		{"peer", &spot.Spot{SourceType: spot.SourcePeer}, "PEER"},
		{"upstream", &spot.Spot{SourceType: spot.SourceUpstream}, "UPSTREAM"},
		{"dxsummit-upstream", &spot.Spot{SourceType: spot.SourceUpstream, SourceNode: "DXSUMMIT"}, "DXSUMMIT"},
		{"node-fallback", &spot.Spot{SourceNode: "PSKREPORTER"}, "PSKREPORTER"},
		{"dxsummit-node-fallback", &spot.Spot{SourceNode: "DXSUMMIT"}, "DXSUMMIT"},
		{"other", &spot.Spot{}, "OTHER"},
	}
	for _, tc := range cases {
		if got := sourceStatsLabel(tc.spot); got != tc.want {
			t.Fatalf("%s: expected %s, got %s", tc.name, tc.want, got)
		}
	}
}

func TestRBNIngestDeltasUsesRBNFT(t *testing.T) {
	sourceTotals := map[string]uint64{
		"RBN":    10,
		"RBN-FT": 7,
	}
	prevSourceTotals := map[string]uint64{
		"RBN":    3,
		"RBN-FT": 2,
	}
	sourceModeTotals := map[string]uint64{
		"RBN|CW":         5,
		"RBN|RTTY":       2,
		"RBN-FT|FT8":     6,
		"RBN-FT|FT4":     1,
		"RBN-FT|FT2":     4,
		"RBN-DIGITAL|CW": 9,
	}
	prevSourceModeTotals := map[string]uint64{
		"RBN|CW":     1,
		"RBN|RTTY":   0,
		"RBN-FT|FT8": 4,
		"RBN-FT|FT4": 1,
		"RBN-FT|FT2": 1,
	}

	rbnTotal, rbnCW, rbnRTTY, rbnFTTotal, rbnFT8, rbnFT4, rbnFT2 :=
		rbnIngestDeltas(sourceTotals, prevSourceTotals, sourceModeTotals, prevSourceModeTotals)

	if rbnTotal != 7 || rbnCW != 4 || rbnRTTY != 2 {
		t.Fatalf("unexpected RBN CW/RTTY deltas: total=%d cw=%d rtty=%d", rbnTotal, rbnCW, rbnRTTY)
	}
	if rbnFTTotal != 5 || rbnFT8 != 2 || rbnFT4 != 0 || rbnFT2 != 3 {
		t.Fatalf("unexpected RBN-FT deltas: total=%d ft8=%d ft4=%d ft2=%d", rbnFTTotal, rbnFT8, rbnFT4, rbnFT2)
	}
}

func TestFormatIngestLineIncludesFT2AndCommaAwareWidths(t *testing.T) {
	got := formatIngestLine("[green]PSK[-]", 24711, 314, 0, 23651, 724, 336, 0, true)
	want := "[green]PSK[-]: 24,711 | [yellow]CW[-] 314   | [yellow]RTTY[-] 0     | [yellow]FT8[-] 23,651 | [yellow]FT4[-] 724    | [yellow]FT2[-] 336    | [yellow]MSK[-] 0   "
	if got != want {
		t.Fatalf("unexpected ingest line:\ngot  %q\nwant %q", got, want)
	}
}

func TestBuildOverviewLinesIncludesFT2IngestRates(t *testing.T) {
	lines := buildOverviewLines(
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		"N2WQ-2",
		true, true, false,
		nil,
		2420, 1337, 0, 981, 102, 18,
		24711, 314, 0, 23651, 724, 336, 0, 0,
		0,
		0, 0, 0, 0,
		"[yellow]Path[-]: n/a",
		"",
		nil,
		"n/a",
	)
	joined := strings.Join(lines, "\n")
	for _, want := range []string{
		"[green]RBN[-]: 2,420  | [yellow]CW[-] 1,337 | [yellow]RTTY[-] 0     | [yellow]FT8[-] 981    | [yellow]FT4[-] 102    | [yellow]FT2[-] 18",
		"[green]PSK[-]: 24,711 | [yellow]CW[-] 314   | [yellow]RTTY[-] 0     | [yellow]FT8[-] 23,651 | [yellow]FT4[-] 724    | [yellow]FT2[-] 336    | [yellow]MSK[-] 0    | [yellow]PSK[-] 0",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected overview lines to include %q, got %v", want, lines)
		}
	}
}

func TestWithIngestStatusLabel(t *testing.T) {
	if got := withIngestStatusLabel("RBN", true); got != "[green]RBN[-]" {
		t.Fatalf("expected live label, got %q", got)
	}
	if got := withIngestStatusLabel("RBN", false); got != "[red]RBN[-]" {
		t.Fatalf("expected offline label, got %q", got)
	}
}

func TestRBNFeedFamilyLiveUsesEitherFeed(t *testing.T) {
	cases := []struct {
		name      string
		rbnCWLive bool
		rbnFTLive bool
		want      bool
	}{
		{name: "both offline"},
		{name: "cw live", rbnCWLive: true, want: true},
		{name: "ft live", rbnFTLive: true, want: true},
		{name: "both live", rbnCWLive: true, rbnFTLive: true, want: true},
	}
	for _, tc := range cases {
		if got := rbnFeedFamilyLive(tc.rbnCWLive, tc.rbnFTLive); got != tc.want {
			t.Fatalf("%s: expected %v, got %v", tc.name, tc.want, got)
		}
	}
}

func TestPSKReporterLive(t *testing.T) {
	now := time.Date(2026, 2, 5, 9, 30, 0, 0, time.UTC)
	if got := pskReporterLive(pskreporter.HealthSnapshot{}, now); got {
		t.Fatal("expected disconnected snapshot to be false")
	}
	snap := pskreporter.HealthSnapshot{
		Connected:     true,
		LastPayloadAt: now.Add(-ingestIdleThreshold + time.Second),
	}
	if got := pskReporterLive(snap, now); !got {
		t.Fatal("expected recent payload to be live")
	}
	snap.LastPayloadAt = now.Add(-ingestIdleThreshold - time.Second)
	if got := pskReporterLive(snap, now); got {
		t.Fatal("expected stale payload to be false")
	}
}

func TestFormatIngestSourceLinesEnabledOnly(t *testing.T) {
	sources := []dashboardIngestSource{
		{Label: "RBN", Enabled: true, Connected: true},
		{Label: "RBN-FT", Enabled: true, Connected: true},
		{Label: "PSKReporter", Enabled: true, Connected: true},
		{Label: "DXSUMMIT", Enabled: true, Connected: true},
		{Label: "Peers", Enabled: false, Connected: false},
	}
	lines := formatIngestSourceLines(sources)
	joined := strings.Join(lines, "\n")
	if lines[0] != "[yellow]Ingest[-]: 4 / 4 connected" {
		t.Fatalf("unexpected summary %q", lines[0])
	}
	for _, want := range []string{"[green]RBN[-]", "[green]RBN-FT[-]", "[green]PSKReporter[-]", "[green]DXSUMMIT[-]"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %q in %q", want, joined)
		}
	}
	if strings.Contains(joined, "Peers") {
		t.Fatalf("disabled peers should not be listed: %q", joined)
	}
}

func TestFormatIngestSourceLinesShowsOfflineEnabledSources(t *testing.T) {
	sources := []dashboardIngestSource{
		{Label: "RBN", Enabled: true, Connected: true},
		{Label: "RBN-FT", Enabled: true, Connected: true},
		{Label: "PSKReporter", Enabled: true, Connected: true},
		{Label: "DXSUMMIT", Enabled: true, Connected: false},
	}
	lines := formatIngestSourceLines(sources)
	joined := strings.Join(lines, "\n")
	if lines[0] != "[yellow]Ingest[-]: 3 / 4 connected" {
		t.Fatalf("unexpected summary %q", lines[0])
	}
	if !strings.Contains(joined, "[red]DXSUMMIT[-]") {
		t.Fatalf("expected offline DXSummit label in %q", joined)
	}
}

func TestFormatIngestSourceLinesPeerAggregate(t *testing.T) {
	t.Run("enabled no sessions", func(t *testing.T) {
		lines := formatIngestSourceLines([]dashboardIngestSource{
			{Label: "Peers", Enabled: true, Connected: false},
		})
		joined := strings.Join(lines, "\n")
		if lines[0] != "[yellow]Ingest[-]: 0 / 1 connected" {
			t.Fatalf("unexpected summary %q", lines[0])
		}
		if !strings.Contains(joined, "[red]Peers[-]") {
			t.Fatalf("expected offline peer aggregate in %q", joined)
		}
	})

	t.Run("multiple sessions count once", func(t *testing.T) {
		lines := formatIngestSourceLines([]dashboardIngestSource{
			{Label: "Peers", Enabled: true, Connected: true, Details: []string{"N2WQ-73", "KM3T-44"}},
		})
		joined := strings.Join(lines, "\n")
		if lines[0] != "[yellow]Ingest[-]: 1 / 1 connected" {
			t.Fatalf("unexpected summary %q", lines[0])
		}
		for _, want := range []string{"[green]N2WQ-73[-]", "[green]KM3T-44[-]"} {
			if !strings.Contains(joined, want) {
				t.Fatalf("expected %q in %q", want, joined)
			}
		}
	})
}

func TestFormatIngestSourceLinesNoEnabledSources(t *testing.T) {
	lines := formatIngestSourceLines([]dashboardIngestSource{
		{Label: "RBN", Enabled: false, Connected: true},
	})
	joined := strings.Join(lines, "\n")
	if lines[0] != "[yellow]Ingest[-]: 0 / 0 connected" {
		t.Fatalf("unexpected summary %q", lines[0])
	}
	if !strings.Contains(joined, "(none enabled)") {
		t.Fatalf("expected none enabled marker in %q", joined)
	}
}

func TestDXSummitIsLive(t *testing.T) {
	now := time.Date(2026, 4, 21, 20, 45, 0, 0, time.UTC)
	if got := dxsummitIsLive(dxsummit.HealthSnapshot{
		Connected:  true,
		LastPollAt: now.Add(-30 * time.Second),
	}, 30, now); !got {
		t.Fatal("expected recent successful poll to be live")
	}
	if got := dxsummitIsLive(dxsummit.HealthSnapshot{
		Connected:  false,
		LastPollAt: now.Add(-30 * time.Second),
	}, 30, now); got {
		t.Fatal("expected disconnected DXSummit snapshot to be offline")
	}
	if got := dxsummitIsLive(dxsummit.HealthSnapshot{
		Connected:  true,
		LastPollAt: now.Add(-62 * time.Second),
	}, 30, now); got {
		t.Fatal("expected stale DXSummit poll to be offline")
	}
}
