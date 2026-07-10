package cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/dxsummit"
	"dxcluster/pathreliability"
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
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
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

func TestBuildOverviewLinesIncludesVOACAPSSN(t *testing.T) {
	lines := buildOverviewLinesForStatsTest(fixedOverviewSSNProvider{ssn: 112, ok: true})
	joined := strings.Join(lines, "\n")
	want := "[yellow]CTY[-]: n/a  [yellow]FCC[-]: n/a  [yellow]Skew[-]: n/a  [yellow]VOACAP SSN[-]: 112"
	if !strings.Contains(joined, want) {
		t.Fatalf("expected overview lines to include %q, got %v", want, lines)
	}

	lines = buildOverviewLinesForStatsTest(fixedOverviewSSNProvider{})
	joined = strings.Join(lines, "\n")
	want = "[yellow]CTY[-]: n/a  [yellow]FCC[-]: n/a  [yellow]Skew[-]: n/a  [yellow]VOACAP SSN[-]: n/a"
	if !strings.Contains(joined, want) {
		t.Fatalf("expected unavailable VOACAP SSN to render as %q, got %v", want, lines)
	}
}

type fixedOverviewSSNProvider struct {
	ssn int
	ok  bool
}

func (p fixedOverviewSSNProvider) CurrentSSN(time.Time) (int, bool) {
	return p.ssn, p.ok
}

type fixedOverviewVOACAPFallbackProvider struct {
	snapshot pathreliability.VOACAPClosedFallbackSnapshot
}

func (p fixedOverviewVOACAPFallbackProvider) Snapshot() pathreliability.VOACAPClosedFallbackSnapshot {
	return p.snapshot
}

func buildOverviewLinesForStatsTest(voacapSSN fixedOverviewSSNProvider) []string {
	return buildOverviewLines(
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, voacapSSN, nil, nil, nil,
		"N2WQ-2",
		false, false, false,
		nil,
		0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0,
		0, 0, 0, 0,
		"[yellow]Path[-]: n/a",
		"",
		nil,
		"n/a",
	)
}

func TestFormatPathLinesIncludesVOACAPCacheSnapshot(t *testing.T) {
	provider := fixedOverviewVOACAPFallbackProvider{
		snapshot: pathreliability.VOACAPClosedFallbackSnapshot{
			CacheEntries:    1234,
			DelayEntries:    56,
			InflightEntries: 7,
			QueueDepth:      8,
		},
	}
	lines := formatPathLines(nil, provider, time.Now().UTC())
	joined := strings.Join(lines, "\n")
	want := "[yellow]VOACAP cache[-]: 1,234 (C) / 56 (D) / 7 (I) / 8 (Q)"
	if !strings.Contains(joined, want) {
		t.Fatalf("expected VOACAP cache line %q, got %v", want, lines)
	}
	if !strings.Contains(joined, "[yellow]H3 path pairs[-]: n/a") {
		t.Fatalf("expected H3 path pairs fallback, got %v", lines)
	}

	lines = formatPathLines(nil, nil, time.Now().UTC())
	joined = strings.Join(lines, "\n")
	if !strings.Contains(joined, "[yellow]VOACAP cache[-]: n/a") {
		t.Fatalf("expected nil VOACAP fallback to render n/a, got %v", lines)
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

func TestFormatIngestSourceLinesCountsAndDisplaysEveryHumanFeed(t *testing.T) {
	sources := make([]dashboardIngestSource, 0, 66)
	sources = append(sources,
		dashboardIngestSource{Label: "RBN", Enabled: true, Connected: true},
		dashboardIngestSource{Label: "RBN-FT", Enabled: true, Connected: false},
	)
	for i := 0; i < 64; i++ {
		sources = append(sources, dashboardIngestSource{
			Label:     fmt.Sprintf("HUMAN/UP%02d", i),
			Enabled:   true,
			Connected: i%2 == 0,
		})
	}

	lines := formatIngestSourceLines(sources)
	joined := strings.Join(lines, "\n")
	if got, want := lines[0], "[yellow]Ingest[-]: 33 / 66 connected"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if strings.Contains(joined, "... +") {
		t.Fatalf("ingest source list was truncated: %q", joined)
	}
	last := -1
	for i := 0; i < 64; i++ {
		label := fmt.Sprintf("HUMAN/UP%02d", i)
		if count := strings.Count(joined, label); count != 1 {
			t.Fatalf("%s count = %d, want 1", label, count)
		}
		idx := strings.Index(joined, label)
		if idx <= last {
			t.Fatalf("%s appears out of YAML order", label)
		}
		last = idx
	}
}

func TestDashboardIngestSourcesKeepsHumanFeedsDistinctFromBuiltins(t *testing.T) {
	cfg := dashboardIngestSourceConfig{
		RBNEnabled:         true,
		RBNDigitalEnabled:  true,
		PSKReporterEnabled: true,
		DXSummitEnabled:    true,
	}
	humans := []dashboardIngestSource{
		{Label: "HUMAN/RBN", Enabled: true, Connected: true},
		{Label: "HUMAN/BACKUP", Enabled: true, Connected: false},
	}
	sources := dashboardIngestSources(cfg, true, false, true, true, false, 0, nil, humans...)

	wantLabels := []string{"RBN", "RBN-FT", "HUMAN/RBN", "HUMAN/BACKUP", "PSKReporter", "DXSUMMIT", "Peers"}
	if len(sources) != len(wantLabels) {
		t.Fatalf("source count = %d, want %d", len(sources), len(wantLabels))
	}
	for i, want := range wantLabels {
		if sources[i].Label != want {
			t.Fatalf("source[%d] label = %q, want %q", i, sources[i].Label, want)
		}
	}
	if !sources[2].Connected || sources[3].Connected {
		t.Fatalf("human connection states = %v/%v, want true/false", sources[2].Connected, sources[3].Connected)
	}
}

func TestIngestHealthStateKeySeparatesIdentityFromDisplayLabel(t *testing.T) {
	human := ingestHealthSource{id: "human:foo", name: "HUMAN/Foo"}
	builtin := ingestHealthSource{id: "rbn:cw", name: "human:foo"}
	if got := ingestHealthSourceStateKey(human); got != "human:foo" {
		t.Fatalf("human state key = %q", got)
	}
	if got := ingestHealthSourceStateKey(builtin); got != "rbn:cw" {
		t.Fatalf("builtin state key = %q", got)
	}
	if ingestHealthSourceStateKey(human) == ingestHealthSourceStateKey(builtin) {
		t.Fatal("human and builtin health identities collided")
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
