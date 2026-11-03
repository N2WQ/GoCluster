package cluster

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	"dxcluster/config"
	"dxcluster/internal/logfiles"
)

func TestFileDestinationConfigsCompleteNormalizedSet(t *testing.T) {
	event := func(path string, days int) config.EventFileLoggingConfig {
		return config.EventFileLoggingConfig{Enabled: true, Dir: path, RetentionDays: days, DedupeWindowSeconds: 11}
	}
	cfg := config.LoggingConfig{
		Enabled: true, Dir: "logs/system", RetentionDays: 9,
		Propagation:   config.PropagationLoggingConfig{Enabled: true, Dir: "logs/propagation", RetentionDays: 3},
		DroppedCalls:  config.DroppedCallLoggingConfig{Enabled: true, Dir: "logs/drops", RetentionDays: 0, DedupeWindowSeconds: 7, BadDEDX: true, NoLicense: true, Harmonics: true},
		LoginAttempts: event("logs/login", 2), ReputationDrops: event("logs/reputation", 4), TelnetConnections: event("logs/telnet", 5), IngestConnections: event("logs/ingest", 6), PeerConnections: event("logs/peer", 8),
	}
	called := false
	hook := func(time.Time, string, string) { called = true }
	got := fileDestinationConfigs(cfg, hook)
	if len(got) != 11 {
		t.Fatalf("got %d destinations", len(got))
	}
	expected := map[uint8]struct {
		path      string
		days      int
		processor bool
	}{
		streamSystemFile: {"logs/system", 9, false}, streamPropagation: {"logs/propagation", 3, false},
		streamDroppedBadCall: {filepath.Join("logs/drops", "bad_de_dx"), 9, true}, streamDroppedUnlicensed: {filepath.Join("logs/drops", "no_license"), 9, true}, streamDroppedHarmonic: {filepath.Join("logs/drops", "harmonics"), 9, true},
		streamLoginAttempts: {"logs/login", 2, true}, streamReputationDrops: {"logs/reputation", 4, true}, streamTelnetConnections: {"logs/telnet", 5, true}, streamIngestConnections: {"logs/ingest", 6, true}, streamPeerConnections: {"logs/peer", 8, true}, streamPeerOverlong: {peerOverlongPath, 0, false},
	}
	for _, item := range got {
		want, ok := expected[item.Stream]
		if !ok {
			t.Fatalf("unexpected/duplicate stream %d", item.Stream)
		}
		delete(expected, item.Stream)
		if item.Path != want.path || item.RetentionDays != want.days || (item.ProcessorFactory != nil) != want.processor {
			t.Fatalf("stream %d got %+v want %+v", item.Stream, item, want)
		}
		if item.Stream == streamPeerOverlong && item.Kind != logfiles.Overlong {
			t.Fatal("overlong policy missing")
		}
		if item.Stream != streamPeerOverlong && item.Kind != logfiles.Daily {
			t.Fatalf("stream %d kind %v", item.Stream, item.Kind)
		}
		if item.Stream == streamPropagation {
			if item.RotateHook == nil {
				t.Fatal("missing propagation hook")
			}
			item.RotateHook(time.Time{}, "", "")
		} else if item.RotateHook != nil {
			t.Fatalf("unexpected hook on %d", item.Stream)
		}
		if item.ProcessorFactory != nil && item.ProcessorFactory() == nil {
			t.Fatalf("stream %d factory returned nil", item.Stream)
		}
	}
	if len(expected) != 0 || !called {
		t.Fatalf("missing streams %v or rotation hook", expected)
	}
}

func TestFileDestinationConfigsDisabledAndUnsafePaths(t *testing.T) {
	cfg := config.LoggingConfig{RetentionDays: 0, DroppedCalls: config.DroppedCallLoggingConfig{Enabled: true, Dir: strings.Repeat("x", maxConfiguredLogPathBytes+1), BadDEDX: true}}
	got := fileDestinationConfigs(cfg, nil)
	if len(got) != 2 || got[0].Stream != streamDroppedBadCall || got[0].Path != "" || got[0].RetentionDays != 7 || got[0].ProcessorFactory != nil {
		t.Fatalf("invalid dropped config: %+v", got)
	}
	if got[1].Stream != streamPeerOverlong || got[1].Path != peerOverlongPath {
		t.Fatalf("disabled daily peer lost overlong destination: %+v", got[1])
	}
}

func TestFileDestinationConfigsDetachAcceptedPathBacking(t *testing.T) {
	// Configuration normalization can leave a short substring of a much larger
	// YAML value. A retained mapping must not retain that original allocation.
	const prefix = 3 << 20
	backing := strings.Repeat("z", prefix) + "  logs/shared  " + strings.Repeat("z", prefix)
	borrowed := backing[prefix : prefix+len("  logs/shared  ")]
	trimmed := strings.TrimSpace(borrowed)
	if unsafe.StringData(trimmed) == nil || unsafe.StringData(trimmed) != unsafe.StringData(backing[prefix+2:]) {
		t.Fatal("test did not create a substring of the large backing")
	}
	event := config.EventFileLoggingConfig{Enabled: true, Dir: borrowed}
	cfg := config.LoggingConfig{
		Enabled: true, Dir: borrowed,
		Propagation:   config.PropagationLoggingConfig{Enabled: true, Dir: borrowed},
		DroppedCalls:  config.DroppedCallLoggingConfig{Enabled: true, Dir: borrowed, BadDEDX: true, NoLicense: true, Harmonics: true},
		LoginAttempts: event, ReputationDrops: event, TelnetConnections: event, IngestConnections: event, PeerConnections: event,
	}
	got := fileDestinationConfigs(cfg, nil)
	if len(got) != 11 {
		t.Fatalf("destinations=%d", len(got))
	}
	for _, item := range got {
		if item.Stream == streamPeerOverlong {
			continue
		}
		want := trimmed
		switch item.Stream {
		case streamDroppedBadCall:
			want = filepath.Join(trimmed, "bad_de_dx")
		case streamDroppedUnlicensed:
			want = filepath.Join(trimmed, "no_license")
		case streamDroppedHarmonic:
			want = filepath.Join(trimmed, "harmonics")
		}
		if item.Path != want {
			t.Fatalf("stream %d path=%q want=%q", item.Stream, item.Path, want)
		}
		if unsafe.StringData(item.Path) == unsafe.StringData(trimmed) {
			t.Fatalf("stream %d retained source backing", item.Stream)
		}
	}
	maxPath := strings.Repeat("x", maxConfiguredLogPathBytes)
	maximum := fileDestinationConfigs(config.LoggingConfig{Enabled: true, Dir: maxPath}, nil)
	if maximum[0].Path != maxPath || unsafe.StringData(maximum[0].Path) == unsafe.StringData(maxPath) {
		t.Fatal("maximum-length path changed or retained its input")
	}
	oversized := fileDestinationConfigs(config.LoggingConfig{Enabled: true, Dir: maxPath + "x"}, nil)
	if oversized[0].Path != "" {
		t.Fatal("oversized path became usable")
	}
}

func TestFileDestinationConfigsAllMaximumPathsHaveIndependentBacking(t *testing.T) {
	// Use a near-maximum path that requires separator conversion on Windows and
	// still leaves room for each dropped-call leaf. The live mapping can contain
	// all ten daily paths at once, even when they came from the same input.
	path := strings.Repeat("a/", (maxConfiguredLogPathBytes-64)/2) + "leaf"
	event := config.EventFileLoggingConfig{Enabled: true, Dir: path, DedupeWindowSeconds: 1}
	cfg := config.LoggingConfig{
		Enabled: true, Dir: path,
		Propagation:   config.PropagationLoggingConfig{Enabled: true, Dir: path},
		DroppedCalls:  config.DroppedCallLoggingConfig{Enabled: true, Dir: path, BadDEDX: true, NoLicense: true, Harmonics: true, DedupeWindowSeconds: 1},
		LoginAttempts: event, ReputationDrops: event, TelnetConnections: event, IngestConnections: event, PeerConnections: event,
	}
	got := fileDestinationConfigs(cfg, nil)
	if len(got) != 11 {
		t.Fatalf("got %d destinations, want 11", len(got))
	}
	seen := make(map[*byte]bool)
	var retained int
	for _, item := range got {
		if item.Stream == streamPeerOverlong {
			continue // Its immutable path is not cloned from configuration.
		}
		if len(item.Path) < maxConfiguredLogPathBytes-128 || len(item.Path) > maxConfiguredLogPathBytes {
			t.Fatalf("stream %d path length %d", item.Stream, len(item.Path))
		}
		ptr := unsafe.StringData(item.Path)
		if ptr == unsafe.StringData(path) || seen[ptr] {
			t.Fatalf("stream %d shares a retained path allocation", item.Stream)
		}
		seen[ptr] = true
		retained += len(item.Path)
	}
	if len(seen) != 10 || retained > 10*maxConfiguredLogPathBytes {
		t.Fatalf("daily paths=%d, retained bytes=%d", len(seen), retained)
	}
}
