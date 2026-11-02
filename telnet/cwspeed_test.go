package telnet

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/filter"
	"dxcluster/spot"
)

func cwSpeedFixture() *spot.Spot {
	sp := spot.NewSpot("K1ABC", "W1AW", 14025, "CW")
	sp.Time = time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	sp.SourceType = spot.SourceRBN
	sp.CWDisplayAvailable = true
	sp.CWDisplayComment = "POTA"
	sp.CWSpeeds = []string{"22 WPM", "30 WPM"}
	sp.Comment = "22 WPM 30 WPM POTA"
	return sp
}

func TestCWSpeedReopenedHistoryPreservesCommentSearch(t *testing.T) {
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1}
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	sp := cwSpeedFixture()
	sp.Time = time.Now().UTC()
	w.Enqueue(sp)
	w.Stop()
	w, err = archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		return w.ReadHistoryPage(req)
	})
	c.configurationInitialized = true
	if line := runHistory(t, s, c, "SHOW DX 1 COMMENT WPM"); strings.Contains(line, "22 WPM") || !strings.Contains(line, "POTA") {
		t.Fatalf("disabled history failed matching or hid ordinary comment: %q", line)
	}
	c.filterMu.Lock()
	c.configuredSettings.CWSpeed = true
	c.filterMu.Unlock()
	if line := runHistory(t, s, c, "SHOW DX 1 COMMENT WPM"); !strings.Contains(line, "22 WPM") || !strings.Contains(line, "POTA") {
		t.Fatalf("enabled history failed matching or display: %q", line)
	}
}

func TestCWSpeedLiveUsersAndDiagnostics(t *testing.T) {
	s := NewServer(ServerOptions{}, nil)
	sp := cwSpeedFixture()
	off := &Client{}
	on := &Client{configuredSettings: filter.SettingsConfiguration{CWSpeed: true}}
	// The same immutable spot is formatted in both orders to detect cache leakage.
	for _, c := range []*Client{off, on, off, on} {
		for _, line := range []string{s.formatSpotForClient(c, sp), s.formatSpotForClientWithPrediction(c, sp, pathPrediction{})} {
			if strings.Contains(line, "22 WPM") != (c == on) || !strings.Contains(line, "POTA") {
				t.Fatalf("unexpected user display: %q", line)
			}
		}
	}
	for _, mode := range []diagMode{diagModeDedupe, diagModeSource, diagModeConfidence, diagModePath, diagModeMode} {
		on.setDiagMode(mode)
		for _, line := range []string{s.formatSpotForClient(on, sp), s.formatSpotForClientWithPrediction(on, sp, pathPrediction{})} {
			if strings.Contains(line, "22 WPM") || strings.Contains(line, "30 WPM") {
				t.Fatalf("diagnostic mode %v leaked CW speed: %q", mode, line)
			}
		}
	}
}

func BenchmarkCWSpeedFormat(b *testing.B) {
	for _, tc := range []struct {
		name       string
		projection bool
		enabled    bool
	}{
		{"ordinary", false, false},
		{"speed-off", true, false},
		{"speed-on", true, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			sp := cwSpeedFixture()
			sp.CWDisplayAvailable = tc.projection
			c := &Client{configuredSettings: filter.SettingsConfiguration{CWSpeed: tc.enabled}}
			line := formatCWSpotForClient(c, sp)
			if line == "" || tc.projection && strings.Contains(line, "22 WPM") != tc.enabled {
				b.Fatalf("incorrect formatted output: %q", line)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = formatCWSpotForClient(c, sp)
			}
		})
	}
}

func TestCWSpeedHumanCommandPersistsAcrossDialects(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s, c := v7MachineTestClient(t)
			c.dialect = dialect
			c.configuredSettings.Dialect = string(dialect)
			if response, handled := s.handleCWSpeedCommand(c, "SET CWSPEED ON"); !handled || response != "CW speed: ON\n" {
				t.Fatalf("enable command: handled=%v response=%q", handled, response)
			}
			record, err := filter.LoadV7UserRecord(c.callsign)
			if err != nil || !record.Configuration.Settings.CWSpeed || !c.cwSpeedEnabled() {
				t.Fatalf("enabled choice not persisted: %+v %v", record, err)
			}
			next := configurationTestClient(s, c.callsign)
			if result := s.restoreV7ClientRecordOwned(next, time.Now().UTC()); result.loadError != nil || !next.cwSpeedEnabled() {
				t.Fatalf("reconnect did not restore enabled choice: %+v", result)
			}
			if response, handled := s.handleCWSpeedCommand(c, "SET CWSPEED OFF extra"); !handled || !strings.Contains(response, "Usage:") || !c.cwSpeedEnabled() {
				t.Fatalf("invalid command changed setting: handled=%v response=%q", handled, response)
			}
			if response, handled := s.handleCWSpeedCommand(c, "SET CWSPEED OFF"); !handled || response != "CW speed: OFF\n" {
				t.Fatalf("disable command: handled=%v response=%q", handled, response)
			}
			record, err = filter.LoadV7UserRecord(c.callsign)
			if err != nil || record.Configuration.Settings.CWSpeed || c.cwSpeedEnabled() {
				t.Fatalf("disabled choice not persisted: %+v %v", record, err)
			}
		})
	}
}

func TestCWSpeedHistoryDisplayAndDiagnostic(t *testing.T) {
	sp := cwSpeedFixture()
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Spots: []*spot.Spot{sp}, End: archive.HistoryExhausted}, nil
	})
	c.filterMu.Lock()
	c.configuredSettings.CWSpeed = true
	c.configurationInitialized = true
	c.filterMu.Unlock()
	if line := runHistory(t, s, c, "SHOW DX 1"); !strings.Contains(line, "22 WPM") {
		t.Fatalf("enabled history omitted speed: %q", line)
	}
	c.setDiagMode(diagModeSource)
	if line := runHistory(t, s, c, "SHOW DX 1"); strings.Contains(line, "22 WPM") || !strings.Contains(line, "POTA") {
		t.Fatalf("diagnostic history showed speed or lost comment: %q", line)
	}
}

func TestCWSpeedHistoryRejectsChangeAndRestore(t *testing.T) {
	for _, kind := range []string{"preference", "diagnostic"} {
		t.Run(kind, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			sp := cwSpeedFixture()
			s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
				close(entered)
				<-release
				return archive.HistoryPage{Spots: []*spot.Spot{sp}, End: archive.HistoryExhausted}, nil
			})
			done := make(chan bool, 1)
			go func() { _, ok := s.handleHistoryCommand(c, "SHOW DX 1"); done <- ok }()
			<-entered
			if kind == "diagnostic" {
				c.setDiagMode(diagModeSource)
				c.setDiagMode(diagModeOff)
			} else {
				for _, value := range []bool{true, false} {
					s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
						c.filterMu.Lock()
						c.configuredSettings.CWSpeed = value
						c.filterMu.Unlock()
						return "", true
					})
				}
			}
			close(release)
			if !<-done {
				t.Fatal("history response failed")
			}
			if response := <-c.controlChan; response.line != historyRestart {
				t.Fatalf("published stale history after %s change: %q", kind, response.line)
			}
		})
	}
}
