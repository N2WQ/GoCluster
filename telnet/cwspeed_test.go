package telnet

import (
	"path/filepath"
	"strings"
	"sync"
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
		name, canonical, want string
		projection, enabled   bool
	}{
		{"ordinary", "POTA", "CW POTA", false, false},
		{"speed-off-equal", "POTA", "CW POTA", true, false},
		{"speed-off-different", "22 WPM 30 WPM POTA", "CW POTA", true, false},
		{"speed-on", "22 WPM 30 WPM POTA", "CW POTA 22 WPM 30 WPM", true, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			sp := cwSpeedFixture()
			sp.CWDisplayAvailable = tc.projection
			sp.Comment = tc.canonical
			c := &Client{configuredSettings: filter.SettingsConfiguration{CWSpeed: tc.enabled}}
			_ = sp.FormatDXCluster() // Compare repeated fanout after the base line is cached.
			line := formatCWSpotForClient(c, sp)
			if got := cwCommentRegion(line); got != tc.want {
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

func cwCommentRegion(line string) string {
	return strings.TrimSpace(line[39 : spot.CurrentDXClusterLayout().TailStartColumn-1])
}

func TestCWSpeedConcurrentUsersColdAndWarm(t *testing.T) {
	for _, tc := range []struct {
		name        string
		warm, equal bool
	}{
		{"cold-equal", false, true}, {"warm-equal", true, true},
		{"cold-different", false, false}, {"warm-different", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sp := cwSpeedFixture()
			if tc.equal {
				sp.Comment = "POTA"
			}
			if tc.warm {
				_ = sp.FormatDXCluster()
			}
			on := &Client{configuredSettings: filter.SettingsConfiguration{CWSpeed: true}}
			off := &Client{}
			start := make(chan struct{})
			errors := make(chan string, 32)
			var wg sync.WaitGroup
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func(enabled bool) {
					defer wg.Done()
					<-start
					c, want := off, "CW POTA"
					if enabled {
						c, want = on, "CW POTA 22 WPM 30 WPM"
					}
					for range 1000 {
						if got := cwCommentRegion(formatCWSpotForClient(c, sp)); got != want {
							errors <- got
							return
						}
					}
				}(i%2 == 0)
			}
			close(start)
			wg.Wait()
			close(errors)
			for got := range errors {
				t.Errorf("mixed-user display: %q", got)
			}
		})
	}
}

func TestCWSpeedHistorySignedTextAndDiagnostics(t *testing.T) {
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1}
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	sp := cwSpeedFixture()
	sp.Time = time.Now().UTC()
	sp.HasReport, sp.Report = true, 5
	sp.Comment, sp.CWDisplayComment, sp.CWSpeeds = "+22 WPM", "+22 WPM", nil
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
	for _, enabled := range []bool{false, true} {
		c.filterMu.Lock()
		c.configuredSettings.CWSpeed = enabled
		c.filterMu.Unlock()
		line := runHistory(t, s, c, "SHOW DX 1 COMMENT +22")
		if !strings.Contains(line, "CW 5 dB +22 WPM") {
			t.Fatalf("speed enabled=%v: lost ordinary signed text or matching: %q", enabled, line)
		}
	}
	c.setDiagMode(diagModeSource)
	line := runHistory(t, s, c, "SHOW DX 1 COMMENT +22")
	if !strings.Contains(line, "CW 5 dB +22 WPM") {
		t.Fatalf("diagnostic history lost ordinary signed text: %q", line)
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
