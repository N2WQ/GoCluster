package commands

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
)

func TestManualHumanCommentRoutingAndArchiveSearch(t *testing.T) {
	input := make(chan *spot.Spot, 1)
	p := NewProcessor(nil, nil, input, nil, nil, nil)
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1}
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	t.Cleanup(w.Stop)
	for _, tc := range []struct {
		line, mode, comment string
		report              int
		present             bool
	}{
		{"DX 14025 K1ABC CW UP 2 QRT 1800Z CWT POTA", "CW", "UP 2 QRT 1800Z CWT POTA", 0, false},
		{"DX K1ABC 14074 FT8 -14 POTA", "FT8", "POTA", -14, true},
		{"DX 14074 K1ABC FT8 UP 2 -12 dB POTA", "FT8", "UP 2 POTA", -12, true},
		{"DX 14074 K1ABC FT8 S9 +10 dB POTA", "FT8", "S9 +10 dB POTA", 0, false},
	} {
		if response := p.ProcessCommandForClient(tc.line, "W1AW", "", nil, "go"); response != "Spot queued.\n" {
			t.Fatalf("manual DX refused: %q", response)
		}
		var got *spot.Spot
		select {
		case got = <-input:
		default:
			t.Fatal("manual DX did not enqueue")
		}
		if got.Mode != tc.mode || got.Comment != tc.comment || got.Report != tc.report || got.HasReport != tc.present || !got.IsHuman || got.Events != spot.EventPOTA || manualDXMode(strings.Fields(tc.line), got.Frequency) != tc.mode {
			t.Fatalf("manual human metadata: %+v", got)
		}
		w.Enqueue(got)
	}
	w.Stop() // A real flush and reopen, not an in-memory fake history reader.
	w, err = archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	p.archive = w
	db := historyCanonicalCTY(t)
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	for _, phrase := range []string{"UP 2", "1800Z", "CWT"} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX K1ABC MODE CW COMMENT "+phrase, "go")
		if !handled || text != "" {
			t.Fatalf("history phrase %q: handled=%v text=%q", phrase, handled, text)
		}
		page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, time.Now().UTC(), nil)
		if err != nil || len(page.Spots) != 1 || page.Spots[0].Comment != "UP 2 QRT 1800Z CWT POTA" || page.Spots[0].HasReport {
			t.Fatalf("stored human phrase %q: %+v error=%v", phrase, page, err)
		}
	}
	command, handled, text := p.ParseHistoryCommand("SHOW DX K1ABC MODE FT8", "go")
	if !handled || text != "" {
		t.Fatalf("FT8 history parse: %v %q", handled, text)
	}
	page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, time.Now().UTC(), nil)
	if err != nil || len(page.Spots) != 3 {
		t.Fatalf("persisted human reports: %+v error=%v", page, err)
	}
	want := map[string]int{"POTA": -14, "UP 2 POTA": -12, "S9 +10 dB POTA": 0}
	for _, got := range page.Spots {
		report, ok := want[got.Comment]
		if !ok || got.Report != report || got.HasReport != (got.Comment != "S9 +10 dB POTA") {
			t.Fatalf("stored report/comment mismatch: %+v", got)
		}
		delete(want, got.Comment)
	}
}
