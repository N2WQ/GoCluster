package rbn

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
)

func TestRBNCWSpeedActualIngestArchiveReopen(t *testing.T) {
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1}
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 10)
	ts := time.Now().UTC().Format("1504Z")
	for _, payload := range []string{"22 30 WPM", "0 WPM"} {
		c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX CW 5 dB " + payload + " CQ " + ts)
		w.Enqueue(mustReadSpot(t, c))
	}
	w.Stop()
	reopened, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(5)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	for _, row := range rows {
		if !row.CWDisplayAvailable || len(row.CWSpeeds) != 1 {
			t.Fatalf("missing source projection: %+v", row)
		}
		switch row.CWSpeeds[0] {
		case "30 WPM":
			if row.Comment != "22 WPM 30" || row.CWDisplayComment != "22" || !strings.Contains(row.FormatDXClusterCW(true), "22 30 WPM") || strings.Contains(row.FormatDXClusterCW(false), "WPM") {
				t.Fatalf("30 WPM projection changed: %+v", row)
			}
		case "0 WPM":
			if row.Comment != "" || row.CWDisplayComment != "" || !reflect.DeepEqual(row.CWSpeeds, []string{"0 WPM"}) || !strings.Contains(row.FormatDXClusterCW(true), "0 WPM") || strings.Contains(row.FormatDXClusterCW(false), "WPM") {
				t.Fatalf("zero WPM projection changed: %+v", row)
			}
		default:
			t.Fatalf("unexpected speed %q", row.CWSpeeds[0])
		}
	}
}
