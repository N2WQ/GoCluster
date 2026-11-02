package rbn

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"
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
	tests := []struct {
		call, payload, canonical, display, off, on string
		report                                     int
		speeds                                     []string
	}{
		{"OZ4ADX", "CW 5 dB 22 30 WPM CQ", "22 WPM 30", "22", "CW 5 dB 22", "CW 5 dB 22 30 WPM", 5, []string{"30 WPM"}},
		{"OZ5ADX", "CW 5 dB 0 WPM CQ", "", "", "CW 5 dB", "CW 5 dB 0 WPM", 5, []string{"0 WPM"}},
		{"OZ6ADX", "CW +22 WPM 5 dB CQ", "+22 WPM", "+22 WPM", "CW 5 dB +22 WPM", "CW 5 dB +22 WPM", 5, nil},
		{"OZ7ADX", "CW 20 WPM 30 WPM 5 dB CQ", "20 WPM WPM 5", "5", "CW 30 dB 5", "CW 30 dB 5 20 WPM", 30, []string{"20 WPM", "30 WPM"}},
	}
	for _, tc := range tests {
		c.parseSpot("DX de K1ABC: 3547.0 " + tc.call + " " + tc.payload + " " + ts)
		w.Enqueue(mustReadSpot(t, c))
	}
	w.Stop()
	reopened, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(10)
	if err != nil || len(rows) != len(tests) {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	byCall := make(map[string]*spot.Spot, len(rows))
	for _, row := range rows {
		byCall[row.DXCall] = row
	}
	for _, tc := range tests {
		row := byCall[tc.call]
		if row == nil || !row.HasReport || row.Report != tc.report || row.Comment != tc.canonical || !row.CWDisplayAvailable || row.CWDisplayComment != tc.display || !reflect.DeepEqual(row.CWSpeeds, tc.speeds) {
			t.Fatalf("%s reopened spot: %+v", tc.call, row)
		}
		if got := commentField(row.FormatDXClusterCW(false)); got != tc.off {
			t.Errorf("%s OFF: got %q want %q", tc.call, got, tc.off)
		}
		if got := commentField(row.FormatDXClusterCW(true)); got != tc.on {
			t.Errorf("%s ON: got %q want %q", tc.call, got, tc.on)
		}
	}
}
