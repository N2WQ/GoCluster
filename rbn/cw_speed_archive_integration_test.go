package rbn

import (
	"path/filepath"
	"reflect"
	"strings"
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

func TestRBNCWPunctuatedUnicodeUnitArchiveReopen(t *testing.T) {
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1}
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 4)
	ts := time.Now().UTC().Format("1504Z")
	for _, tc := range []struct{ call, payload string }{
		{"OZ8ADX", "CW " + ts + " 5 dB 22 WPM,\u2003 CQ"},
		{"OZ9ADX", "CW 5 dB 22 " + ts + "WPM,\u2003 CQ"},
		{"OZ0ADX", "CW 5 dB 22 ıııı FOO WPMX CQ " + ts},
	} {
		c.parseSpot("DX de K1ABC: 3547.0 " + tc.call + " " + tc.payload)
		w.Enqueue(mustReadSpot(t, c))
	}
	w.Stop()
	reopened, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(4)
	if err != nil || len(rows) != 3 {
		t.Fatalf("reopen: rows=%d err=%v", len(rows), err)
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		if seen[row.DXCall] {
			t.Fatalf("duplicate archived call %q", row.DXCall)
		}
		seen[row.DXCall] = true
		if row.DXCall == "OZ0ADX" {
			if !row.HasReport || row.Report != 5 || row.Comment != "22 WPM ıııı WPMX" || !row.CWDisplayAvailable || row.CWDisplayComment != "22 ıııı FOO WPMX" || len(row.CWSpeeds) != 0 {
				t.Fatalf("reopened Unicode-offset spot: %+v", row)
			}
			for _, show := range []bool{false, true} {
				if got := commentField(row.FormatDXClusterCW(show)); !strings.Contains(got, "FOO") {
					t.Errorf("reopened Unicode-offset spot show=%v: %q", show, got)
				}
			}
			continue
		}
		if row.DXCall != "OZ8ADX" && row.DXCall != "OZ9ADX" {
			t.Fatalf("unexpected archived call %q", row.DXCall)
		}
		if !row.HasReport || row.Report != 5 || row.Comment != "" || !row.CWDisplayAvailable || row.CWDisplayComment != "22 WPM," || len(row.CWSpeeds) != 0 {
			t.Fatalf("reopened spot: %+v", row)
		}
		for _, show := range []bool{false, true} {
			if got := commentField(row.FormatDXClusterCW(show)); got != "CW 5 dB 22 WPM," {
				t.Errorf("%s show=%v: %q", row.DXCall, show, got)
			}
		}
	}
}
