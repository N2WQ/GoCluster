package cluster

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"
)

func TestCWSpeedBeaconArchiveSnapshotReopen(t *testing.T) {
	cfg := config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1}
	w, err := archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	sp := spot.NewSpot("K1ABC", "W1AW", 14025, "CW")
	sp.Time = time.Now().UTC()
	sp.SourceType = spot.SourceRBN
	sp.IsBeacon, sp.BeaconSourceClass = true, true
	sp.BeaconComment = spot.BeaconCommentNCDXF
	sp.CWDisplayAvailable = true
	sp.CWSpeeds = []string{"0 WPM", "30 WPM"}
	snapshot := archiveSnapshotForSpot(sp)
	if snapshot == sp || sp.Comment != "" || snapshot.Comment != spot.BeaconCommentNCDXF {
		t.Fatalf("beacon snapshot did not preserve ownership: source=%q snapshot=%q", sp.Comment, snapshot.Comment)
	}
	w.EnqueueOwned(snapshot)
	w.Stop()
	w, err = archive.NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	rows, err := w.Recent(1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("reopen: rows=%d err=%v", len(rows), err)
	}
	got := rows[0]
	if !got.CWDisplayAvailable || len(got.CWSpeeds) != 2 || got.CWSpeeds[0] != "0 WPM" || got.CWSpeeds[1] != "30 WPM" {
		t.Fatalf("reopened beacon lost speeds: %+v", got)
	}
	for _, warm := range []bool{false, true} {
		if warm {
			_ = got.FormatDXCluster()
		}
		if off := got.FormatDXClusterCW(false); !strings.Contains(off, "NCDXF BEACON") || strings.Contains(off, "0 WPM") {
			t.Fatalf("bad disabled display (warm=%v): %q", warm, off)
		}
	}
	if on := got.FormatDXClusterCW(true); !strings.Contains(on, "NCDXF BEACON") || !strings.Contains(on, "0 WPM") {
		t.Fatalf("bad enabled display: %q", on)
	}
}
