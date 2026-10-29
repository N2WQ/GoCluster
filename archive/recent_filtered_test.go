package archive

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestRecentFilteredDeepScan(t *testing.T) {
	dir := t.TempDir()
	cfg := config.ArchiveConfig{
		DBPath:              dir,
		AutoDeleteCorruptDB: true,
		Synchronous:         "off",
	}
	db, err := openArchiveDB(cfg)
	if err != nil {
		t.Fatalf("open archive db: %v", err)
	}
	defer db.Close()

	writeOpts, err := archiveWriteOptions(cfg)
	if err != nil {
		t.Fatalf("write opts: %v", err)
	}

	batch := db.NewBatch()
	defer batch.Close()

	const (
		total      = 12000
		matchEvery = 1000
		limit      = 10
	)
	start := time.Now().UTC().Add(-time.Duration(total) * time.Second)
	seq := uint32(0)
	for i := 0; i < total; i++ {
		dx := "MISS"
		if i%matchEvery == 0 {
			dx = "MATCH"
		}
		s := &spot.Spot{
			DXCall:    dx,
			DECall:    "DE1AA",
			Frequency: 7000.0,
			Mode:      "CW",
			Time:      start.Add(time.Duration(i) * time.Second),
		}
		key := spotKeyBytes(normalizeUnixNano(s.Time), seq)
		seq++
		if err := batch.Set(key, encodeRecord(s), nil); err != nil {
			t.Fatalf("batch set: %v", err)
		}
	}
	if err := batch.Commit(writeOpts); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	w := &Writer{db: db}
	match := func(s *spot.Spot) bool {
		return s != nil && s.DXCall == "MATCH"
	}
	got, err := w.RecentFiltered(limit, match)
	if err != nil {
		t.Fatalf("RecentFiltered: %v", err)
	}
	if len(got) != limit {
		t.Fatalf("expected %d results, got %d", limit, len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Time.After(got[i-1].Time) {
			t.Fatalf("expected newest-first order, got %s after %s", got[i].Time, got[i-1].Time)
		}
	}
}

func TestRecentFilteredAppliesEventFilter(t *testing.T) {
	dir := t.TempDir()
	cfg := config.ArchiveConfig{
		DBPath:              dir,
		AutoDeleteCorruptDB: true,
		Synchronous:         "off",
	}
	db, err := openArchiveDB(cfg)
	if err != nil {
		t.Fatalf("open archive db: %v", err)
	}
	defer db.Close()

	writeOpts, err := archiveWriteOptions(cfg)
	if err != nil {
		t.Fatalf("write opts: %v", err)
	}
	batch := db.NewBatch()
	defer batch.Close()

	now := time.Now().UTC()
	spots := []*spot.Spot{
		{
			DXCall:    "K1POTA",
			DECall:    "DE1AA",
			Frequency: 14074.0,
			Mode:      "CW",
			Time:      now.Add(-2 * time.Second),
			Comment:   "POTA-1234",
			Events:    spot.EventPOTA,
		},
		{
			DXCall:    "K1SOTA",
			DECall:    "DE1AA",
			Frequency: 14074.0,
			Mode:      "CW",
			Time:      now.Add(-1 * time.Second),
			Comment:   "SOTA-ABC",
			Events:    spot.EventSOTA,
		},
	}
	for i, s := range spots {
		key := spotKeyBytes(normalizeUnixNano(s.Time), uint32(i))
		if err := batch.Set(key, encodeRecord(s), nil); err != nil {
			t.Fatalf("batch set: %v", err)
		}
	}
	if err := batch.Commit(writeOpts); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	_, f, err := filter.PrepareV6Configuration(filter.V6Configuration{Filters: filter.V6FilterConfiguration{
		Rules: []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "EVENT=POTA AND SOURCE=HUMAN,SKIMMER"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := (&Writer{db: db}).RecentFiltered(10, f.Matches)
	if err != nil {
		t.Fatalf("RecentFiltered: %v", err)
	}
	if len(got) != 1 || got[0].DXCall != "K1POTA" {
		t.Fatalf("expected only K1POTA history result, got %+v", got)
	}
}
