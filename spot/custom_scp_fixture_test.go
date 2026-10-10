package spot

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// The exporter records what actually happened, even on the defective baseline.
// Setup success is separate from preservation: the consumer must receive the
// generated DB and apply its own fixed positive delivery expectation.
func TestCustomSCPCleanupFixtureProducer(t *testing.T) {
	root, runID := os.Getenv("GOCLUSTER_CUSTOMSCP_FIXTURE"), os.Getenv("GOCLUSTER_CUSTOMSCP_RUN_ID")
	if root == "" && runID == "" {
		root, runID = t.TempDir(), "ordinary-test"
	} else if root == "" || runID == "" {
		t.Fatal("fixture directory and run identity are both required")
	}
	if _, err := os.Stat(filepath.Join(root, "ready.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture must be a fresh run without an existing ready marker")
	}
	path := filepath.Join(root, "scp")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture database must not already exist")
	}
	opts := CustomSCPOptions{Path: path, HorizonDays: 1, StaticHorizonDays: 1, CoreMinScore: 1, CoreMinH3Cells: 2}
	s, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	key := customSCPKey{call: "K1HST", band: "20m", bucket: "cw"}
	s.recordObservation("K1HST", "20m", "CW", "N0BBB", 202, 10, true, now)
	old := now.Add(-48 * time.Hour).Unix()
	setRawObservation(t, s, key, "N0AAA", old, 99)
	static := make([]byte, 8)
	binary.BigEndian.PutUint64(static, uint64(old))
	if err := s.db.Set([]byte("m|K1STL"), static, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	refresh := func() {
		s.recordObservation("K1HST", "20m", "CW", "N0AAA", 101, 10, true, now)
		s.recordObservation("K1STL", "20m", "CW", "N0CCC", 303, 10, true, now)
		// Prove successful refresh before cleanup gets its obsolete candidates.
		if !customSCPFixtureExactValue(t, s.db, observationKeyString(key, "N0AAA"), now.Unix(), 101) ||
			!customSCPFixtureExactValue(t, s.db, "m|K1STL", now.Unix(), 0) {
			t.Fatal("counterexample setup did not successfully persist the refresh")
		}
	}
	exerciseCustomSCPFixture(t, s, now, refresh)
	observationPresent := customSCPFixtureExactValue(t, s.db, observationKeyString(key, "N0AAA"), now.Unix(), 101)
	staticPresent := customSCPFixtureExactValue(t, s.db, "m|K1STL", now.Unix(), 0)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCustomSCPStore(opts)
	if err != nil {
		t.Fatal(err)
	}
	support := reopened.RecentSupportCount("K1HST", "20m", "CW", now)
	t.Logf("reopened exact observation=%v static=%v", customSCPFixtureExactValue(t, reopened.db, observationKeyString(key, "N0AAA"), now.Unix(), 101),
		customSCPFixtureExactValue(t, reopened.db, "m|K1STL", now.Unix(), 0))
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := struct {
		RunID              string `json:"run_id"`
		TimeUnix           int64  `json:"time_unix"`
		ObservationPresent bool   `json:"observation_present"`
		StaticPresent      bool   `json:"static_present"`
		RecoveredSupport   int    `json:"recovered_support"`
	}{runID, now.Unix(), observationPresent, staticPresent, support}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ready.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("completed fixture run=%s observation_present=%v static_present=%v reopened_qualified_support=%d", runID, observationPresent, staticPresent, support)
}

func customSCPFixtureExactValue(t *testing.T, db *pebble.DB, key string, at int64, cell uint16) bool {
	t.Helper()
	value, closer, err := db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		t.Logf("raw persisted %s: absent", key)
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	t.Logf("raw persisted %s: %x", key, value)
	wantSize := 10
	if cell == 0 {
		wantSize = 8
	}
	return len(value) == wantSize && int64(binary.BigEndian.Uint64(value[:8])) == at && (cell == 0 || binary.BigEndian.Uint16(value[8:]) == cell)
}

// The connected runner replaces only this scheduling function on the baseline.
// Production has no test hook. Capture a real iterator snapshot, refresh after
// discovery, and apply those actual candidates through the repaired path.
func exerciseCustomSCPFixture(t *testing.T, s *CustomSCPStore, now time.Time, refresh func()) {
	t.Helper()
	iter, err := s.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	iter.First()
	candidates := scanCustomSCPCleanupChunk(iter, s.observationHorizonCutoffUnix(now), s.staticHorizonCutoffUnix(now))
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected exactly two stale snapshot candidates, got %d", len(candidates))
	}
	refresh()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.applyCleanupDeletes(candidates)
}
