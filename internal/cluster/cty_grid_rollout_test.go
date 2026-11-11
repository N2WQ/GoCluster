package cluster

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/gridstore"
	"dxcluster/internal/pebbleresilience"
)

func startCTYRolloutGrid(t *testing.T, path string, db *cty.CTYDatabase) *clusterRuntime {
	t.Helper()
	enabled := true
	r := &clusterRuntime{ctx: t.Context(), cfg: &config.Config{
		GridDBPath: path, GridDBCheckOnMiss: &enabled, GridFlushSec: 3600,
		GridBlockCacheMB: 1, GridMemTableSizeMB: 1, GridL0Compaction: 2,
		GridL0StopWrites: 4, GridWriteQueueDepth: 4,
	}, gridStoreHandle: newGridStoreHandle(nil), metaCache: newCallMetaCache(64, 0)}
	r.ctyDB.Store(db)
	t.Cleanup(r.retireGridStore)
	if !r.initializeGridStore() {
		t.Fatalf("disposable startup: %v", r.startupErr)
	}
	return r
}

func assertRolloutRows(t *testing.T, store *gridstore.Store, legacy bool) {
	t.Helper()
	want := []struct {
		call, grid string
		derived    bool
	}{{"K1ABC", "FN42", true}, {"W6NEW", "DM04AA", false}}
	absent := []string{"JA1OLD", "W6OLD"}
	if legacy {
		want = []struct {
			call, grid string
			derived    bool
		}{{"JA1OLD", "CM05", true}, {"W6OLD", "DM05AA", false}}
		absent = []string{"K1ABC", "W6NEW"}
	}
	if count, err := store.Count(); err != nil || count != 2 {
		t.Fatalf("row count = %d/%v, want exactly two", count, err)
	}
	for _, tc := range want {
		rec, err := store.Get(tc.call)
		if err != nil || rec == nil || !rec.Grid.Valid || rec.Grid.String != tc.grid || rec.GridDerived != tc.derived {
			t.Fatalf("%s = %+v/%v, want %s derived=%v", tc.call, rec, err, tc.grid, tc.derived)
		}
	}
	for _, call := range absent {
		if rec, err := store.Get(call); err != nil || rec != nil {
			t.Fatalf("unexpected carried row %s: %+v/%v", call, rec, err)
		}
	}
}

// All paths belong to one disposable root. This drives production grid startup,
// checkpoint verification and candidate selection; distinct names alone would
// not prove that legacy recovery candidates cannot repopulate a fresh store.
func TestCTYGridDisposableCutoverAndRollback(t *testing.T) {
	t.Setenv(envGridDBCheckOnMiss, "true")
	ctx := context.Background()
	root := t.TempDir()
	legacyPath, backupPath := filepath.Join(root, "legacy"), filepath.Join(root, "verified-backup")
	activePath := filepath.Join(root, "fresh-active")
	legacyCheckpointTime := time.Now().UTC().Add(24 * time.Hour)
	opts := gridRetirementStoreOptions()
	func() {
		legacy, err := gridstore.Open(legacyPath, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer legacy.Close()
		now := time.Now().UTC()
		if err := legacy.UpsertBatch([]gridstore.Record{
			{Call: "JA1OLD", Grid: sql.NullString{String: "CM05", Valid: true}, GridDerived: true, FirstSeen: now, UpdatedAt: now},
			{Call: "W6OLD", Grid: sql.NullString{String: "DM05AA", Valid: true}, FirstSeen: now, UpdatedAt: now},
		}); err != nil {
			t.Fatal(err)
		}
		// A newer timestamp must not make an external legacy tree eligible.
		oldCheckpoint := filepath.Join(gridCheckpointRoot(legacyPath), legacyCheckpointTime.Format(gridCheckpointNameLayoutUTC))
		if err := legacy.Checkpoint(oldCheckpoint); err != nil {
			t.Fatal(err)
		}
		if err := legacy.Checkpoint(backupPath); err != nil {
			t.Fatal(err)
		}
	}()
	stats, err := gridstore.VerifyCheckpoint(ctx, backupPath, 5*time.Second)
	if err != nil || stats.Records != 2 {
		t.Fatalf("backup verification: %+v/%v", stats, err)
	}
	// Inspect a verification copy, leaving the backup itself read-only.
	verification := filepath.Join(root, "verification-copy")
	if err := os.MkdirAll(verification, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := pebbleresilience.CopyDirCtx(ctx, backupPath, verification); err != nil {
		t.Fatal(err)
	}
	func() {
		store, err := gridstore.Open(verification, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		assertRolloutRows(t, store, true)
	}()
	if recovered, selected, err := restoreGridStoreFromCheckpoint(ctx, activePath, opts); err == nil || recovered != nil || selected != "" || !strings.Contains(err.Error(), "no checkpoints available") {
		if recovered != nil {
			_ = recovered.Close()
		}
		t.Fatalf("external legacy checkpoint became eligible: %q/%v", selected, err)
	}
	db := loadShippedClusterCTY(t)
	r := startCTYRolloutGrid(t, activePath, db)
	if count, err := r.gridStoreHandle.Store().Count(); err != nil || count != 0 {
		t.Fatalf("new active store did not start empty: %d/%v", count, err)
	}
	r.gridLookup("K1ABC")
	r.gridUpdater("W6NEW", "DM04aa")
	r.retireGridStore()
	// Retirement cancels and joins the actual background owners. An hourly tick
	// may already have produced an older checkpoint; make the controlled final
	// snapshot strictly newer than every candidate after those owners have joined.
	checkpointTime := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	prior, err := listGridCheckpoints(gridCheckpointRoot(activePath))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range prior {
		if !entry.At.Before(checkpointTime) {
			checkpointTime = entry.At.Add(time.Second)
		}
	}
	if !legacyCheckpointTime.After(checkpointTime) {
		t.Fatal("external negative control must be newer than the selected active checkpoint")
	}
	activeCheckpoint := filepath.Join(gridCheckpointRoot(activePath), checkpointTime.Format(gridCheckpointNameLayoutUTC))
	func() {
		store, err := gridstore.Open(activePath, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		assertRolloutRows(t, store, false)
		if err := store.Checkpoint(activeCheckpoint); err != nil {
			t.Fatal(err)
		}
	}()
	func() {
		store, selected, err := restoreGridStoreFromCheckpoint(ctx, activePath, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if selected != activeCheckpoint {
			t.Fatalf("recovery selected %s, want new recovery set %s", selected, activeCheckpoint)
		}
		assertRolloutRows(t, store, false)
	}()
	restarted := startCTYRolloutGrid(t, activePath, db)
	restarted.gridLookup("K1ABC")
	restarted.retireGridStore()
	if grid, derived, ok := restarted.metaCache.LookupGrid("K1ABC", nil); !ok || grid != "FN42" || !derived {
		t.Fatalf("cold restart did not hydrate derived grid: %s/%v/%v", grid, derived, ok)
	}
	rollbackPath := filepath.Join(root, "rollback-copy")
	if err := os.MkdirAll(rollbackPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := pebbleresilience.CopyDirCtx(ctx, backupPath, rollbackPath); err != nil {
		t.Fatal(err)
	}
	rollback, err := gridstore.Open(rollbackPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback.Close()
	assertRolloutRows(t, rollback, true)
}
