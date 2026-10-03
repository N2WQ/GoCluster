//go:build sqlite3_qualification

package peer

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

func TestV15TopologyFailedInitializationRetainsReservation(t *testing.T) {
	var blocked atomic.Bool
	blocked.Store(true)
	ctx := sqlite3.WithRetirementBlockedForQualification(t.Context(), &blocked)
	path := filepath.Join(t.TempDir(), "partial.db")
	db, err := openTopologyDatabase(ctx, path+"?_pragma=invalid(")
	if err == nil || db != nil {
		t.Fatalf("partial initialization accepted: %v", err)
	}
	owner := topologyReservation.Load()
	if owner == nil || owner.conn == nil || !owner.failedCleanup.Load() || owner.state.Load() != topologyRetiring {
		t.Fatal("constructor forgot failed owner")
	}
	t.Cleanup(func() {
		blocked.Store(false)
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	usage := owner.usage.Load()
	if usage == nil || usage.EngineBacking.Load() != topologyEngineBytes {
		t.Fatal("failure no longer owns real backing")
	}
	if _, err = openTopologyDatabase(context.Background(), filepath.Join(t.TempDir(), "replacement.db")); !errors.Is(err, errTopologyReservation) {
		t.Fatalf("failed owner allowed replacement: %v", err)
	}
	if topologyReservation.Load() != owner || usage.EngineBacking.Load() != topologyEngineBytes {
		t.Fatal("rejected replacement changed failed owner's charge")
	}
	blocked.Store(false)
	next, err := openTopologyDatabase(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if usage.EngineBacking.Load() != 0 || next == owner {
		t.Fatal("replacement preceded verified old retirement")
	}
}

func TestV15TopologyDisabledManagerObservesFailedOwner(t *testing.T) {
	var blocked atomic.Bool
	blocked.Store(true)
	ctx := sqlite3.WithRetirementBlockedForQualification(t.Context(), &blocked)
	db, err := openTopologyDatabase(ctx, filepath.Join(t.TempDir(), "retained.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { blocked.Store(false); db.Close() })
	db.projectionCommits.Store(17)
	if err = db.Close(); err == nil {
		t.Fatal("cleanup fault not exercised")
	}
	m := newProtocolTestManager(t)
	s := m.TopologyPersistenceStats()
	if s.Enabled || s.ProjectionCommits != 0 || s.LegacyCommits != 0 || s.ReservedBytes != topologyPersistenceBytes || s.EngineBackingBytes != topologyEngineBytes || s.Retiring != 1 || !s.CleanupFailed {
		t.Fatalf("disabled manager hid global owner or inherited commits: %+v", s)
	}
	blocked.Store(false)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s = m.TopologyPersistenceStats()
	if s.ReservedBytes != 0 || s.EngineBackingBytes != 0 || s.Retiring != 0 || s.CleanupFailed {
		t.Fatalf("verified retirement remains charged: %+v", s)
	}
}

func TestV15TopologyLifecycle1000(t *testing.T) {
	for _, mode := range topologyQualificationModes() {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(func() {
				if owner := topologyReservation.Load(); owner != nil && owner != &topologyOpeningReservation {
					owner.Close()
				}
			})
			path := filepath.Join(t.TempDir(), "cycles.db")
			var counts [4]int
			for cycle := 0; cycle < 1000; cycle++ {
				kind := cycle % 4
				ctx, cancel := context.WithTimeout(topologyQualificationContext(t.Context(), mode), topologyDBTimeout)
				name := path
				if kind == 2 {
					name += "?_pragma=invalid("
				}
				db, err := openTopologyDatabase(ctx, name)
				if kind == 2 {
					cancel()
					if err == nil || db != nil || topologyReservation.Load() != nil {
						t.Fatalf("cycle %d partial initialization: db=%v err=%v", cycle, db, err)
					}
					counts[kind]++
					continue
				}
				if err != nil {
					cancel()
					t.Fatalf("cycle %d open: %v", cycle, err)
				}
				usage := db.usage.Load()
				switch kind {
				case 0:
					err = db.run(ctx, func(c *sqlite3.Conn) error {
						return c.Exec("create table if not exists kept(v);insert into kept values(1)")
					})
				case 1:
					err = db.run(ctx, func(c *sqlite3.Conn) error {
						cancel()
						return c.Exec("with recursive n(x) as(values(0) union all select x+1 from n where x<100000000) select sum(x) from n")
					})
					if err == nil {
						t.Fatalf("cycle %d failed to cancel actual engine operation", cycle)
					}
				case 3:
					err = db.run(ctx, func(c *sqlite3.Conn) error { return c.Exec("select randomblob(16777216)") })
					if !errors.Is(err, errTopologyBudget) {
						t.Fatalf("cycle %d actual OOM=%v", cycle, err)
					}
				}
				cancel()
				if kind == 0 && err != nil {
					t.Fatalf("cycle %d successful operation: %v", cycle, err)
				}
				if err = db.Close(); err != nil {
					t.Fatalf("cycle %d retirement: %v", cycle, err)
				}
				if topologyReservation.Load() != nil || usage.EngineBacking.Load() != 0 || usage.WALViews.Load() != 0 || usage.WALSlots.Load() != 0 {
					t.Fatalf("cycle %d retained allocation or reservation", cycle)
				}
				counts[kind]++
			}
			for kind, count := range counts {
				if count != 250 {
					t.Fatalf("kind %d count %d", kind, count)
				}
			}
			t.Logf("normal/cancel/partial-init/actual-OOM cycles=%v; all owners retired", counts)
		})
	}
}

func TestV15TopologyCleanupFailure250(t *testing.T) {
	for _, mode := range topologyQualificationModes() {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "retirement.db")
			var blocked atomic.Bool
			t.Cleanup(func() {
				blocked.Store(false)
				if owner := topologyReservation.Load(); owner != nil && owner != &topologyOpeningReservation {
					owner.Close()
				}
			})
			for cycle := 0; cycle < 250; cycle++ {
				blocked.Store(true)
				ctx := sqlite3.WithRetirementBlockedForQualification(topologyQualificationContext(t.Context(), mode), &blocked)
				db, err := openTopologyDatabase(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				usage := db.usage.Load()
				if err = db.Close(); err == nil || topologyReservation.Load() != db || !db.failedCleanup.Load() || usage.EngineBacking.Load() != topologyEngineBytes {
					t.Fatalf("cycle %d failed cleanup lost live ownership: %v", cycle, err)
				}
				if _, err = openTopologyDatabase(t.Context(), path); !errors.Is(err, errTopologyReservation) {
					t.Fatalf("cycle %d admitted overlapping engine: %v", cycle, err)
				}
				blocked.Store(false)
				if err = db.Close(); err != nil || topologyReservation.Load() != nil || usage.EngineBacking.Load() != 0 || usage.WALSlots.Load() != 0 {
					t.Fatalf("cycle %d verified retirement failed: %v", cycle, err)
				}
			}
			t.Log("250 real allocated owner cleanup-failure/retry cycles; reservation retained throughout each fault")
		})
	}
}

func TestV15TopologyRealSQLCancellation(t *testing.T) {
	db, err := openTopologyDatabase(t.Context(), filepath.Join(t.TempDir(), "cancel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = db.run(ctx, func(c *sqlite3.Conn) error {
		return c.Exec("with recursive n(x) as(values(0) union all select x+1 from n where x<100000000) select sum(x) from n")
	})
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("SQL deadline not enforced: %v elapsed=%s", err, time.Since(start))
	}
	if err = db.run(t.Context(), func(c *sqlite3.Conn) error { return c.Exec("create table after_cancel(v)") }); err != nil {
		t.Fatal("cancellation prevented subsequent operation", err)
	}
}
