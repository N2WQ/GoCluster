//go:build sqlite3_qualification && windows

package sqlite3

import (
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
	"path/filepath"
	"testing"
)

func TestV15SQLiteFallbackVFSReleaseRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry.db")
	c, err := OpenTopologyContext(WithFallbackForQualification(WithMaxMemory(t.Context(), 8<<20)), path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Retire()
	if err = c.Exec("pragma journal_mode=WAL;create table saved(v);insert into saved values('committed')"); err != nil {
		t.Fatal(err)
	}
	usage := c.Ownership()
	restore := sqlite3_wrap.FailFallbackHandleCloseForQualification()
	defer restore()
	for attempt := 0; attempt < 2; attempt++ {
		if err = c.Close(); err == nil {
			t.Fatal("native handle close fault was not observed")
		}
		if usage.WALViews.Load() != 0 || usage.WALSlots.Load() == 0 || usage.WALShadows.Load() == 0 || usage.EngineBacking.Load() != 8<<20 {
			t.Fatalf("partial close lost owner: view=%d slots=%d shadows=%d engine=%d", usage.WALViews.Load(), usage.WALSlots.Load(), usage.WALShadows.Load(), usage.EngineBacking.Load())
		}
	}
	restore()
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if usage.WALSlots.Load() != 0 || usage.WALShadows.Load() != 0 || usage.EngineBacking.Load() != 0 {
		t.Fatal("verified close retained backing")
	}
	c, err = OpenTopologyContext(WithFallbackForQualification(WithMaxMemory(t.Context(), 8<<20)), path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, _, err := c.Prepare("select v from saved")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.Step() || s.ColumnText(0) != "committed" {
		t.Fatal("saved commit changed after partial close", s.Err())
	}
}

func TestV15SQLiteFallbackWAL(t *testing.T) {
	c, err := OpenContext(WithFallbackForQualification(WithMaxMemory(t.Context(), 8<<20)), filepath.Join(t.TempDir(), "wal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.wrp.CanMapFiles() {
		t.Fatal("fixture failed to select fallback")
	}
	if err = c.Exec("pragma journal_mode=WAL; create table x(v); begin; insert into x values('committed'); commit"); err != nil {
		t.Fatal(err)
	}
	stmt, _, err := c.Prepare("select v from x")
	if err != nil {
		t.Fatal(err)
	}
	if !stmt.Step() || stmt.ColumnText(0) != "committed" {
		t.Fatalf("WAL contents err=%v", stmt.Err())
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}
