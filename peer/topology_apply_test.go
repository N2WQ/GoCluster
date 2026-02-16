package peer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyPC92ParsesEntriesAfterType(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "topo.db")
	store, err := openTopologyStore(dbPath, time.Hour)
	if err != nil {
		t.Fatalf("openTopologyStore: %v", err)
	}
	defer store.Close()

	now := time.Unix(1700000000, 0)

	// Frame layout: origin=NODE1, ts=123, type=A, entry=9CALL:ver:build:ip
	frame, err := ParseFrame("PC92^NODE1^123^A^^9CALL:ver:build:1.2.3.4^H10^")
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	store.applyPC92(context.Background(), frame, now)

	var count int
	var origin, call, version, build, ip string
	row := store.db.QueryRowContext(context.Background(), `select count(*), origin, call, version, build, ip from peer_nodes`)
	if err := row.Scan(&count, &origin, &call, &version, &build, &ip); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
	if origin != "NODE1" || call != "CALL" || version != "ver" || build != "build" || ip != "1.2.3.4" {
		t.Fatalf("unexpected row data: origin=%s call=%s version=%s build=%s ip=%s", origin, call, version, build, ip)
	}

	// Type D should delete the row.
	frameDel, err := ParseFrame("PC92^NODE1^124^D^^9CALL:ver:build:1.2.3.4^H10^")
	if err != nil {
		t.Fatalf("ParseFrame delete: %v", err)
	}
	store.applyPC92(context.Background(), frameDel, now)
	row = store.db.QueryRowContext(context.Background(), `select count(*) from peer_nodes`)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan count after delete: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected delete to remove row, still have %d", count)
	}

	// Ensure multiple entries are all processed.
	frameMulti, err := ParseFrame("PC92^NODE2^125^A^^9A:verA:::1.1.1.1^8B:verB^H5^")
	if err != nil {
		t.Fatalf("ParseFrame multi: %v", err)
	}
	store.applyPC92(context.Background(), frameMulti, now)
	row = store.db.QueryRowContext(context.Background(), `select count(*) from peer_nodes where origin='NODE2'`)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan multi count: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows for NODE2, got %d", count)
	}

	// Clean up temp DB file explicitly to avoid Windows file lock surprises in CI.
	_ = store.Close()
	_ = os.Remove(dbPath)
}
