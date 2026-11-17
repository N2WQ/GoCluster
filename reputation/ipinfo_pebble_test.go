package reputation

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIPInfoPebbleImportAndLookup(t *testing.T) {
	tmp := t.TempDir()
	csvPath := filepath.Join(tmp, "ipinfo.csv")
	lines := []string{
		"network,country,country_code,continent,continent_code,asn",
		"203.0.113.0/24,United States,US,North America,NA,AS64500",
		"2001:db8::/32,Testland,TL,Test,TS,AS64501",
	}
	if err := os.WriteFile(csvPath, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	root := filepath.Join(tmp, "pebble")
	dbPath, err := buildIPInfoPebble(context.Background(), csvPath, root, true)
	if err != nil {
		t.Fatalf("buildIPInfoPebble: %v", err)
	}
	if err := updateIPInfoPebbleCurrent(t.Context(), root, dbPath); err != nil {
		t.Fatalf("update current: %v", err)
	}
	store, err := openIPInfoStore(dbPath, 0)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	v4 := netip.MustParseAddr("203.0.113.42")
	res4, ok := store.lookupV4(v4)
	if !ok || res4.ASN != "64500" || res4.CountryCode != "US" {
		t.Fatalf("v4 lookup mismatch: ok=%t res=%+v", ok, res4)
	}
	v6 := netip.MustParseAddr("2001:db8::1")
	res6, ok := store.lookupV6(v6)
	if !ok || res6.ASN != "64501" || res6.CountryCode != "TL" {
		t.Fatalf("v6 lookup mismatch: ok=%t res=%+v", ok, res6)
	}

	index, err := loadIPv4IndexFromStore(t.Context(), store)
	if err != nil {
		t.Fatalf("load ipv4 index: %v", err)
	}
	if index == nil || index.loadedAt.IsZero() {
		t.Fatalf("expected ipv4 index with loadedAt")
	}
	if _, ok := index.lookup(v4); !ok {
		t.Fatalf("ipv4 index lookup failed")
	}
	if index.loadedAt.After(time.Now().Add(time.Minute)) {
		t.Fatalf("unexpected index loadedAt: %s", index.loadedAt)
	}
}
