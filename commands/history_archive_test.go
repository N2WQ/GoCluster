package commands

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

// A v2 byte fixture is deliberately persisted without invoking any Spot
// constructor or current archive encoder. Its old DX field retains numeric SSIDs.
func legacyHistoryRecord(call string) []byte {
	fields := []string{call, "W1AAA", "", "CW", "", "", "", "", "", "", "", "", ""}
	raw := make([]byte, 54)
	raw[0] = 2
	binary.BigEndian.PutUint64(raw[4:], math.Float64bits(14030))
	binary.BigEndian.PutUint32(raw[20:], 291)
	for i, value := range fields {
		binary.BigEndian.PutUint16(raw[28+i*2:], uint16(len(value)))
	}
	for _, value := range fields {
		raw = append(raw, value...)
	}
	return raw
}

func TestHistoryRealArchiveLegacyIdentityAndRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i, call := range []string{"K1ABC-1", "K1ABCD", "W6XYZ", "K1ABC/P", "W6/LZ5VV-1", "K1ABC"} {
		key := make([]byte, 14)
		copy(key, "s|")
		ts := now.Add(-time.Duration(i) * time.Second)
		if i == 5 {
			ts = now.Add(-61 * time.Second)
		}
		binary.BigEndian.PutUint64(key[2:], uint64(ts.UnixNano()))
		binary.BigEndian.PutUint32(key[10:], uint32(i))
		if err := db.Set(key, legacyHistoryRecord(call), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := archive.NewWriter(config.ArchiveConfig{DBPath: path, RetentionSeconds: 60, Synchronous: "off"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	dbCTY := historyCanonicalCTY(t)
	p := NewProcessor(nil, w, nil, func() *cty.CTYDatabase { return dbCTY }, nil, nil)
	for _, test := range []struct {
		selector string
		count    int
	}{{"K1ABC", 2}, {"K1ABC-1", 2}, {"W6/LZ5VV", 1}, {"K", 5}, {"", 5}} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX "+test.selector, "go")
		if !handled || text != "" {
			t.Fatalf("%s: %q", test.selector, text)
		}
		page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, now, nil)
		if err != nil || len(page.Spots) != test.count {
			t.Fatalf("%s: %+v %v", test.selector, page, err)
		}
		for _, s := range page.Spots {
			if s.Time.Before(now.Add(-60 * time.Second)) {
				t.Fatal("expired row returned before physical cleanup")
			}
			if strings.HasPrefix(test.selector, "K1ABC") && s.DXCallNorm != "K1ABC" {
				t.Fatalf("entity leak: %s", s.DXCallNorm)
			}
		}
	}
}
