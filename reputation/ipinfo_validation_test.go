package reputation

import (
	"bytes"
	"encoding/binary"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestIPInfoCSVReadersSkipUnusableRows(t *testing.T) {
	root := t.TempDir()
	csv := filepath.Join(root, "source.csv")
	content := "start_ip,end_ip,asn,country_code\n" +
		"192.0.2.1,2001:db8::1,AS64501,US\n" +
		"2001:db8::2,192.0.2.2,AS64501,US\n" +
		"192.0.2.30,192.0.2.29,AS64501,US\n" +
		"2001:db8::30,2001:db8::29,AS64501,US\n" +
		"192.0.2.6,192.0.2.6,AS, \n" +
		"192.0.2.7,192.0.2.7,AS64500,\n" +
		"192.0.2.8,::ffff:192.0.2.8,,CA\n" +
		"::ffff:192.0.2.9,192.0.2.9,AS64502,\n" +
		"2001:db8::10,2001:db8::12,,US\n"
	if err := os.WriteFile(csv, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	index, err := LoadIPInfoSnapshot(csv)
	if err != nil {
		t.Fatal(err)
	}
	path, err := buildIPInfoPebble(t.Context(), csv, root, false)
	if err != nil {
		t.Fatal(err)
	}
	store, err := openIPInfoStore(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	diskLookup := func(addr netip.Addr) (LookupResult, bool) {
		if addr.Is4() {
			return store.lookupV4(addr)
		}
		return store.lookupV6(addr)
	}
	for _, tc := range []struct {
		address, asn, country string
	}{
		{address: "192.0.2.1"}, {address: "2001:db8::2"},
		{address: "192.0.2.30"}, {address: "2001:db8::30"},
		{address: "192.0.2.6"},
		{address: "192.0.2.7", asn: "64500"},
		{address: "192.0.2.8", country: "CA"},
		{address: "::ffff:192.0.2.9", asn: "64502"},
		{address: "2001:db8::12", country: "US"},
		{address: "2001:db8::13"},
	} {
		addr := netip.MustParseAddr(tc.address)
		want := tc.asn != "" || tc.country != ""
		for name, lookup := range map[string]func(netip.Addr) (LookupResult, bool){
			"CSV index": index.lookup,
			"Pebble":    diskLookup,
		} {
			got, found := lookup(addr)
			if found != want || got.ASN != tc.asn || got.CountryCode != tc.country {
				t.Errorf("%s %s: found=%t result=%+v, want ASN=%q country=%q", name, addr, found, got, tc.asn, tc.country)
			}
		}
	}
}

func TestIPInfoLegacyRowsWithoutUsableDataRemainMisses(t *testing.T) {
	cfg := ipinfoTestConfig(t, "")
	cfg.IPInfoPebbleLoadIPv4 = true
	g := openTestGate(t, cfg)
	path := filepath.Join(cfg.IPInfoPebblePath, "db")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})
	now := time.Now().UTC()
	set := func(key, value []byte) {
		t.Helper()
		if err := db.Set(key, value, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	set([]byte(ipinfoPebbleMetaVersion), encodeIPInfoMetaInt(ipinfoPebbleVersion))
	set([]byte(ipinfoPebbleMetaBuiltAt), encodeIPInfoMetaTime(now))

	unusable := []string{"192.0.2.2", "192.0.2.3", "192.0.2.4", "2001:db8::2", "2001:db8::3",
		"192.0.3.1", "192.0.4.2", "192.0.5.1", "2001:db9::2", "2001:dba::2", "2001:dbb::2"}
	usable := []string{"192.0.2.1", "192.0.2.5", "2001:db8::1", "2001:db8::4"}
	writeRow := func(address string, asn string, truncated bool) {
		t.Helper()
		addr := netip.MustParseAddr(address)
		value := encodeIPInfoValue(addr.AsSlice(), asn, "")
		if truncated {
			value = value[:len(value)-1]
		}
		if addr.Is4() {
			set(makeV4Key(ip4ToUint32(addr.As4())), value)
		} else {
			set(makeV6Key(addr.As16()), value)
		}
	}
	writeRow(unusable[0], "", false)
	writeRow(unusable[1], "64500", true)
	set(makeV4Key(ip4ToUint32(netip.MustParseAddr(unusable[2]).As4())),
		encodeIPInfoValue(netip.MustParseAddr(unusable[1]).AsSlice(), "64500", ""))
	writeRow(unusable[3], "", false)
	writeRow(unusable[4], "64500", true)
	// Raw version-one bytes make this test independent of the writer.
	set([]byte{'4', 192, 0, 3}, []byte{192, 0, 3, 1, 1, '1', 0})
	set([]byte{'4', 192, 0, 4, 1, 0}, []byte{192, 0, 4, 2, 1, '1', 0})
	set([]byte{'4', 192, 0, 5, 1}, []byte{192, 0, 5, 1, 1, '1', 0, 0xff})
	for i, address := range unusable[8:] {
		addr := netip.MustParseAddr(address).As16()
		key := append([]byte{'6'}, addr[:]...)
		key[len(key)-1] = 1
		value := append(bytes.Clone(addr[:]), 1, '1', 0)
		switch i {
		case 0:
			key = key[:len(key)-1]
		case 1:
			key = append(key, 0)
		case 2:
			value = append(value, 0xff)
		}
		set(key, value)
	}
	for _, address := range usable {
		writeRow(address, "64500", false)
	}
	closeErr := db.Close()
	db = nil
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if err := g.LoadStore(); err != nil {
		t.Fatal(err)
	}
	if got := len(g.ipinfoView.Load().index.v4); got != 2 {
		t.Fatalf("IPv4 preload kept unusable legacy rows: %d indexed entries, want 2", got)
	}

	var fallbackCalls int
	g.lookupCymruFunc = func(_ netip.Addr, when time.Time) (LookupResult, bool, bool) {
		fallbackCalls++
		return LookupResult{Source: cymruSource, ASN: "64512", FetchedAt: when}, true, false
	}
	for _, address := range unusable {
		if _, found := g.LookupIPForAdmission(address, now); found {
			t.Errorf("pre-login accepted unusable local data for %s", address)
		}
	}
	if fallbackCalls != 0 {
		t.Fatal("pre-login invoked network fallback")
	}
	for _, address := range unusable {
		requireIPInfoASN(t, g, netip.MustParseAddr(address), now, "64512")
	}
	for _, address := range usable {
		requireIPInfoASN(t, g, netip.MustParseAddr(address), now, "64500")
	}
	if fallbackCalls != len(unusable) {
		t.Fatalf("fallback calls=%d, want %d", fallbackCalls, len(unusable))
	}
}

func TestDecodeIPInfoValueRefusesInvalidLengths(t *testing.T) {
	for _, width := range []int{4, 16} {
		end := make([]byte, width)
		for _, tc := range []struct {
			name string
			tail []byte
		}{
			{name: "missing ASN length"},
			{name: "truncated varint", tail: []byte{0x80}},
			{name: "ASN exceeds remainder", tail: []byte{2, '1'}},
			{name: "country exceeds remainder", tail: []byte{1, '1', 2, 'A'}},
			{name: "ASN exceeds native int", tail: binary.AppendUvarint(nil, math.MaxUint64)},
			{name: "country exceeds native int", tail: binary.AppendUvarint([]byte{1, '1'}, math.MaxUint64)},
			{name: "overlong ASN length", tail: []byte{0x81, 0x00, '1', 0}},
			{name: "overlong country length", tail: []byte{0, 0x81, 0x00, 'A'}},
			{name: "trailing bytes", tail: []byte{1, '1', 0, 0xff}},
			{name: "no enrichment", tail: []byte{0, 0}},
		} {
			if _, _, _, ok := decodeIPInfoValue(append(bytes.Clone(end), tc.tail...), width); ok {
				t.Errorf("%d-byte address: accepted %s", width, tc.name)
			}
		}
	}
}

func FuzzDecodeIPInfoRecord(f *testing.F) {
	f.Add([]byte{}, []byte{}, byte('4'))
	f.Add([]byte{'4', 192, 0, 3, 1}, []byte{192, 0, 3, 1, 1, '1', 0}, byte('4'))
	f.Add(makeV6Key([16]byte{}), encodeIPInfoValue(make([]byte, 16), "", "US"), byte('6'))
	f.Fuzz(func(t *testing.T, key, data []byte, family byte) {
		start, end, asn, country, ok := decodeIPInfoRecord(key, data, family)
		if ok {
			wantWidth := 4
			if family == '6' {
				wantWidth = 16
			}
			if (family != '4' && family != '6') || len(key) != wantWidth+1 || key[0] != family ||
				!bytes.Equal(key[1:], start) || len(end) != wantWidth || bytes.Compare(start, end) > 0 ||
				(asn == "" && country == "") || !bytes.Equal(data, encodeIPInfoValue(end, asn, country)) {
				t.Fatal("accepted a noncanonical range record")
			}
		}
		for _, valid := range []struct {
			family byte
			width  int
		}{{'4', 4}, {'6', 16}} {
			bound := make([]byte, valid.width)
			copy(bound, data)
			canonicalKey := append([]byte{valid.family}, bound...)
			value := encodeIPInfoValue(bound, "64500", "CA")
			start, end, asn, country, ok := decodeIPInfoRecord(canonicalKey, value, valid.family)
			if !ok || !bytes.Equal(start, bound) || !bytes.Equal(end, bound) || asn != "64500" || country != "CA" {
				t.Fatal("valid record failed to round trip")
			}
		}
	})
}
