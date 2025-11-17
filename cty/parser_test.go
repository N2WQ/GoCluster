package cty

import (
	"strings"
	"testing"
)

const samplePLIST = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
<key>K1ABC</key>
	<dict>
		<key>Country</key>
		<string>Alpha</string>
		<key>Prefix</key>
		<string>K1ABC</string>
		<key>ExactCallsign</key>
		<true/>
	</dict>
<key>K1</key>
	<dict>
		<key>Country</key>
		<string>Alpha</string>
		<key>Prefix</key>
		<string>K1</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
<key>XM3</key>
	<dict>
		<key>Country</key>
		<string>Zed</string>
		<key>Prefix</key>
		<string>XM3</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
</dict>
</plist>`

func loadSampleDatabase(t *testing.T) *CTYDatabase {
	t.Helper()
	db, err := LoadCTYDatabaseFromReader(strings.NewReader(samplePLIST))
	if err != nil {
		t.Fatalf("load sample database: %v", err)
	}
	return db
}

func TestLookupExactCallsign(t *testing.T) {
	db := loadSampleDatabase(t)
	info, ok := db.LookupCallsign("K1ABC")
	if !ok {
		t.Fatalf("expected K1ABC to resolve")
	}
	if info.Country != "Alpha" {
		t.Fatalf("expected Alpha, got %q", info.Country)
	}
}

func TestLookupLongestPrefix(t *testing.T) {
	db := loadSampleDatabase(t)
	info, ok := db.LookupCallsign("K1XYZ")
	if !ok {
		t.Fatalf("expected prefix match for K1XYZ")
	}
	if info.Prefix != "K1" {
		t.Fatalf("expected prefix K1, got %q", info.Prefix)
	}
}

func TestSuffixNormalization(t *testing.T) {
	db := loadSampleDatabase(t)
	info, ok := db.LookupCallsign("K1ABC/M")
	if !ok {
		t.Fatalf("expected normalized call to resolve")
	}
	if info.Prefix != "K1ABC" {
		t.Fatalf("expected prefix K1ABC, got %q", info.Prefix)
	}
}

func TestLongerPrefixFallback(t *testing.T) {
	db := loadSampleDatabase(t)
	info, ok := db.LookupCallsign("XM3A")
	if !ok {
		t.Fatalf("expected longest prefix match for XM3A")
	}
	if info.Country != "Zed" {
		t.Fatalf("expected Zed, got %q", info.Country)
	}
}
