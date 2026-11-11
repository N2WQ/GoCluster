package cty

import (
	"strings"
	"testing"
)

func TestShippedExactOnlyPrefixContract(t *testing.T) {
	db := loadShippedCTY(t)
	for _, tc := range []struct {
		call string
		adif int
	}{
		{"K1D", 202}, {"K1DW", 291}, {"K1DG", 291}, {"K7DX", 291},
		{"G4XYZ", 223}, {"G0XYZ", 223}, {"4U1AX", 248},
		{"K1", 291}, {"C7A", 206}, {"C7AX", 0},
		{"N2WQ/VE3", 1}, {"VE3/N2WQ", 1}, {"K1D/B", 202},
	} {
		t.Run(tc.call, func(t *testing.T) {
			info, ok := db.LookupCallsignPortable(tc.call)
			if tc.adif == 0 {
				if ok || info != nil {
					t.Fatalf("%s must miss, got %+v", tc.call, info)
				}
				return
			}
			if !ok || info == nil || info.ADIF != tc.adif {
				t.Fatalf("%s = %+v/%v, want ADIF %d", tc.call, info, ok, tc.adif)
			}
		})
	}
	info, ok := db.LookupCallsign("K1DG")
	if !ok || info.CQZone != 5 || info.ITUZone != 8 {
		t.Fatalf("K1DG must use the K1 ordinary entry: %+v", info)
	}
	// Exact keys remain available to full-key lookup and canonical inventory.
	if len(db.Keys) != len(db.Data) || !db.Data["C7A"].ExactCallsign {
		t.Fatal("exact-key inventory was removed or changed")
	}
}

const eligibilityPLIST = `<plist version="1.0"><dict>
<key>X1</key><dict><key>Country</key><string>parent</string></dict>
<key>X1A</key><dict><key>Country</key><string>exact</string><key>ExactCallsign</key><true/></dict>
<key>X1AB</key><dict><key>Country</key><string>child</string></dict>
<key>X1ABC</key><dict><key>Country</key><string>long-exact</string><key>ExactCallsign</key><true/></dict>
<key>Q7A</key><dict><key>Country</key><string>no-parent</string><key>ExactCallsign</key><true/></dict>
<key>Z9/</key><dict><key>Country</key><string>slash-parent</string></dict>
<key>Z9/P</key><dict><key>Country</key><string>slash-exact</string><key>ExactCallsign</key><true/></dict>
</dict></plist>`

func TestPrefixEligibilityKeepsBestOrdinaryAncestor(t *testing.T) {
	db, err := LoadCTYDatabaseFromReader(strings.NewReader(eligibilityPLIST))
	if err != nil {
		t.Fatal(err)
	}
	for call, country := range map[string]string{
		"X1A": "exact", "X1AC": "parent", "X1AB": "child",
		"X1ABC": "long-exact", "X1ABCZ": "child", "Q7AX": "",
		"Q7A": "no-parent", "Z9/P": "slash-exact", "Z9/PX": "slash-parent",
	} {
		info, ok := db.LookupCallsign(call)
		if country == "" {
			if ok || info != nil {
				t.Fatalf("%s must miss, got %+v", call, info)
			}
		} else if !ok || info.Country != country {
			t.Fatalf("%s = %+v/%v, want %s", call, info, ok, country)
		}
	}
}

// The oracle scans eligible dictionary keys. It does not use the production
// trie or assume the longest terminal is eligible, the defect under test.
func FuzzCTYPrefixEligibility(f *testing.F) {
	db, err := LoadCTYDatabaseFromReader(strings.NewReader(eligibilityPLIST))
	if err != nil {
		f.Fatal(err)
	}
	for _, call := range []string{"", "X1A", "X1AC", "X1ABCZ", "Q7A", "Q7AX", "Z9/PX", "x1a", "X1A/B"} {
		f.Add(call)
	}
	f.Fuzz(func(t *testing.T, call string) {
		want, found := db.Data[call]
		if !found {
			longest := 0
			for key, info := range db.Data {
				if !info.ExactCallsign && len(key) > longest && strings.HasPrefix(call, key) {
					want, found, longest = info, true, len(key)
				}
			}
		}
		got, ok := db.LookupCallsign(call)
		if ok != found || (found && (got == nil || *got != want)) || (!found && got != nil) {
			t.Fatalf("%q = %+v/%v, want %+v/%v", call, got, ok, want, found)
		}
	})
}
