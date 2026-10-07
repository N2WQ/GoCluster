package uls

import (
	"regexp"
	"testing"
)

func TestNormalizeForLicensePrefersStationIdentity(t *testing.T) {
	for _, tc := range []struct{ call, base string }{
		{"VE3/W1A", "W1A"}, {"W1A/VE3", "W1A"},
		{"KH6/W1A", "W1A"}, {"W1A/KH6", "W1A"},
		{"CY0/W1A", "W1A"}, {"W1A/CY0", "W1A"},
		{"W1234/VE3A", "VE3A"}, {"VE3A/W1234", "VE3A"},
		{"VE1234/W1A", "W1A"}, {"W1A/VE1234", "W1A"},
		{"VE3/W1A-1", "W1A"}, {"W1A-1/VE3", "W1A"},
		{"VE3/W1A-#", "W1A"}, {"W1A/VE3-#", "W1A"},
		{" ve3 / w1a ", "W1A"}, {"VE3/W1ABC", "W1ABC"},
		{"W1ABC/VE3", "W1ABC"}, {"K1/3D2A", "3D2A"},
		{"VE3/VC3R20", "VC3R20"}, {"VC3R20/VE3", "VC3R20"},
		{"W1A", "W1A"}, {"W1A-1", "W1A"},
		{"W1A/P", "W1A"}, {"W1A/7", "W1A"},
		{"VE3/W1", "VE3"}, {"W1A/K1B", "W1A"},
		{"W1A/K1ABC", "K1ABC"}, {"K1ABC/W1A", "K1ABC"},
	} {
		t.Run(tc.call, func(t *testing.T) {
			if got := NormalizeForLicense(tc.call); got != tc.base {
				t.Fatalf("base=%q want=%q", got, tc.base)
			}
		})
	}
}

// The oracle deliberately describes bare prefixes and station suffixes without
// calling the production identity classifier. Both orders must select the base
// even when a numeric location prefix is longer than the complete callsign.
func FuzzNormalizeForLicensePortableIdentity(f *testing.F) {
	for _, seed := range [][2]string{{"VE3", "W1A"}, {"KH6", "W1A"}, {"W1234", "VE3A"}, {"VE1234", "W1A"}, {"VE3", "VC3R20"}} {
		f.Add(seed[0], seed[1])
	}
	prefixPattern := regexp.MustCompile(`^[A-Z]{1,2}[0-9]{1,4}$`)
	basePattern := regexp.MustCompile(`^[A-Z]{1,2}[0-9][A-Z]{1,4}[0-9]{0,2}$`)
	f.Fuzz(func(t *testing.T, prefix, base string) {
		if len(prefix)+len(base)+1 > 15 || !prefixPattern.MatchString(prefix) || !basePattern.MatchString(base) {
			return
		}
		for _, call := range []string{prefix + "/" + base, base + "/" + prefix} {
			if got := NormalizeForLicense(call); got != base {
				t.Fatalf("%q selected %q want %q", call, got, base)
			}
		}
	})
}

func TestIsLicensedUSFailsOpenDuringRefresh(t *testing.T) {
	SetRefreshInProgress(true)
	defer SetRefreshInProgress(false)

	if ok := IsLicensedUS("W1AW"); !ok {
		t.Fatal("expected IsLicensedUS to fail open during refresh")
	}
}
