package util

import (
	"strings"
	"testing"
)

func TestV15SQLiteBoundedBooleanPragma(t *testing.T) {
	original := func(s string) (bool, bool) {
		if len(s) == 0 {
			return false, false
		}
		if s[0] == '0' {
			return false, true
		}
		if '1' <= s[0] && s[0] <= '9' {
			return true, true
		}
		switch strings.ToLower(s) {
		case "true", "yes", "on":
			return true, true
		case "false", "no", "off":
			return false, true
		}
		return false, false
	}
	large := strings.Repeat("\xff", 64000)
	for _, value := range []string{"", "true", "FALSE", "Yes", "oN", "oFf", "falſe", "İ", "Ⱥ", "\xff", large, "0" + large, "9" + large} {
		got, ok := ParseBool(value)
		want, wantOK := original(value)
		if got != want || ok != wantOK {
			t.Fatalf("boolean mismatch for input length %d: %t/%t want %t/%t", len(value), got, ok, want, wantOK)
		}
	}
	if count := testing.AllocsPerRun(20, func() { ParseBool(large) }); count != 0 {
		t.Fatalf("large unknown boolean allocated %g objects", count)
	}
}
