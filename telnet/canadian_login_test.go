package telnet

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/cty"
	"dxcluster/uls"
)

func TestCanadianLoginUsesBaseIdentityAndPreservesExceptions(t *testing.T) {
	var plist strings.Builder
	plist.WriteString("<plist><dict>")
	for _, row := range []struct {
		prefix string
		adif   int
	}{{"VE", 1}, {"VA", 1}, {"CY0", 211}, {"CY9", 252}, {"W", 291}, {"K", 291}, {"KH6", 110}, {"DL", 230}} {
		fmt.Fprintf(&plist, "<key>%s</key><dict><key>Country</key><string>Fixture</string><key>Prefix</key><string>%s</string><key>ADIF</key><integer>%d</integer></dict>", row.prefix, row.prefix, row.adif)
	}
	plist.WriteString("</dict></plist>")
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(plist.String()))
	if err != nil {
		t.Fatal(err)
	}
	uls.SetAllowlistPath("")
	t.Cleanup(func() { uls.SetAllowlistPath("") })
	for _, row := range []struct {
		call, base string
		canadian   bool
	}{
		{"VE3ABC", "VE3ABC", true}, {"W1/VE3ABC", "VE3ABC", true}, {"VE3ABC/W1", "VE3ABC", true},
		{"CY0ABC", "CY0ABC", true}, {"CY9ABC", "CY9ABC", true},
		{"VE3/W1ABC", "W1ABC", false}, {"W1ABC/VE3", "W1ABC", false},
		{"VE3/W1A", "W1A", false}, {"W1A/VE3", "W1A", false},
		{"W1234/VE3A", "VE3A", true}, {"VE3A/W1234", "VE3A", true},
	} {
		t.Run(row.call, func(t *testing.T) {
			usCalls, caCalls := []string{}, []string{}
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.CTYLookup = func() *cty.CTYDatabase { return db }
				opts.USLicenseCheck = func(call string) bool { usCalls = append(usCalls, call); return false }
				opts.CanadianLicenseCheck = func(call string) bool { caCalls = append(caCalls, call); return false }
			})
			got := s.validateLoginCallsign(row.call)
			wantReason, actual := loginValidationReasonUSUnlicensed, usCalls
			if row.canadian {
				wantReason, actual = loginValidationReasonCAUnlicensed, caCalls
			}
			if got.valid || got.reason != wantReason || len(actual) != 1 || actual[0] != row.base || len(usCalls)+len(caCalls) != 1 {
				t.Fatalf("result=%+v US=%v CA=%v", got, usCalls, caCalls)
			}
		})
	}
	checks := 0
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.CTYLookup = func() *cty.CTYDatabase { return db }
		opts.USLicenseCheck = func(string) bool { checks++; return false }
		opts.CanadianLicenseCheck = func(string) bool { checks++; return false }
	})
	for _, call := range []string{"VE3TEST", "VE3TEST-1", "KH6ABC", "DL1ABC"} {
		if result := s.validateLoginCallsign(call); !result.valid || checks != 0 {
			t.Fatalf("exception %s: %+v checks=%d", call, result, checks)
		}
	}
	path := filepath.Join(t.TempDir(), "allowlist.txt")
	if err := os.WriteFile(path, []byte("1: VE3ALLOW\n211: CY0ALLOW\n252: CY9ALLOW\nUS: VE3WRONG\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	uls.SetAllowlistPath(path)
	for _, call := range []string{"VE3ALLOW", "CY0ALLOW", "CY9ALLOW"} {
		if result := s.validateLoginCallsign(call); !result.valid || checks != 0 {
			t.Fatalf("allowlist %s: %+v checks=%d", call, result, checks)
		}
	}
	if result := s.validateLoginCallsign("VE3WRONG"); result.valid || checks != 1 {
		t.Fatal("US allowlist leaked into Canadian authority")
	}
	for _, row := range []struct{ call, qualified, wrong string }{
		{"VE3/W1A", "US:^W1A$", "1:^W1A$"},
		{"W1A/VE3", "US:^W1A$", "1:^W1A$"},
		{"W1234/VE3A", "1:^VE3A$", "US:^VE3A$"},
		{"VE3A/W1234", "1:^VE3A$", "US:^VE3A$"},
	} {
		t.Run("allowlist/"+row.call, func(t *testing.T) {
			for _, policy := range []struct {
				entry string
				valid bool
			}{{row.wrong, false}, {row.qualified, true}} {
				allowlist := filepath.Join(t.TempDir(), "allowlist.txt")
				if err := os.WriteFile(allowlist, []byte(policy.entry+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				uls.SetAllowlistPath(allowlist)
				before := checks
				got := s.validateLoginCallsign(row.call)
				wantChecks := before + 1
				if policy.valid {
					wantChecks = before
				}
				if got.valid != policy.valid || checks != wantChecks {
					t.Fatalf("policy=%q result=%+v checks=%d want=%d", policy.entry, got, checks, wantChecks)
				}
			}
		})
	}
	previous := uls.CanadianLicenseChecksEnabled()
	uls.SetCanadianLicenseChecksEnabled(false)
	t.Cleanup(func() { uls.SetCanadianLicenseChecksEnabled(previous); uls.SetCanadianLicenseDBPath("") })
	s.canadianLicenseCheck = uls.IsLicensedCanadian
	if result := s.validateLoginCallsign("VE3MISS"); !result.valid {
		t.Fatal("disabled enforcement rejected login")
	}
	uls.SetCanadianLicenseChecksEnabled(true)
	uls.SetCanadianLicenseDBPath(filepath.Join(t.TempDir(), "missing.db"))
	if result := s.validateLoginCallsign("VE3MISS"); !result.valid {
		t.Fatal("unavailable snapshot failed closed")
	}
}
