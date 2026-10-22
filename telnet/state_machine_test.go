package telnet

import (
	"testing"

	"dxcluster/cty"
	"dxcluster/uls"
)

func TestStateExpansionPreservesLoginLicensePolicy(t *testing.T) {
	db := &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{
		"W1ABC": {Prefix: "K", ADIF: 291}, "W1ABC-1": {Prefix: "K", ADIF: 291},
		"KH6ABC": {Prefix: "KH6", ADIF: 110}, "KH6ABC-1": {Prefix: "KH6", ADIF: 110},
	}}
	checks := 0
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.CTYLookup = func() *cty.CTYDatabase { return db }
		opts.USLicenseCheck = func(string) bool { checks++; return false }
	})
	if !s.validateLoginCallsign("KH6ABC-1").valid || checks != 0 {
		t.Fatal("spot territory coverage widened login gate")
	}
	if s.validateLoginCallsign("W1ABC-1").valid || checks != 1 {
		t.Fatal("enabled mainland login gate changed")
	}
	previous := uls.LicenseChecksEnabled()
	uls.SetLicenseChecksEnabled(false)
	t.Cleanup(func() { uls.SetLicenseChecksEnabled(previous) })
	s.usLicenseCheck = uls.IsLicensedUS
	if !s.validateLoginCallsign("W1ABC-1").valid {
		t.Fatal("disabled enforcement rejected login")
	}
}
