package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/uls"
)

const ingestSamplePLIST = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
<key>DL</key>
	<dict>
		<key>Country</key>
		<string>Germany</string>
		<key>Prefix</key>
		<string>DL</string>
		<key>ADIF</key>
		<integer>230</integer>
		<key>CQZone</key>
		<integer>14</integer>
		<key>ITUZone</key>
		<integer>28</integer>
		<key>Continent</key>
		<string>EU</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
<key>K1</key>
	<dict>
		<key>Country</key>
		<string>United States</string>
		<key>Prefix</key>
		<string>K1</string>
		<key>ADIF</key>
		<integer>291</integer>
		<key>CQZone</key>
		<integer>5</integer>
		<key>ITUZone</key>
		<integer>8</integer>
		<key>Continent</key>
		<string>NA</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
<key>N2</key>
	<dict>
		<key>Country</key>
		<string>United States</string>
		<key>Prefix</key>
		<string>N2</string>
		<key>ADIF</key>
		<integer>291</integer>
		<key>CQZone</key>
		<integer>5</integer>
		<key>ITUZone</key>
		<integer>8</integer>
		<key>Continent</key>
		<string>NA</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
<key>LZ</key>
	<dict>
		<key>Country</key>
		<string>Bulgaria</string>
		<key>Prefix</key>
		<string>LZ</string>
		<key>ADIF</key>
		<integer>212</integer>
		<key>CQZone</key>
		<integer>20</integer>
		<key>ITUZone</key>
		<integer>28</integer>
		<key>Continent</key>
		<string>EU</string>
		<key>ExactCallsign</key>
		<false/>
	</dict>
</dict>
</plist>`

func loadIngestCTY(t testing.TB) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(ingestSamplePLIST))
	if err != nil {
		t.Fatalf("load ingest CTY: %v", err)
	}
	return db
}

func TestIngestValidatorPreservesGrids(t *testing.T) {
	db := loadIngestCTY(t)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	v.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true, Found: true} }

	s := spot.NewSpotNormalized("DL1ABC", "K1ABC", 14074.0, "FT8")
	s.SourceType = spot.SourcePSKReporter
	s.DXMetadata.Grid = "JN58"
	s.DEMetadata.Grid = "FN42"

	if !v.validateSpot(s) {
		t.Fatalf("expected spot to pass validation")
	}
	if s.DXMetadata.Grid != "JN58" {
		t.Fatalf("expected DX grid to be preserved, got %q", s.DXMetadata.Grid)
	}
	if s.DEMetadata.Grid != "FN42" {
		t.Fatalf("expected DE grid to be preserved, got %q", s.DEMetadata.Grid)
	}
	if s.DXMetadata.ADIF != 230 {
		t.Fatalf("expected DX ADIF 230, got %d", s.DXMetadata.ADIF)
	}
	if s.DEMetadata.ADIF != 291 {
		t.Fatalf("expected DE ADIF 291, got %d", s.DEMetadata.ADIF)
	}
}

func TestIngestValidatorReportsCTYUnknownDrop(t *testing.T) {
	db := loadIngestCTY(t)
	var got struct {
		source string
		role   string
		reason string
		call   string
		de     string
		dx     string
		mode   string
		detail string
	}
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	v.SetBadCallReporter(func(source, role, reason, call, deCall, dxCall, mode, detail string) {
		got.source = source
		got.role = role
		got.reason = reason
		got.call = call
		got.de = deCall
		got.dx = dxCall
		got.mode = mode
		got.detail = detail
	})
	v.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true, Found: true} }

	s := spot.NewSpotNormalized("ZZ9ABC", "K1ABC", 14074.0, "FT8")
	s.SourceNode = "RBN"
	if v.validateSpot(s) {
		t.Fatalf("expected unknown CTY DX to be dropped")
	}
	if got.source != "RBN" || got.role != "DX" || got.reason != "cty_unknown" || got.call != "ZZ9ABC" || got.de != "K1ABC" || got.dx != "ZZ9ABC" || got.mode != "FT8" || got.detail != "cty_validation" {
		t.Fatalf("unexpected bad-call report: %+v", got)
	}
}

func TestIngestValidatorReportsInvalidCallsignDrop(t *testing.T) {
	for _, call := range []string{"ABC1D", "SET", "SET/NOFT8"} {
		db := loadIngestCTY(t)
		var got struct {
			role   string
			reason string
			call   string
			detail string
		}
		v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
		v.SetBadCallReporter(func(source, role, reason, call, deCall, dxCall, mode, detail string) {
			got.role = role
			got.reason = reason
			got.call = call
			got.detail = detail
		})
		v.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true, Found: true} }

		s := spot.NewSpotNormalized(call, "K1ABC", 14074.0, "FT8")
		s.SourceNode = "RBN"
		if v.validateSpot(s) {
			t.Fatalf("expected invalid DX %q to be dropped", call)
		}
		if got.role != "DX" || got.reason != "invalid_callsign" || got.call != call || got.detail != "cty_prefilter" {
			t.Fatalf("unexpected bad-call report for %q: %+v", call, got)
		}
	}
}

func TestIngestValidatorDropsUnlicensedUSSpotter(t *testing.T) {
	db := loadIngestCTY(t)
	var gotSource, gotRole, gotCall, gotMode string
	var gotFreq float64
	reported := false

	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	v.unlicensedReporter = func(source, role, call, deCall, dxCall, mode string, freq float64) {
		reported = true
		gotSource = source
		gotRole = role
		gotCall = call
		gotMode = mode
		gotFreq = freq
	}
	v.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true} }
	v.licenseChecksEnabled = func() bool { return true }

	s := spot.NewSpotNormalized("DL1ABC", "K1ABC", 14074.0, "FT8")
	s.SourceNode = "PSKREPORTER"

	if v.validateSpot(s) {
		t.Fatalf("expected unlicensed spotter to be dropped")
	}
	if !reported {
		t.Fatalf("expected unlicensed reporter to be invoked")
	}
	if gotSource != "PSKREPORTER" {
		t.Fatalf("expected source PSKREPORTER, got %q", gotSource)
	}
	if gotRole != "DE" {
		t.Fatalf("expected role DE, got %q", gotRole)
	}
	if gotCall != "K1ABC" {
		t.Fatalf("expected call K1ABC, got %q", gotCall)
	}
	if gotMode != "FT8" {
		t.Fatalf("expected mode FT8, got %q", gotMode)
	}
	if gotFreq != 14074.0 {
		t.Fatalf("expected frequency 14074.0, got %.1f", gotFreq)
	}
}

func TestIngestValidatorSkipsULSForTestSpotter(t *testing.T) {
	db := loadIngestCTY(t)
	reported := false

	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	v.unlicensedReporter = func(source, role, call, deCall, dxCall, mode string, freq float64) {
		reported = true
	}
	v.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true} }
	v.licenseChecksEnabled = func() bool { return true }

	s := spot.NewSpotNormalized("DL1ABC", "K1TEST", 14074.0, "FT8")
	s.SourceNode = "TELNET"
	s.IsTestSpotter = true

	if !v.validateSpot(s) {
		t.Fatalf("expected test spotter to bypass ULS validation")
	}
	if reported {
		t.Fatalf("did not expect unlicensed reporter for test spotter")
	}
}

func TestIngestValidatorUsesBaseCallForLicenseJurisdiction(t *testing.T) {
	db := loadIngestCTY(t)
	checked := make([]string, 0, 2)

	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	v.lookupUS = func(call string) uls.LookupResult {
		checked = append(checked, call)
		return uls.LookupResult{Available: true, Found: true}
	}

	spotUSBase := spot.NewSpotNormalized("DL1ABC", "HR9/N2WQ", 14074.0, "FT8")
	if !v.validateSpot(spotUSBase) {
		t.Fatalf("expected HR9/N2WQ to pass validation")
	}
	if len(checked) != 1 || checked[0] != "N2WQ" {
		t.Fatalf("expected license check on base call N2WQ, got %v", checked)
	}

	checked = checked[:0]
	spotForeignBase := spot.NewSpotNormalized("DL1ABC", "K1/LZ5VV", 14074.0, "FT8")
	if !v.validateSpot(spotForeignBase) {
		t.Fatalf("expected K1/LZ5VV to pass validation")
	}
	if len(checked) != 0 {
		t.Fatalf("expected no license check for foreign base call, got %v", checked)
	}
}

func TestIngestValidatorDropsInvalidCallsignShape(t *testing.T) {
	db := loadIngestCTY(t)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	v.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true, Found: true} }

	for _, call := range []string{"ABC1", "SET/FT8", "NOFT8"} {
		s := spot.NewSpotNormalized(call, "K1ABC", 14074.0, "FT8")
		if v.validateSpot(s) {
			t.Fatalf("expected invalid callsign shape %q to be dropped", call)
		}
	}
}

func TestIngestValidatorAllowlistOverridesInvalidLeadingLetters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "allowlist.txt")
	if err := os.WriteFile(path, []byte("(?i)^ABC1$\n"), 0o644); err != nil {
		t.Fatalf("write allowlist: %v", err)
	}
	uls.SetAllowlistPath(path)
	defer uls.SetAllowlistPath("")

	db := loadIngestCTY(t)
	v := newIngestValidator(func() *cty.CTYDatabase { return db }, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
	v.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true, Found: true} }

	s := spot.NewSpotNormalized("ABC1", "K1ABC", 14074.0, "FT8")
	if !v.validateSpot(s) {
		t.Fatalf("expected allowlisted invalid call to pass validation")
	}
}
