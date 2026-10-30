package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func testNativeCCCanonicalCountryOperands(t *testing.T, field, operator string) {
	t.Helper()
	c, engine, _ := nativeCCCommandFixture(t)
	calls := 0
	c.server.ctyLookup = func() *cty.CTYDatabase { calls++; return canonicalTestCTY() }
	family, action := "DXCTY", "PASS"
	if field == "DEDXCC" {
		family = "DOC"
	}
	if operator == "<>" {
		action = "REJECT"
	}
	outcome := engine.stageNativeCCFilterCommand(c, "SET/FILTER "+family+"/"+action+" k,291,VE,1,IT9,I,IG9,3D2/R,3Y/P")
	if !outcome.handled || !outcome.publish || strings.Contains(outcome.response, "No changes applied.") {
		t.Fatalf("native canonical countries were refused: %+v", outcome)
	}
	row := nativeCCConfiguration(t, c).Geography[family].Global
	if calls != 1 || !slices.Equal(row.Countries, []int{1, 199, 248, 291, 460}) {
		t.Fatalf("native countries lost canonical numeric/deduplicated state: %v calls=%d", row.Countries, calls)
	}
	for _, adif := range []int{1, 199, 248, 291, 460, 99999, 0} {
		sp := &spot.Spot{Mode: "CW", Frequency: 14025, IsHuman: true,
			DXMetadata: spot.CallMetadata{ADIF: 291}, DEMetadata: spot.CallMetadata{ADIF: 291}}
		if family == "DOC" {
			sp.DEMetadata.ADIF = adif
		} else {
			sp.DXMetadata.ADIF = adif
		}
		want := adif != 99999 && adif != 0
		if action == "REJECT" {
			want = !want // Native REJECT allows missing metadata.
		}
		if actual := c.filter.Matches(sp); actual != want {
			t.Fatalf("native %s %s ADIF %d: got %t want %t", family, action, adif, actual, want)
		}
	}
}

func nativeCCPersistenceReferences(t *testing.T, c *Client) {
	t.Helper()
	configuration, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	goPreset, err := filter.V7PresetFromConfiguration(configuration, "go")
	if err != nil {
		t.Fatal(err)
	}
	ccPreset, err := filter.V7PresetFromConfiguration(configuration, "cc")
	if err != nil {
		t.Fatal(err)
	}
	c.v7PresetReferences = filter.V7PresetReferences{
		GO: &filter.V7PresetReference{Name: "GO-BASE", Baseline: goPreset},
		CC: &filter.V7PresetReference{Name: "CC-BASE", Baseline: ccPreset},
	}
	if err := c.saveFilter(); err != nil {
		t.Fatal(err)
	}
}

func nativeCCPersistenceFault(t *testing.T, s *Server, c *Client, fault, path string) {
	t.Helper()
	if fault == "save" {
		return
	}
	body := "[broken"
	if fault != "malformed" {
		body = "configuration_version: 99\nnoise_class: URBAN\n"
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if fault == "protected" {
		restored, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute))
		if err != nil || !c.recordProtected || !strings.Contains(restored.warning, "temporary defaults") {
			t.Fatalf("protected native login: %+v, %v", restored, err)
		}
	}
}

func testNativeCCPersistenceWarning(t *testing.T, fault, operation string) {
	t.Helper()
	s, c := nativeCCTestCommittedSession(t)
	nativeCCPersistenceReferences(t, c)
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	nativeCCPersistenceFault(t, s, c, fault, path)
	configuration, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	configuration.Settings.Dialect = "cc"
	configuration.Filters.CC.Modes, configuration.Filters.CC.Sources = []string{"CW"}, []string{"SKIMMER"}
	configuration.Filters.CC.Geography["DOC"] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCReject, Countries: []int{291}}}
	configuration.Filters.CC.DXBM = filter.CCDXBM{Active: true, BaseAllow: false, Slots: map[string]bool{"40-CW": true}}
	canonical, _, runtime, err := filter.PrepareV7Configuration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	c.filterBanks, c.configuredSettings, c.dialect = &canonical.Filters, canonical.Settings, DialectCC
	*c.filter = runtime
	c.refreshConfigurationRevision()
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	references, pointer := c.v7PresetReferences, c.filter
	oldBytes := presetDiskBytes(t, path)
	attempts := 0
	if fault == "save" {
		s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return os.ErrPermission }
	}
	line := map[string]string{"mode": "SET/FT8", "source": "SET/NOSKIMMER", "country": "SET/FILTER DOC/PASS 1", "category-off": "SET/FILTER DOC/OFF", "reset": "SET/NOFILTER", "dialect": "DIALECT GO"}[operation]
	response, handled := s.filterEngine.Handle(c, line)
	if operation == "dialect" {
		response, handled = s.handleDialectCommand(c, line)
	}
	if !handled {
		t.Fatalf("native fault command was not handled: %s", line)
	}
	assertPersistenceWarning(t, response)
	after, err := c.captureV7Configuration()
	if err != nil || before.Equal(after) || c.filter != pointer || c.v7PresetReferences != references || !(filter.V6Configuration{Filters: before.Filters.GO}).Equal(filter.V6Configuration{Filters: after.Filters.GO}) {
		t.Fatal("native failed save lost live change, other bank, reference or pointer ownership")
	}
	valid := false
	switch operation {
	case "mode":
		valid = slices.Equal(after.Filters.CC.Modes, []string{"CW", "FT8"})
	case "source":
		valid = len(after.Filters.CC.Sources) == 0
	case "country":
		row := after.Filters.CC.Geography["DOC"].Global
		valid = row.Action == filter.CCPass && slices.Equal(row.Countries, []int{1})
	case "category-off":
		valid = after.Filters.CC.Geography["DOC"].Global.Action == filter.CCOff
	case "reset":
		defaults, err := filter.CurrentDefaultCCConfiguration()
		valid = err == nil && slices.Equal(after.Filters.CC.Modes, defaults.Modes) && slices.Equal(after.Filters.CC.Sources, defaults.Sources) && !after.Filters.CC.DXBM.Active
	case "dialect":
		valid = c.dialect == DialectGo && after.Settings.Dialect == "go" && after.Filters.CC.Equal(before.Filters.CC)
	}
	if !valid || !bytes.Equal(oldBytes, presetDiskBytes(t, path)) || fault == "save" && attempts != 1 || !c.configurationUnsaved {
		t.Fatalf("native live/durable result: valid=%t attempts=%d unsaved=%t", valid, attempts, c.configurationUnsaved)
	}
	if fault == "protected" {
		c.interrupt()
		s.unregisterClient(c)
		if !bytes.Equal(oldBytes, presetDiskBytes(t, path)) {
			t.Fatal("native protected teardown replaced original bytes")
		}
	}
}

func testNativeCCPersistenceSaveAttempts(t *testing.T) {
	t.Helper()
	s, c := nativeCCTestCommittedSession(t)
	nativeCCPersistenceReferences(t, c)
	references := c.v7PresetReferences
	attempts := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return os.ErrPermission }
	for _, line := range []string{"DIALECT CC", "RESET FILTER RULE 9", "SET/FT8 EXTRA", "SET/FILTER DOC/PASS 0", "PASS BAND 20"} {
		response, handled := s.handleDialectCommand(c, line)
		if !handled {
			response, handled = s.filterEngine.Handle(c, line)
		}
		if !handled || attempts != 0 || strings.Contains(response, "warning: failed to persist") {
			t.Fatalf("native no-op/refusal attempted a save: %s => %q (%d)", line, response, attempts)
		}
	}
	response, _ := s.filterEngine.Handle(c, "SET/CW")
	assertPersistenceWarning(t, response)
	if attempts != 1 || c.v7PresetReferences != references {
		t.Fatal("repeated native selection lost save intent or changed a bank reference")
	}
}
