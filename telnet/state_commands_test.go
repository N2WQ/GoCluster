package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

// This accepted vocabulary is a literal oracle rather than a copy of the new
// getter. It includes US mailing codes and Canadian provinces/territories.
const stateCodesFixture = "AA AB AE AK AL AP AR AS AZ BC CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA MA MB MD ME MI MN MO MP MS MT NB NC ND NE NH NJ NL NM NS NT NU NV NY OH OK ON OR PA PE PR QC RI SC SD SK TN TX UM UT VA VI VT WA WI WV WY YT"

func TestStateCommandsAtomicBothDialects(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, domain := range []string{"DXSTATE", "DESTATE"} {
			t.Run(string(dialect)+domain, func(t *testing.T) {
				s := presetTestServer(t)
				c := configurationTestClient(s, "W1ABC-1")
				c.dialect = dialect
				e := newFilterCommandEngine()
				pass, reject := "PASS ", "REJECT "
				if dialect == DialectCC {
					pass, reject = "SET/FILTER ", "UNSET/FILTER "
				}
				if response, handled := e.Handle(c, pass+domain+" ca,TX on ca"); !handled || strings.Contains(response, "Invalid") {
					t.Fatal(response)
				}
				before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
				path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
				beforeDisk, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, line := range []string{pass + domain + " NY,ZZ", reject + domain + " NY,California", pass + domain + " ALL,CA", pass + domain + " ZZ"} {
					response, _ := e.Handle(c, line)
					afterDisk, err := os.ReadFile(path)
					if !strings.Contains(response, "Invalid") || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || err != nil || !bytes.Equal(beforeDisk, afterDisk) {
						t.Fatalf("non-atomic %q: %s", line, response)
					}
				}
				candidate := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
				for _, state := range []string{"CA", "TX", "ON", "QC", "NY", ""} {
					candidate.DXMetadata.State, candidate.DEMetadata.State = state, state
					if got := c.filter.Matches(candidate); got != (state == "CA" || state == "TX" || state == "ON") {
						t.Fatalf("PASS %s state=%q got=%t", domain, state, got)
					}
				}
				e.Handle(c, pass+domain+" ALL")
				e.Handle(c, reject+domain+" CA,ON")
				for _, state := range []string{"CA", "ON", "QC", "TX", ""} {
					candidate.DXMetadata.State, candidate.DEMetadata.State = state, state
					if got := c.filter.Matches(candidate); got != (state != "CA" && state != "ON") {
						t.Fatalf("REJECT %s state=%q got=%t", domain, state, got)
					}
				}
				e.Handle(c, reject+domain+" ALL")
				candidate.DXMetadata.State, candidate.DEMetadata.State = "", ""
				if c.filter.Matches(candidate) {
					t.Fatal("REJECT ALL passed unknown")
				}
				e.Handle(c, "RESET FILTER")
				if !c.filter.Matches(candidate) {
					t.Fatal("RESET retained state restrictions")
				}
				e.Handle(c, pass+domain+" CA")
				e.Handle(c, "PASS NOFILTER")
				if !c.filter.Matches(candidate) {
					t.Fatal("NOFILTER retained state restrictions")
				}
			})
		}
	}
}

func TestStateReadbacksCompleteFiniteSelections(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	e := newFilterCommandEngine()
	want := strings.Fields(stateCodesFixture)
	if got := spot.StateCodes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("code vocabulary: %v", got)
	}
	for _, domain := range []string{"DXSTATE", "DESTATE"} {
		response, _ := e.Handle(c, "PASS "+domain+" "+stateCodesFixture)
		if strings.Contains(response, "Invalid") {
			t.Fatal(response)
		}
		response, err := s.renderHumanReadback(c, "FILTER", domain, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		assertHumanWire(t, response)
		for _, code := range want {
			if !strings.Contains(response, code+",") && !strings.Contains(response, code+"\r\n") {
				t.Fatalf("%s omitted %s: %s", domain, code, response)
			}
		}
	}
	response, err := s.renderHumanReadback(c, "FILTER", "", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertHumanWire(t, response)
	if strings.Contains(response, "73 states") || !strings.Contains(response, "DX states") || !strings.Contains(response, "YT") {
		t.Fatal("finite overview collapsed selections")
	}
}

func TestStateHistorySnapshotAndPresetIdentity(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.SetDXState("ON", true)
	c.filter.SetDEState("NY", true)
	snapshot := c.captureHistoryFilter()
	sp := spot.NewSpot("K1ABC", "W2XYZ", 14030, "CW")
	sp.DXMetadata.State, sp.DEMetadata.State = "ON", "NY"
	if !snapshot.matches(s, sp) {
		t.Fatal("captured state rules did not match")
	}
	response, _ := s.handlePresetCommand(c, "SAVE PRESET STATES")
	if !strings.Contains(response, "Saved preset") {
		t.Fatal(response)
	}
	newFilterCommandEngine().Handle(c, "REJECT DXSTATE ON")
	if !snapshot.matches(s, sp) || c.historyFilterDigest() == snapshot.digest {
		t.Fatal("history snapshot borrowed state or digest omitted state")
	}
	response, _ = s.handlePresetCommand(c, "LOAD PRESET STATES")
	if !strings.Contains(response, "Loaded preset") || !c.filter.Matches(sp) {
		t.Fatalf("preset state lost: %s", response)
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || !record.DXStates["ON"] || !record.DEStates["NY"] || record.Preset == nil || !record.Preset.Baseline.DXStates["ON"] {
		t.Fatalf("durable state/baseline lost: %v", err)
	}
}

func FuzzStateList(f *testing.F) {
	for _, seed := range []string{stateCodesFixture, "ca,TX ca", "", "ALL,CA", "CA,\x00TX", "CA,AB"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			t.Skip()
		}
		states, invalid := parseStateList(input)
		if len(states) > 73 {
			t.Fatal("unbounded accepted vocabulary")
		}
		seen := make(map[string]bool)
		for _, state := range states {
			if !strings.Contains(" "+stateCodesFixture+" ", " "+state+" ") || seen[state] {
				t.Fatalf("invalid accepted token %q", state)
			}
			seen[state] = true
		}
		for _, token := range invalid {
			if strings.Contains(" "+stateCodesFixture+" ", " "+token+" ") {
				t.Fatalf("valid token rejected %q", token)
			}
		}
	})
}
