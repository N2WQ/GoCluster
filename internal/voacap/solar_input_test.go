package voacap

import (
	"encoding/json"
	"strings"
	"testing"
	"unsafe"
)

func TestSolarInputSourceTaggedLiteralWire(t *testing.T) {
	// This is the literal generation-2 legacy wire shape, independent of adapters.
	flux := `{"recent_observed_unix":1780948800,"mean_observed_unix":1780948800,"input_updated_unix":1780952400,"ewma_half_life_seconds":172800,"latest_noon_flux_sfu":140,"recent_ewma_flux_sfu":130,"published_mean_flux_sfu":110,"blended_flux_sfu":120,"recent_weight":0.5,"source_id":1,"units_id":1,"conversion_id":1,"flags":7}`
	count := `{"recent_observed_unix":1780948800,"input_updated_unix":1780952400,"ewma_half_life_seconds":172800,"latest_raw_count_index":102,"smoothed_count_index":101,"source_id":2,"units_id":2,"conversion_id":2,"flags":1}`
	unknown := `{"recent_observed_unix":0,"mean_observed_unix":0,"input_updated_unix":0,"ewma_half_life_seconds":0,"latest_noon_flux_sfu":0,"recent_ewma_flux_sfu":0,"published_mean_flux_sfu":0,"blended_flux_sfu":0,"recent_weight":0,"source_id":0,"units_id":0,"conversion_id":0,"flags":0}`
	for _, body := range []string{flux, count, unknown} {
		var p SolarInputProvenance
		if err := json.Unmarshal([]byte(body), &p); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var round SolarInputProvenance
		if err = json.Unmarshal(encoded, &round); err != nil || round != p {
			t.Fatalf("round trip %s %+v %v", encoded, round, err)
		}
		if p.SourceID == 2 && strings.Contains(string(encoded), "sfu") {
			t.Fatalf("counts labeled SFU: %s", encoded)
		}
		if p.SourceID == 1 && !strings.Contains(string(encoded), `"blended_flux_sfu":120`) {
			t.Fatalf("legacy flux relabeled: %s", encoded)
		}
	}
	for _, bad := range []string{strings.Replace(count, `"latest_raw_count_index":102,`, "", 1), strings.Replace(count, `"smoothed_count_index":101,`, "", 1), strings.Replace(count, `"source_id":2`, `"source_id":99`, 1),
		strings.Replace(count, `"input_updated_unix":1780952400`, `"input_updated_unix":-1`, 1),
		strings.Replace(count, `"recent_observed_unix":1780948800`, `"recent_observed_unix":1780952401`, 1),
		strings.Replace(count, `"latest_raw_count_index":102`, `"latest_raw_count_index":102.5`, 1),
		strings.Replace(count, `"smoothed_count_index":101`, `"smoothed_count_index":9007199254740992`, 1), strings.Replace(count, `"source_id":2`, `"source_id":2,"source_id":2`, 1), strings.Replace(count, `"latest_raw_count_index":102`, `"latest_raw_count_index":null`, 1), strings.Replace(count, `"latest_raw_count_index":102`, `"latest_raw_count_index":102,"blended_flux_sfu":102`, 1), strings.Replace(unknown, `"latest_noon_flux_sfu":0`, `"latest_noon_flux_sfu":1`, 1)} {
		var p SolarInputProvenance
		if err := json.Unmarshal([]byte(bad), &p); err == nil {
			t.Fatalf("mixed/unsupported provenance accepted %s", bad)
		}
	}
}
func TestSolarInputPhysicalEnvelope(t *testing.T) {
	if got := unsafe.Sizeof(SolarInputProvenance{}); got != 80 {
		t.Fatalf("provenance bytes=%d", got)
	}
	if got := unsafe.Sizeof(SolarInputSnapshot{}); got != 88 {
		t.Fatalf("snapshot bytes=%d", got)
	}
	p := SolarInputProvenance{RecentObservedUnix: 1780948800, InputUpdatedUnix: 1780952400, EWMAHalfLifeSeconds: 172800, SourceID: 2, UnitsID: 2, ConversionID: 2, Flags: 1, LatestValue: 102, SmoothedValue: 101}
	if !p.Valid() {
		t.Fatal("fixture invalid")
	}
	if got := testing.AllocsPerRun(1000, func() {
		if !p.Valid() {
			panic("invalid")
		}
	}); got != 0 {
		t.Fatalf("provenance validation allocations=%g", got)
	}
}

func TestSolarInputCountSnapshotRejectsContradictorySSN(t *testing.T) {
	p := SolarInputProvenance{RecentObservedUnix: 1780948800, InputUpdatedUnix: 1780952400, EWMAHalfLifeSeconds: 172800, LatestValue: 102, SmoothedValue: 101, SourceID: 2, UnitsID: 2, ConversionID: 2, Flags: 1}
	if !(SolarInputSnapshot{SSN: 101, Provenance: p}).Valid() {
		t.Fatal("literal count snapshot rejected")
	}
	if (SolarInputSnapshot{SSN: 102, Provenance: p}).Valid() {
		t.Fatal("contradictory rounded SSN accepted")
	}
}
