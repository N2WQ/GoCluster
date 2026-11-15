package floodcorpus

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/spot"
)

func syntheticEvent(arrival int, source, logical, kind string) Event {
	return Event{Version: Schema, Arrival: arrival, NS: int64(arrival - 1), Source: source, Transport: "generated", Spot: &SpotRecord{DX: "K1AAA", DE: "W1AAA", Node: "N0ORG", Mode: "CW", Frequency: 14020, Time: "2026-11-15T12:00:00Z"}, Truth: &TruthLabel{Kind: kind, LogicalID: logical, Provenance: "independent-fixture"}}
}

func corpusFixture(t testing.TB, events []Event) (string, Manifest) {
	t.Helper()
	dir := t.TempDir()
	var b bytes.Buffer
	for _, e := range events {
		if err := writeEvent(&b, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), b.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b.Bytes())
	m := Manifest{Version: Schema, Stage: corpusStage, Synthetic: true, Transport: "generated", Events: len(events), EventsSHA256: hex.EncodeToString(h[:]), Complete: true, CaptureComplete: true, Timing: "generated", Eligibility: "synthetic assumed eligible", Implementation: buildIdentity()}
	if err := writeExclusiveJSON(filepath.Join(dir, "manifest.json"), m); err != nil {
		t.Fatal(err)
	}
	return dir, m
}

func TestReplenishingIndependentBoundaryVectors(t *testing.T) {
	o := Options{Threshold: 2, WindowSeconds: 10, ActorCap: 1}
	r := newReplenishing(o)
	s := spot.NewSpotNormalized("K1AAA", "W1AAA", 14020, "CW")
	s.SourceType = spot.SourceManual
	for i, tc := range []struct {
		ns    int64
		allow bool
	}{{0, true}, {0, true}, {0, false}, {5e9 - 1, false}, {5e9, true}, {5e9, false}, {10e9, true}, {100e9, true}, {100e9, true}, {100e9, false}} {
		if got := r.admit(tc.ns, s); got != tc.allow {
			t.Fatalf("vector%d ns%d allowed%v want%v", i, tc.ns, got, tc.allow)
		}
	}
	s.DECall, s.DECallNorm = "W2BBB", "W2BBB"
	if !r.admit(100e9, s) || r.overflow != 1 || r.actors != 1 {
		t.Fatal("actor capacity did not fail open")
	}
	for _, source := range []spot.SourceType{spot.SourceRBN, spot.SourceFT4, spot.SourceFT8, spot.SourcePSKReporter} {
		s.SourceType = source
		if !r.admit(100e9, s) || r.actors != 1 {
			t.Fatal("native candidate reporter quota")
		}
	}
	maximum := Options{Threshold: 100000, WindowSeconds: 86400, ActorCap: 1}
	if err := maximum.validate(); err != nil {
		t.Fatal(err)
	}
	r = newReplenishing(maximum)
	s.SourceType = spot.SourceManual
	if !r.admit(0, s) || !r.admit(math.MaxInt64, s) {
		t.Fatal("long gap saturation failed")
	}
	for _, bad := range []Options{{math.MaxInt, 1, 1, false}, {1, math.MaxInt, 1, false}, {0, 10, 1, false}, {1, 1, 0, false}} {
		if bad.validate() == nil {
			t.Fatal("unsafe options accepted")
		}
	}
	// Independent fractional-credit vector: one third of ten seconds is still
	// short by one credit unit; the next nanosecond crosses the exact boundary.
	r = newReplenishing(Options{Threshold: 3, WindowSeconds: 10, ActorCap: 1})
	for range 3 {
		if !r.admit(0, s) {
			t.Fatal("initial burst")
		}
	}
	if r.admit(3333333333, s) || !r.admit(3333333334, s) || !r.admit(6666666667, s) {
		t.Fatal("fractional boundary wrong")
	}
}

func TestTruthCounterfactualAndGenuineRepeatAmbiguity(t *testing.T) {
	events := []Event{syntheticEvent(1, "MANUAL", "one", "known_post"), syntheticEvent(2, "MANUAL", "one", "known_copy"), syntheticEvent(3, "MANUAL", "two", "known_post"), syntheticEvent(4, "MANUAL", "three", "known_post"), syntheticEvent(5, "MANUAL", "four", "known_post"), syntheticEvent(6, "UPSTREAM", "one", "known_post")}
	events[2].Spot.DX = "K1BBB"
	events[4].Spot.Frequency = 14021
	dir, _ := corpusFixture(t, events)
	r, err := Compare(dir, Options{Threshold: 2, WindowSeconds: 10, ActorCap: 4})
	if err != nil || r.LogicalPosts == nil || *r.LogicalPosts != 5 || r.RecoveredLogicalPosts == nil || *r.RecoveredLogicalPosts != 1 || r.IndistinguishablePosts == nil || *r.IndistinguishablePosts != 1 || r.KnownCopyArrivals != 1 {
		t.Fatalf("truth comparison: %+v %v", r, err)
	}
	// Frequency makes the fourth genuine post distinguishable. The same truth
	// ID in another selected source partition remains a different allowance.
	for i := range events {
		events[i].Truth = nil
	}
	dir, _ = corpusFixture(t, events)
	r, err = Compare(dir, Options{Threshold: 2, WindowSeconds: 10, ActorCap: 4})
	if err != nil || r.RecoveredLogicalPosts != nil || r.IndistinguishablePosts != nil || r.LogicalPosts != nil {
		t.Fatalf("unlabelled harm inferred: %+v %v", r, err)
	}
}

func TestGeneratedScenariosRepeatAndStayOffline(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "generated")
	m, err := Generate(dir)
	if err != nil || m.Events != 30 {
		t.Fatalf("generation: %+v %v", m, err)
	}
	o := Options{Threshold: 2, WindowSeconds: 10, ActorCap: 8}
	a, err := Compare(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Compare(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	if !bytes.Equal(left, right) || a.EventsSHA256 != m.EventsSHA256 || a.Arrivals != m.Events || a.RecoveredLogicalPosts == nil || *a.RecoveredLogicalPosts != 1 || a.RollingAllowed >= a.Arrivals {
		t.Fatalf("generated controls missed actual suppression/recovery: %+v", a)
	}
	if !bytes.Contains(left, []byte("successful allowances only")) || bytes.Contains(left, []byte("W1AAA")) {
		t.Fatal("report hides semantic difference or publishes source data")
	}
	found := false
	for _, s := range a.Scenarios {
		if s.Name == "slower_recovery" {
			found = true
			if s.RollingResumed == nil || *s.RollingResumed != 92e9 || s.ReplenishingResumed == nil || *s.ReplenishingResumed != 86e9 {
				t.Fatalf("recovery timing: %+v", s)
			}
		}
	}
	if !found {
		t.Fatal("missing generated recovery summary")
	}
}

func TestCorpusRefusesTamperedContracts(t *testing.T) {
	for _, scalar := range []string{"null", "0.0", `"0"`} {
		t.Run(scalar, func(t *testing.T) {
			dir, m := corpusFixture(t, []Event{syntheticEvent(1, "MANUAL", "one", "known_post")})
			path := filepath.Join(dir, "events.jsonl")
			raw, _ := os.ReadFile(path)
			raw = bytes.Replace(raw, []byte(`"receipt_ns":0`), []byte(`"receipt_ns":`+scalar), 1)
			os.WriteFile(path, raw, 0600)
			h := sha256.Sum256(raw)
			m.EventsSHA256 = hex.EncodeToString(h[:])
			encoded, _ := json.Marshal(m)
			os.WriteFile(filepath.Join(dir, "manifest.json"), encoded, 0600)
			if _, err := Compare(dir, Options{Threshold: 2, WindowSeconds: 10, ActorCap: 4}); err == nil {
				t.Fatal("noninteger event time accepted")
			}
		})
	}
	input := writeFixture(t, encodeJournal(t, journalRecords([]capturedBlock{{"s2c", 1, testWire("PC11", "K1AAA", "CQ")}})))
	dir := filepath.Join(t.TempDir(), "captured")
	m, err := Convert(input, dir, "s2c", "peer-A")
	if err != nil {
		t.Fatal(err)
	}
	e := syntheticEvent(1, "MANUAL", "one", "known_post")
	var b bytes.Buffer
	writeEvent(&b, e)
	os.WriteFile(filepath.Join(dir, "events.jsonl"), b.Bytes(), 0600)
	h := sha256.Sum256(b.Bytes())
	m.EventsSHA256 = hex.EncodeToString(h[:])
	raw, _ := json.Marshal(m)
	os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600)
	if _, err := Compare(dir, Options{Threshold: 2, WindowSeconds: 10, ActorCap: 4}); err == nil {
		t.Fatal("captured connection provenance changed to MANUAL")
	}
	m.Sent = m.Received + 1
	raw, _ = json.Marshal(m)
	os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600)
	if _, err := readManifest(dir); err == nil {
		t.Fatal("invalid manifest counts accepted")
	}
}

func TestEventRecordAndCountLimits(t *testing.T) {
	dir, m := corpusFixture(t, nil)
	file, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= MaxEvents+1; i++ {
		if _, err := fmt.Fprintf(file, "{\"version\":1,\"arrival\":%d,\"receipt_ns\":0,\"source\":\"MANUAL\",\"transport\":\"generated\"}\n", i); err != nil {
			t.Fatal(err)
		}
	}
	file.Close()
	m.Events = MaxEvents
	visited := 0
	if err := readEvents(dir, m, func(Event) error { visited++; return nil }); err == nil || err.Error() != "event record or count limit" || visited != MaxEvents {
		t.Fatalf("event count guard missed: visited=%d error=%v", visited, err)
	}
	large := syntheticEvent(1, "MANUAL", "one", "known_post")
	large.Spot.Node = string(bytes.Repeat([]byte{'x'}, MaxRecordBytes))
	raw, err := json.Marshal(large)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(raw)
	m.Events = 1
	m.EventsSHA256 = hex.EncodeToString(h[:])
	visited = 0
	if err := readEvents(dir, m, func(Event) error { visited++; return nil }); err == nil || err.Error() != "event record or count limit" || visited != 0 {
		t.Fatalf("event record guard missed: visited=%d error=%v", visited, err)
	}
}

func TestRollingRecoveryVectors(t *testing.T) {
	c := rolling(Options{Threshold: 2, WindowSeconds: 10, ActorCap: 4})
	s := spot.NewSpotNormalized("K1AAA", "W1AAA", 14020, "CW")
	s.SourceType = spot.SourceManual
	base := time.Unix(0, 0)
	for i, tc := range []struct {
		ns    int64
		allow bool
	}{{0, true}, {0, true}, {0, false}, {10e9, false}, {10e9 + 1, true}, {30e9, true}} {
		if got := c.Admit(base.Add(time.Duration(tc.ns)), s); got != tc.allow {
			t.Fatalf("vector%d got%v want%v", i, got, tc.allow)
		}
	}
}
