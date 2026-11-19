package correctionflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

type frequencyFrozenCases struct {
	TraceSHA256    string `json:"trace_sha256"`
	VerifiedFrames int    `json:"verified_frames"`
	Cases          []struct {
		Ordinal   int                          `json:"ordinal"`
		Selection ResolverSelectionDiagnostics `json:"selection"`
	} `json:"cases"`
}

func f0Frozen(t *testing.T) frequencyFrozenCases {
	t.Helper()
	b, err := os.ReadFile("testdata/frequency_consumed_v7.json")
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != "48a0f58f0e86e4b9670721d9d8a52a93d131659dc522be9cdd1af74d95f7907a" {
		t.Fatal("frozen evidence changed")
	}
	var fixture frequencyFrozenCases
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.TraceSHA256 != "139bac19958dfcd38c7dc7316b15831e407bdd51d4b5c93bcaf4157dfb8e78a7" || fixture.VerifiedFrames != 621 || len(fixture.Cases) != 4 {
		t.Fatal("capture binding mismatch")
	}
	return fixture
}

func TestCCV2F0FrozenWitnessControls(t *testing.T) {
	for _, c := range f0Frozen(t).Cases {
		d := c.Selection
		sources, err := ImportFrequencySources(d, 2)
		if err != nil {
			t.Fatalf("ordinal %d: %v", c.Ordinal, err)
		}
		key := frequencyDiagnosticKey(d.InputKey)
		o := ResolverPrimarySelection{SnapshotOK: d.Output.SnapshotOK, NeighborhoodSplit: d.Output.NeighborhoodSplit,
			Snapshot: spot.ResolverSnapshot{Key: key, State: d.Output.Snapshot.State, EvidenceLoss: d.Output.Snapshot.EvidenceLoss}}
		cfg := config.CallCorrectionConfig{MaxEditDistance: 3, ResolverNeighborhoodEnabled: true, ResolverNeighborhoodBucketRadius: 1,
			ResolverNeighborhoodMaxDistance: 1, DistanceModelCW: "plain", DistanceModelRTTY: "plain", FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6}
		focal := 7021.0
		if c.Ordinal == 174575 {
			focal = 14005.9
		}
		if c.Ordinal == 432308 {
			focal = 3538
		}
		if c.Ordinal == 480813 {
			focal = 14027.5
		}
		got := AssessFrequencyProposals(key, d.Anchor, focal, o, sources, cfg, FrequencyProposalOptions{150, time.Minute, 2})
		t.Logf("ordinal=%d reason=%s components=%d excluded=%d proposals=%+v", c.Ordinal, got.Reason, got.Components, got.ExcludedComponents, got.Proposals)
		switch c.Ordinal {
		case 520537:
			if got.Reason != "assessed" || got.ExcludedComponents != 1 {
				t.Fatalf("DL3 assessment unavailable: %+v", got)
			}
			found := false
			for _, p := range got.Proposals {
				if p.Identity == "DL3KWF" {
					// HA3NU's DK0KK receiver also belongs to DL3KWF. Retaining
					// its dissent does not add a fifteenth distinct receiver.
					found = p.Support == 14 && p.TotalReporters == 14 && p.ConfidencePercent == 100
				}
			}
			if !found {
				t.Fatal("DL3 current support/group counts not recovered")
			}
		case 174575:
			if got.ExcludedComponents != 0 {
				t.Fatal("R9 time-only gap promoted")
			}
			for _, p := range got.Proposals {
				if p.ConfidencePercent >= 45 {
					t.Fatal("R9 dissent erased")
				}
			}
		case 480813:
			if got.Reason != "ordinary_protected" || len(got.Proposals) != 0 {
				t.Fatal("N2QLV conflict protection lost")
			}
		case 432308:
			if got.ExcludedComponents != 0 {
				t.Fatal("same-frequency reference control separated")
			}
		}
	}
}

func TestCCV2F0ImportCapture(t *testing.T) {
	for _, mutate := range []func(*DiagnosticLookup){
		func(r *DiagnosticLookup) { r.Snapshot.TotalReporters++ },
		func(r *DiagnosticLookup) { r.Snapshot.TotalWeightedSupportMilli++ },
		func(r *DiagnosticLookup) { r.Snapshot.Candidates[0].Support++ },
		func(r *DiagnosticLookup) { r.Snapshot.Candidates[0].VoteKey = "CORRUPT" },
		func(r *DiagnosticLookup) { r.Snapshot.Candidates[0].ObservedCalls = []string{"CORRUPT"} },
		func(r *DiagnosticLookup) { r.RawRows[0].RawOrigin = "invented:1" },
	} {
		// Reload nested slices for every corruption: no shared mutable fixture.
		fixture := f0Frozen(t)
		d := fixture.Cases[3].Selection
		if _, err := ImportFrequencySources(d, 2); err != nil {
			t.Fatal(err)
		}
		for i := range d.Reads {
			if d.Reads[i].Present && len(d.Reads[i].RawRows) > 0 {
				mutate(&d.Reads[i])
				break
			}
		}
		if _, err := ImportFrequencySources(d, 2); err == nil {
			t.Fatal("corrupt captured metadata accepted")
		}
	}
}
