//go:build !qualification

// File role: Proves accepted-observation learning follows actual final identity
// and preserves CustomSCP V-only admission after accepted/rejected replacements.
package cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestAcceptedLearningFinalIdentity(t *testing.T) {
	source := acceptedLearningPathsConfigSource(t)
	for _, custom := range []bool{false, true} {
		for _, rejected := range []bool{false, true} {
			t.Run(fmt.Sprintf("custom_%t/rejected_%t", custom, rejected), func(t *testing.T) {
				temporalDeliveryDirectory(t)
				f := newFamilyDeliveryFixture(t, source, custom, false, 60, 8, stabilizerTimeoutRelease, true)
				p := f.pipeline
				// Loaded production construction supplies all store/fanout owners.
				// This fixture isolates the immediate acceptance boundary; separate
				// path tests cover actual scheduler admission and resolution.
				p.stabilizerEnabled = false
				p.telnetStabilizer.Stop()
				p.correctionCfg.MinConsensusReports, p.correctionCfg.MinAdvantage, p.correctionCfg.MinConfidencePercent = 2, 1, 45
				p.correctionCfg.MaxEditDistance = 6
				p.correctionCfg.ResolverRecentPlus1Enabled, p.correctionCfg.BayesBonus.Enabled = false, false
				p.correctionCfg.TemporalDecoder.Enabled = false
				p.temporal = newRuntimeTemporalController(p.correctionCfg)
				if rejected {
					p.correctionCfg.InvalidAction = "broadcast"
					// The replacement has no CTY match, while the receiver still has
					// eligible metadata after the production license-gate refresh.
					db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<plist><dict><key>N0</key><dict><key>Country</key><string>United States</string><key>Prefix</key><string>N0</string><key>ADIF</key><integer>291</integer><key>Continent</key><string>NA</string><key>CQZone</key><integer>5</integer></dict><key>K1CTL</key><dict><key>Country</key><string>United States</string><key>Prefix</key><string>K1CTL</string><key>ADIF</key><integer>291</integer><key>Continent</key><string>NA</string><key>CQZone</key><integer>5</integer></dict></dict></plist>`))
					if err != nil {
						t.Fatal(err)
					}
					p.ctyLookup = func() *cty.CTYDatabase { return db }
				}
				at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
				control := acceptedLearningPathsControl(t, f, at)
				r, driver, _ := correctionLossResolver(t, 4, 8)
				t.Cleanup(r.Stop)
				key := spot.NewResolverSignalKey(14030, "20m", "CW", p.correctionCfg.FrequencyToleranceHz)
				observed := at.Add(-time.Second)
				correctionLossAdmit(t, r, driver, key, "K1ABC", 14030, observed, 2*time.Minute, "N0BBB", "N0CCC")
				correctionLossAdmit(t, r, driver, key, "K1ABD", 14030, observed, 2*time.Minute, "N0AAA")
				selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, p.correctionCfg, "K1ABD", time.Now().UTC())
				if !selection.SnapshotOK || selection.NeighborhoodSplit || selection.Snapshot.EvidenceLoss || selection.Snapshot.Winner != "K1ABC" || selection.Snapshot.State != spot.ResolverStateConfident || correctionflow.ResolverSupportForCall(selection.Snapshot, "K1ABC") != 2 || correctionflow.ResolverSupportForCall(selection.Snapshot, "K1ABD") != 1 {
					t.Fatalf("actual current B2/A1 correction population is required: %+v", selection)
				}
				p.signalResolver = r
				incoming := familyDeliveryInput{"K1ABD", "N0AAA", "learning identity input", "?", at.Add(time.Second)}
				s := incoming.spot()
				p.processSpot(s, nil)
				local := incoming
				local.call, local.glyph = "K1ABC", "C"
				wantCorrections, wantPercent := uint64(1), 66
				wantA, wantB := 0, 1
				if rejected {
					local.call, local.glyph = "K1ABD", "B"
					wantCorrections, wantPercent, wantA, wantB = 0, 33, 1, 0
					if p.tracker.CorrectionDecisionReasons()[resolverDecisionCTYMiss] != 1 {
						t.Fatal("fixture did not exercise the actual rejected replacement")
					}
				}
				if s.DXCallNorm != local.call || s.Confidence != local.glyph || !s.ConfidencePercentOK || s.ConfidencePercent != wantPercent || p.tracker.CallCorrections() != wantCorrections || p.tracker.StabilizerHeld() != 0 {
					t.Fatalf("actual final acceptance changed: spot=%+v corrections=%d", s, p.tracker.CallCorrections())
				}
				acceptedLearningPathsWho(t, f, "K1ABD", wantA)
				acceptedLearningPathsWho(t, f, "K1ABC", wantB)
				if custom {
					wantA, wantB = 0, 0 // Both B and C remain outside V-only storage.
					if f.custom.StaticContains("K1ABD") || f.custom.StaticContains("K1ABC") {
						t.Fatal("non-V correction outcome acquired CustomSCP membership")
					}
				}
				if acceptedLearningPathsSupport(f, "K1ABD") != wantA || acceptedLearningPathsSupport(f, "K1ABC") != wantB {
					t.Fatalf("store-specific learning must follow final identity: A want=%d B want=%d", wantA, wantB)
				}
				for _, line := range []string{
					correctionEvidenceReadDX(t, f.normalConn, f.normalReader),
					correctionEvidenceReadDX(t, f.diagConn, f.diagReader),
					correctionEvidenceReadDX(t, f.selfConn, f.selfReader),
				} {
					if len(line) < 78 || strings.TrimSpace(line[27:39]) != local.call || line[71:72] != local.glyph {
						t.Errorf("actual TCP must deliver final accepted identity/glyph: %q", line)
					}
				}
				peerOriginalAssertFields(t, f.peer, []string{"14030", incoming.call, incoming.at.Format("02-Jan-2006"), incoming.at.Format("1504Z"), incoming.comment, incoming.receiver, "N0CALL-1", "192.0.2.1"})
				acceptedLearningIdentityStored(t, f, []familyDeliveryInput{control, local}, local.call, wantPercent)
				acceptedLearningPathsAssertCustomDisk(t, f, []familyDeliveryInput{control})
			})
		}
	}
}

func acceptedLearningIdentityStored(t *testing.T, f familyDeliveryFixture, inputs []familyDeliveryInput, call string, percent int) {
	t.Helper()
	rows := f.ring.GetRecent(8)
	f.assertRows(t, "accepted identity ring", rows, inputs, true)
	for _, row := range rows {
		if row.DXCallNorm == call && (!row.ConfidencePercentOK || row.ConfidencePercent != percent || row.SpotterIP != "192.0.2.1") {
			t.Error("final learned identity disagrees with local percentage or original source IP")
		}
	}
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Stop()
	rows, err = reopened.Recent(8)
	if err != nil {
		t.Fatal(err)
	}
	f.assertRows(t, "reopened accepted identity archive", rows, inputs, false)
	for _, row := range rows {
		if row.ConfidencePercentOK || row.ConfidencePercent != 0 {
			t.Error("archive acquired a percentage outside its existing format")
		}
	}
}
