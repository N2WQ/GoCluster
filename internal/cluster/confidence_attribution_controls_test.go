//go:build !qualification

package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestCallCorrectionDeliveredAttributionControls(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, mode := range []string{"gate_rejected", "unchanged", "split", "temporal_abstain", "missing", "disabled", "local_self", "ft_owned", "beacon_owned"} {
		t.Run(mode, func(t *testing.T) {
			cfg := confidenceAttributionConfig()
			at := time.Now().UTC().Truncate(time.Second)
			observed := at.Add(-3 * time.Second)
			r, driver, key := confidenceAttributionPopulation(t, observed, "K1ABC", nil)
			if mode == "gate_rejected" {
				cfg.MinConsensusReports = 3
			}
			if mode == "disabled" {
				cfg.Enabled = false
			}
			if mode == "temporal_abstain" {
				cfg.TemporalDecoder.MinMarginScore, cfg.TemporalDecoder.OverflowAction = 50000, "abstain"
			}
			if mode == "split" {
				temporalDeliverySeed(t, r, driver, key, observed.Add(time.Millisecond), 2*time.Minute, "K1ABC", "K1ABD", 3, 2)
				driver.Sweep(observed.Add(2 * time.Millisecond))
				published, ok := r.Lookup(key)
				if !ok || published.State != spot.ResolverStateSplit || published.TotalReporters != 5 {
					t.Fatal("split control did not publish literal B3/A2 ambiguity")
				}
			}
			if mode == "missing" {
				r = nil
			}
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			if mode == "ft_owned" {
				// Default human filters exclude FT8; admit it through actual
				// command replies before checking the owned-rail delivery.
				p4FTEnableReceiver(t, f.normalConn, f.normalReader)
				p4FTEnableReceiver(t, f.diagConn, f.diagReader)
			}
			s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
			s.SourceType = spot.SourceManual
			want := correctionEvidenceDeliveryCase{wantCall: "K1ABD", wantGlyph: "P", wantPercent: 33}
			unavailable, comment, outputMode := false, "CW", "CW"
			switch mode {
			case "unchanged":
				s.DXCall, s.DXCallNorm = "K1ABC", "K1ABC"
				want.wantCall, want.wantGlyph, want.wantPercent = "K1ABC", "V", 66
			case "split":
				want.wantPercent = 40
			case "missing":
				// Missing eligible support must clear a supplied stale percentage.
				s.ConfidencePercent, s.ConfidencePercentOK = 77, true
				want.wantGlyph, unavailable = "?", true
			case "disabled":
				s.Confidence = "V"
				want.wantGlyph, unavailable = "V", true
			case "local_self":
				s.DXCall, s.DXCallNorm = "N0AAA", "N0AAA"
				want.wantCall, want.wantGlyph, unavailable = "N0AAA", "V", true
				if !spot.IsLocalSelfSpot(s) {
					t.Fatal("local self-spot control does not qualify the authoritative bypass")
				}
			case "ft_owned":
				// A pre-stamped FT result belongs to its corroboration rail. The
				// CW resolver publication must not donate B's percentage to it.
				s.Mode, s.ModeNorm, s.Confidence = "FT8", "FT8", "P"
				unavailable, comment, outputMode = true, "FT8", "FT8"
			case "beacon_owned":
				s.BeaconSourceClass, s.Comment = true, "BEACON"
				want.wantGlyph, unavailable, comment = "V", true, "CW BEACON"
			}
			if unavailable {
				want.wantPercent = 0
			}
			want.subject = s.DXCall
			if mode == "temporal_abstain" {
				controller := newRuntimeTemporalController(cfg)
				f.pipeline.temporal = controller
				selection := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
				temporalDeliveryObserve(t, controller, observed, key, s, selection)
				releases := controller.Drain(time.Now().UTC(), false)
				if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionAbstain {
					t.Fatalf("control failed to exercise actual configured abstain: %+v", releases)
				}
				f.pipeline.processSpot(releases[0].pending.spot, &releases[0])
			} else {
				f.pipeline.processSpot(s, nil)
			}
			if mode == "gate_rejected" && f.pipeline.tracker.CorrectionDecisionReasons()[resolverGateDecisionReason("min_reports")] != 1 {
				t.Errorf("control did not reach ordinary report gate: %v", f.pipeline.tracker.CorrectionDecisionReasons())
			}
			if mode == "unchanged" && f.pipeline.tracker.CorrectionDecisionReasons()[resolverDecisionSameCall] != 1 {
				t.Error("control did not exercise unchanged winner")
			}
			if mode == "ft_owned" || mode == "beacon_owned" {
				confidenceAttributionCheckOwnedRail(t, f, want, at, outputMode, comment)
			} else if unavailable {
				temporalDeliveryCheckWithoutPercent(t, f, want, at, true)
			} else {
				f.check(t, want, at)
			}
		})
	}
}

// Same connected fixture, with explicit mode/comment oracles for rails whose
// output contract differs from the CW correction fixture. No new server is used.
func confidenceAttributionCheckOwnedRail(t *testing.T, f ambiguityDeliveryFixture, want correctionEvidenceDeliveryCase, at time.Time, mode, comment string) {
	t.Helper()
	for _, diag := range []bool{false, true} {
		conn, reader, expectedComment := f.normalConn, f.normalReader, comment
		if diag {
			// DIAG CONF substitutes its tag for the source comment; the mode
			// prefix remains. The beacon's normal BEACON text is not appended.
			conn, reader, expectedComment = f.diagConn, f.diagReader, mode+" --%"
		}
		if got, expected := correctionEvidenceReadDX(t, conn, reader), correctionEvidenceWire(want.wantCall, want.wantGlyph, expectedComment, at); got != expected {
			t.Errorf("owned %s rail TCP diag=%t: got %q want %q", mode, diag, got, expected)
		}
	}
	peerComment := " "
	if comment == "CW BEACON" {
		peerComment = "BEACON"
	}
	fields := strings.Split(peerAdmissionReadSpot(t, f.peer), "^")
	if len(fields) < 9 || fields[0] != "PC61" || fields[2] != want.subject || fields[5] != peerComment || fields[6] != "N0AAA" || fields[7] != "N0CALL-1" {
		t.Errorf("owned %s rail peer fields: %v", mode, fields)
	}
	rows := f.ring.GetRecent(2)
	if len(rows) != 1 {
		t.Errorf("owned %s rail ring count=%d", mode, len(rows))
	} else {
		confidenceAttributionAssertOwnedSpot(t, "sealed ring", rows[0], want, at, mode)
		if rows[0].ConfidencePercent != 0 || rows[0].ConfidencePercentOK || rows[0].SnapshotForAsync() != rows[0] {
			t.Error("owned rail borrowed resolver percentage or lost sealed ownership")
		}
	}
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err = reopened.Recent(2)
	if err != nil || len(rows) != 1 {
		t.Fatalf("owned %s rail archive rows=%v err=%v", mode, rows, err)
	}
	confidenceAttributionAssertOwnedSpot(t, "reopened archive", rows[0], want, at, mode)
	if f.pipeline.tracker.CallCorrections() != 0 {
		t.Error("owned rail recorded a resolver correction")
	}
}

func confidenceAttributionAssertOwnedSpot(t *testing.T, destination string, s *spot.Spot, want correctionEvidenceDeliveryCase, at time.Time, mode string) {
	t.Helper()
	if s == nil || s.DXCallNorm != want.wantCall || s.DECallNorm != "N0AAA" || s.Frequency != 14020 ||
		s.ModeNorm != mode || s.Confidence != want.wantGlyph || !s.Time.Equal(at) {
		t.Errorf("%s owned rail fields=%+v want %s/%s/%s at %s", destination, s, want.wantCall, mode, want.wantGlyph, at)
	}
	if mode == "CW" && s != nil && !s.IsBeacon {
		t.Errorf("%s lost owned beacon classification", destination)
	}
}
