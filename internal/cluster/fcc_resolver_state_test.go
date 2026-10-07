// File role: Exercises FCC state enrichment after an actual resolver correction,
// including immediate delivery, stabilizer release and decoded archive storage.
package cluster

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestFCCResolverCorrectionDeliveryArchive(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, delayed := range []bool{false, true} {
			t.Run(fmt.Sprintf("enforcement=%t/delayed=%t", enabled, delayed), func(t *testing.T) {
				configureFCCStateFixture(t, enabled)
				db := loadIngestCTY(t)
				cfg := config.CallCorrectionConfig{
					Enabled:                 true,
					MaxEditDistance:         6,
					MinConsensusReports:     2,
					MinAdvantage:            1,
					FrequencyToleranceHz:    500,
					DistanceModelCW:         "morse",
					DistanceModelRTTY:       "baudot",
					InvalidAction:           "broadcast",
					StabilizerEnabled:       true,
					StabilizerMaxChecks:     1,
					StabilizerTimeoutAction: stabilizerTimeoutRelease,
				}
				resolver := newFCCStateCorrectionResolver(t, cfg)
				writer, err := archive.NewWriter(config.ArchiveConfig{
					DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				writer.Start()
				t.Cleanup(writer.Stop)
				ring := buffer.NewRingBuffer(4)
				srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
				p := newDeliveryTestPipeline(ring, writer, srv)
				p.ctyLookup = func() *cty.CTYDatabase { return db }
				p.signalResolver, p.correctionCfg = resolver, cfg
				v := newIngestValidator(p.ctyLookup, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, true)
				s := spot.NewSpot("K1ABC", "N2AAA", 14020, "CW")
				if !v.validateSpot(s) || applyLicenseGate(s, db, nil, nil) || s.DXMetadata.State != "CA" || s.DEMetadata.State != "AP" {
					t.Fatalf("initial licensed identity: DX=%+v DE=%+v", s.DXMetadata, s.DEMetadata)
				}

				// Only the production resolver stage may change this identity. The
				// initial CA fact makes stale metadata observable after correction.
				if delayed {
					p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s})
				} else {
					ctx := &outputSpotContext{spot: s, ctyDB: db, modeUpper: "CW"}
					if !p.applyResolverStage(ctx, nil) || !p.finalizeSpotForMetrics(ctx) || !p.prepareFanoutSpot(ctx) {
						t.Fatal("resolver-corrected spot was suppressed")
					}
					p.deliverSpot(ctx)
				}
				if s.DXCallNorm != "K1ABD" || s.Confidence != "C" {
					t.Fatalf("resolver did not apply its winner: call=%q confidence=%q", s.DXCallNorm, s.Confidence)
				}
				assertFCCResolverStateDelivery(t, ring, srv, writer)
			})
		}
	}
}

func newFCCStateCorrectionResolver(t *testing.T, cfg config.CallCorrectionConfig) *spot.SignalResolver {
	t.Helper()
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 64, MaxActiveKeys: 16, MaxCandidatesPerKey: 8, MaxReportersPerCand: 16,
		InactiveTTL: time.Minute, EvalMinInterval: 5 * time.Millisecond, SweepInterval: 5 * time.Millisecond,
		HysteresisWindows: 1, FreqGuardRunnerUpRatio: 0.6, MaxEditDistance: 3,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)
	var key spot.ResolverSignalKey
	for _, reporter := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		evidence, ok := buildResolverEvidenceSnapshot(spot.NewSpot("K1ABD", reporter, 14020, "CW"), cfg, nil, time.Now().UTC())
		if !ok || !resolver.Enqueue(evidence) {
			t.Fatal("could not enqueue actual resolver evidence")
		}
		key = evidence.Key
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, ok := resolver.Lookup(key)
		if ok && snapshot.Winner == "K1ABD" && snapshot.WinnerSupport == 3 &&
			(snapshot.State == spot.ResolverStateConfident || snapshot.State == spot.ResolverStateProbable) {
			return resolver
		}
		select {
		case <-deadline.C:
			t.Fatalf("resolver did not select seeded winner: available=%t snapshot=%+v", ok, snapshot)
		case <-ticker.C:
		}
	}
}

func assertFCCResolverStateDelivery(t *testing.T, ring *buffer.RingBuffer, srv *telnet.Server, writer *archive.Writer) {
	t.Helper()
	check := func(s *spot.Spot) bool {
		return s != nil && s.DXCallNorm == "K1ABD" && s.Confidence == "C" && s.DXMetadata.State == "TX" && s.DEMetadata.State == "AP"
	}
	out, ok := tryReadTelnetBroadcastSpot(srv)
	if !ok || out == nil {
		t.Fatal("corrected spot did not reach the broadcast queue")
	}
	if !check(out) || ring.GetCount() != 1 {
		t.Fatalf("corrected delivery: count=%d call=%q confidence=%q DX=%+v DE=%+v", ring.GetCount(), out.DXCallNorm, out.Confidence, out.DXMetadata, out.DEMetadata)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, err := writer.Recent(1)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 {
			if !check(rows[0]) {
				t.Fatalf("corrected decoded archive: %+v", rows[0])
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("corrected spot did not reach the archive")
		case <-ticker.C:
		}
	}
}
