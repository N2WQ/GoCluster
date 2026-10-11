//go:build !qualification

// File role: Proves original peer payload ownership across production temporal,
// FT and toxicity admission, while actual local TCP receives local decisions.
package cluster

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/internal/correctionflow"
	"dxcluster/internal/toxicity"
	"dxcluster/peer"
	"dxcluster/spot"
)

func peerOriginalCorrectionFixture(t *testing.T, temporal bool) (ambiguityDeliveryFixture, spot.ResolverSignalKey, time.Time) {
	t.Helper()
	t.Chdir(t.TempDir())
	previous := spot.CurrentDXClusterLayout()
	if err := spot.SetDXClusterLineLength(78); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := spot.SetDXClusterLineLength(previous.LineLength); err != nil {
			t.Error(err)
		}
	})
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	resolver, key := correctionEvidenceResolver(t, correctionEvidenceDeliveryCase{
		aReceivers: []string{"N0AAA", "N0BBB", "N0DDD"}, dissent: "K1ABD",
	}, 1, at)
	cfg := ambiguityDeliveryConfig(false)
	cfg.MinConsensusReports = 3
	if temporal {
		cfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
		cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 1, 1
	}
	f := newAmbiguityDeliveryFixture(t, resolver, cfg)
	if temporal {
		f.pipeline.temporal = newRuntimeTemporalController(cfg)
	}
	selection := correctionflow.SelectResolverPrimarySnapshotForCall(resolver, key, cfg, "K1ABD")
	if !selection.SnapshotOK || selection.Snapshot.Winner != "K1ABC" || selection.Snapshot.WinnerSupport != 3 {
		t.Fatalf("fixture must positively establish an eligible B3/A1 correction: %+v", selection)
	}
	return f, key, at
}

func peerOriginalAssertTCP(t *testing.T, f ambiguityDeliveryFixture, call, glyph string) {
	t.Helper()
	for _, line := range []string{
		correctionEvidenceReadDX(t, f.normalConn, f.normalReader),
		correctionEvidenceReadDX(t, f.diagConn, f.diagReader),
	} {
		if len(line) < 78 || strings.TrimSpace(line[27:39]) != call || line[71:72] != glyph {
			t.Errorf("local TCP must receive call=%s glyph=%s: %q", call, glyph, line)
		}
	}
}

func peerOriginalAssertFields(t *testing.T, socket *peerAdmissionSocket, want []string) {
	t.Helper()
	frame, err := peer.ParseFrame(peerAdmissionReadSpot(t, socket))
	if err != nil || frame.Type != "PC61" || !reflect.DeepEqual(frame.Fields, want) {
		t.Errorf("peer must receive independent source tuple: frame=%+v err=%v want=%q", frame, err, want)
	}
}

// The independent key bypasses a temporal hold, and an accepted safe decision
// bypasses AI admission. Receiving it first proves preceding holds have not
// published to either destination; the peer session preserves queue order.
func peerOriginalDeliveryBarrier(t *testing.T, f ambiguityDeliveryFixture, at time.Time) {
	t.Helper()
	s := newDeliveryTestSpot("K1XYZ", "N0AAA", at)
	s.SourceType, s.Frequency, s.ToxicityStatus = spot.SourceManual, 14030, spot.ToxicitySafe
	s.Comment = "CW"
	f.pipeline.processSpot(s, nil)
	peerOriginalAssertTCP(t, f, "K1XYZ", "?")
	peerOriginalAssertFields(t, f.peer, []string{
		"14030", "K1XYZ", at.Format("02-Jan-2006"), at.Format("1504Z"), "CW", "N0AAA", "N0CALL-1", "192.0.2.1",
	})
}

func TestPeerOriginalTemporalProductionHoldDelivery(t *testing.T) {
	f, _, at := peerOriginalCorrectionFixture(t, true)
	p := f.pipeline
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	s.Comment = "  original temporal\tcomment  "
	s.SpotterIP = "2001:db8::1"
	want := []string{"14020", "K1ABD", at.Format("02-Jan-2006"), at.Format("1504Z"), "  original temporal\tcomment  ", "N0AAA", "N0CALL-1", "2001:db8::1"}
	p.processSpot(s, nil)
	if f.ring.GetCount() != 0 || p.tracker.CallCorrections() != 0 || p.tracker.TemporalPending() != 1 || len(p.temporal.pending) != 1 {
		t.Fatalf("production admission must hold without correcting or delivering: ring=%d corrections=%d pending=%d retained=%d",
			f.ring.GetCount(), p.tracker.CallCorrections(), p.tracker.TemporalPending(), len(p.temporal.pending))
	}
	peerOriginalDeliveryBarrier(t, f, at)
	// Mutate only the local payload after capture. Release must not recapture
	// these values, even though the normal release processes this same pointer.
	s.Comment, s.SpotterIP = "local payload after hold", "203.0.113.2"
	s.Time = at.Add(time.Minute)
	due, ok := p.temporal.NextDue()
	if !ok {
		t.Fatal("production hold did not schedule a deadline")
	}
	releases := p.temporal.Drain(due.Add(time.Second), false)
	if len(releases) != 1 || releases[0].pending.spot != s || releases[0].decision.Action != correctionflow.TemporalDecisionActionApply {
		t.Fatalf("actual decoder must release the held pointer with Apply: %+v", releases)
	}
	p.processSpot(releases[0].pending.spot, &releases[0])
	if p.tracker.CallCorrections() != 1 || s.DXCallNorm != "K1ABC" {
		t.Fatalf("release failed to make the real local correction: call=%s corrections=%d", s.DXCallNorm, p.tracker.CallCorrections())
	}
	peerOriginalAssertTCP(t, f, "K1ABC", "C")
	peerOriginalAssertFields(t, f.peer, want)
	if len(p.temporal.pending) != 0 || p.temporal.queue.Len() != 0 {
		t.Fatal("released temporal owner still retains the source payload")
	}
}

func TestPeerOriginalFTProductionHoldDelivery(t *testing.T) {
	f, _, at := peerOriginalCorrectionFixture(t, false)
	p4FTEnableReceiver(t, f.normalConn, f.normalReader)
	p4FTEnableReceiver(t, f.diagConn, f.diagReader)
	p := f.pipeline
	p.ftConfidence = newFTConfidenceController(config.CallCorrectionConfig{})
	sources := []struct {
		receiver, comment, ip string
		stamp                 time.Time
	}{
		{"N0AAA", " first FT\tcomment ", "192.0.2.21", at},
		{"N0BBB", " second FT comment ", "2001:db8::22", at.Add(time.Minute)},
	}
	for _, source := range sources {
		s := spot.NewSpot("K1ABC", source.receiver, 14074.13, "FT8")
		s.SourceType, s.Time, s.Comment, s.SpotterIP = spot.SourceManual, source.stamp, source.comment, source.ip
		p.processSpot(s, nil)
		// Holding follows process arrival time, not the display timestamp.
		s.Comment, s.SpotterIP = "local held FT payload", "203.0.113.40"
	}
	if f.ring.GetCount() != 0 || p.ftConfidence.pendingSpots != 2 || len(p.ftConfidence.pending) != 1 {
		t.Fatalf("both production FT inputs must remain in one held burst: ring=%d pending=%d groups=%d",
			f.ring.GetCount(), p.ftConfidence.pendingSpots, len(p.ftConfidence.pending))
	}
	peerOriginalDeliveryBarrier(t, f, at)
	p.releaseDueFT(time.Now().UTC(), true)
	for _, source := range sources {
		// Local formatter prefixes vary with receiver; the independent call and
		// glyph positions remain fixed in both actual TCP profiles.
		peerOriginalAssertTCP(t, f, "K1ABC", "P")
		peerOriginalAssertFields(t, f.peer, []string{
			"14074.13", "K1ABC", source.stamp.Format("02-Jan-2006"), source.stamp.Format("1504Z"),
			source.comment, source.receiver, "N0CALL-1", source.ip,
		})
	}
	if f.ring.GetCount() != 3 || p.ftConfidence.pendingSpots != 0 || len(p.ftConfidence.pending) != 0 || p.ftConfidence.queue.Len() != 0 {
		t.Fatalf("forced FT release must retire all reservations and heap entries: ring=%d pending=%d groups=%d heap=%d",
			f.ring.GetCount(), p.ftConfidence.pendingSpots, len(p.ftConfidence.pending), p.ftConfidence.queue.Len())
	}
}

type peerOriginalBlockingToxicityClient struct {
	entered chan string
	release chan struct{}
}

func (c *peerOriginalBlockingToxicityClient) Classify(ctx context.Context, comment string) (toxicity.Decision, error) {
	select {
	case c.entered <- comment:
	case <-ctx.Done():
		return toxicity.Decision{}, ctx.Err()
	}
	select {
	case <-c.release:
		return toxicity.Decision{Status: spot.ToxicitySafe, Model: "peer-original-test"}, nil
	case <-ctx.Done():
		return toxicity.Decision{}, ctx.Err()
	}
}

func TestPeerOriginalToxicityProductionReentryDelivery(t *testing.T) {
	f, _, at := peerOriginalCorrectionFixture(t, false)
	p := f.pipeline
	gate, err := toxicity.NewSafeGateFromLists([]string{"CW"}, nil, 12)
	if err != nil {
		t.Fatal(err)
	}
	client := &peerOriginalBlockingToxicityClient{entered: make(chan string, 1), release: make(chan struct{})}
	p.toxicityClassifier = toxicity.NewClassifier(toxicity.Config{
		Enabled: true, Workers: 1, QueueSize: 1, CacheMaxEntries: 1, CacheTTLSeconds: 60, MaxCommentBytes: 512, TimeoutMS: 30000,
	}, gate, client)
	p.toxicityClassifier.Start()
	t.Cleanup(p.toxicityClassifier.Stop)
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType, s.Comment, s.SpotterIP = spot.SourceManual, "  please preserve\tthis comment  ", "2001:db8::31"
	p.processSpot(s, nil)
	select {
	case classified := <-client.entered:
		if classified != "please preserve this comment" {
			t.Fatalf("classifier should receive its cleaned private request: %q", classified)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("production toxicity stage did not enter blocking client")
	}
	if f.ring.GetCount() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatal("a pending toxicity decision reached correction or local fanout")
	}
	peerOriginalDeliveryBarrier(t, f, at)
	close(client.release)
	var result toxicity.Result
	select {
	case result = <-p.toxicityClassifier.Results():
	case <-time.After(3 * time.Second):
		t.Fatal("classifier did not transfer its safe result")
	}
	if result.Spot != s || !p.processToxicityResult(result) {
		t.Fatal("production result admission failed to reenter with the original spot")
	}
	if s.ToxicityStatus != spot.ToxicitySafe || s.DXCallNorm != "K1ABC" || p.tracker.CallCorrections() != 1 {
		t.Fatalf("safe reentry must preserve classification and actually correct locally: status=%s call=%s corrections=%d", s.ToxicityStatus, s.DXCallNorm, p.tracker.CallCorrections())
	}
	peerOriginalAssertTCP(t, f, "K1ABC", "C")
	peerOriginalAssertFields(t, f.peer, []string{
		"14020", "K1ABD", at.Format("02-Jan-2006"), at.Format("1504Z"), "  please preserve\tthis comment  ", "N0AAA", "N0CALL-1", "2001:db8::31",
	})
	p.toxicityClassifier.Stop()
	if snapshot := p.toxicityClassifier.Snapshot(); snapshot.Pending != 0 || snapshot.ResultBacklog != 0 {
		t.Fatalf("classification owner did not retire: %+v", snapshot)
	}
}

func TestPeerOriginalAbsentReleaseDoesNotInventPublication(t *testing.T) {
	f, key, at := peerOriginalCorrectionFixture(t, false)
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	f.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{
		spot: s, resolverKey: key, hasResolverKey: true, evidenceEnqueued: true,
	})
	if f.pipeline.tracker.CallCorrections() != 1 || s.DXCallNorm != "K1ABC" {
		t.Fatal("missing provenance fixture must still perform the real local correction")
	}
	peerOriginalAssertTCP(t, f, "K1ABC", "C")
	// A following valid publication is a FIFO barrier on the same recipient.
	// An invented publication from the preceding release would arrive first;
	// there is no negative socket deadline or scheduling assumption here.
	barrier := newDeliveryTestSpot("K1XYZ", "N0AAA", at)
	barrier.SourceType, barrier.Frequency = spot.SourceManual, 14030
	barrier.Comment = "CW"
	f.pipeline.processSpot(barrier, nil)
	peerOriginalAssertFields(t, f.peer, []string{
		"14030", "K1XYZ", at.Format("02-Jan-2006"), at.Format("1504Z"), "CW", "N0AAA", "N0CALL-1", "192.0.2.1",
	})
}

func TestPeerOriginalPublicationOwnedFields(t *testing.T) {
	at := time.Date(2026, 10, 11, 6, 7, 8, 0, time.UTC)
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType, s.Comment = spot.SourceManual, "  original\tcomment "
	s.Confidence, s.ConfidencePercent, s.ConfidencePercentOK = "C", 75, true
	s.SealForAsync()
	if s.SnapshotForAsync() != s {
		t.Fatal("fixture must exercise a sealed spot whose async snapshot aliases it")
	}
	original := captureOriginalPeerPublication(s)
	s.DXCall, s.DECall, s.Comment, s.SpotterIP, s.Frequency, s.Time = "K1ABC", "N0BBB", "changed", "203.0.113.9", 14100, at.Add(time.Hour)
	got := original.publicationSpot()
	if got.DXCall != "K1ABD" || got.DECall != "N0AAA" || got.Comment != "  original\tcomment " || got.SpotterIP != "192.0.2.1" || got.Frequency != 14020 || !got.Time.Equal(at) {
		t.Fatalf("source publication must own field values independently of a sealed Spot: %+v", &got)
	}
	if got.Confidence != "" || got.ConfidencePercentOK || got.Mode != "" || len(got.ToxicityCategories) != 0 || got.DEMetadata.Grid != "" {
		t.Fatal("private peer adapter copied unrelated local confidence or metadata")
	}
	if captureOriginalPeerPublication(nil) != nil {
		t.Fatal("nil source must not manufacture original payload")
	}
}

func TestPeerOriginalStabilizerSchedulerPreservesRecord(t *testing.T) {
	s := newDeliveryTestSpot("K1ABD", "N0AAA", time.Now().UTC())
	s.SourceType, s.Comment = spot.SourceManual, "CW"
	original := captureOriginalPeerPublication(s)
	s.DXCall, s.Comment = "K1ABC", "local later value"
	scheduler := newTelnetSpotStabilizer(time.Nanosecond, 1)
	scheduler.Start()
	t.Cleanup(scheduler.Stop)
	for check := 0; check < 2; check++ {
		if !scheduler.enqueueWithTemporalResult(s, check, "ambiguous", spot.ResolverSignalKey{}, false, false, nil, original) {
			t.Fatal("actual scheduler refused a free bounded slot")
		}
		select {
		case release := <-scheduler.ReleaseChan():
			if release == nil || release.spot != s || release.peerOriginal != original || release.checksCompleted != check || scheduler.Pending() != 0 {
				t.Fatalf("scheduler/retry lost its original record or reservation: %+v pending=%d", release, scheduler.Pending())
			}
			if got := release.peerOriginal.publicationSpot(); got.DXCall != "K1ABD" || got.Comment != "CW" {
				t.Fatalf("scheduler must retain the first accepted source fields: %+v", &got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("actual stabilizer scheduler did not release")
		}
	}
	scheduler.Stop()
	if scheduler.Pending() != 0 {
		t.Fatal("stabilizer retirement retained a source reservation")
	}
}

func TestPeerOriginalFinalSuppressionGatesActualPeer(t *testing.T) {
	for _, gate := range []string{"secondary_med", "family"} {
		t.Run(gate, func(t *testing.T) {
			f, _, at := peerOriginalCorrectionFixture(t, false)
			p := f.pipeline
			seedCall, deniedCall := "K1DUP", "K1DUP"
			if gate == "secondary_med" {
				p.secondaryMed = dedup.NewSecondaryDeduper(time.Minute, false)
				p.secondaryActive = true
			} else {
				p.familySuppressor = newTestTelnetFamilySuppressor()
				seedCall, deniedCall = "KP4/N2WQ", "N2WQ"
			}
			// Establish the existing gate through a real delivered source, not a
			// manually denied plan. Both sources are valid manual peer inputs.
			seed := newDeliveryTestSpot(seedCall, "N0AAA", at)
			seed.SourceType, seed.Frequency, seed.Comment = spot.SourceManual, 14030, "seed peer gate"
			p.processSpot(seed, nil)
			peerOriginalAssertTCP(t, f, seedCall, "?")
			peerOriginalAssertFields(t, f.peer, []string{
				"14030", seedCall, at.Format("02-Jan-2006"), at.Format("1504Z"), "seed peer gate", "N0AAA", "N0CALL-1", "192.0.2.1",
			})
			if f.ring.GetCount() != 1 {
				t.Fatal("gate seed was not actually delivered locally")
			}
			denied := newDeliveryTestSpot(deniedCall, "N0AAA", at.Add(time.Second))
			denied.SourceType, denied.Frequency, denied.Comment = spot.SourceManual, 14030, "denied peer gate"
			spot.ApplySourceHumanFlag(denied)
			if !p.peerManager.ShouldPublishLocalSpot(denied) {
				t.Fatal("gate fixture must be eligible for local peer publication")
			}
			p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: denied, peerOriginal: captureOriginalPeerPublication(denied)})
			if gate == "secondary_med" {
				if f.ring.GetCount() != 1 {
					t.Fatal("denied final MED gate reached normal local fanout")
				}
				processed, duplicates, _ := p.secondaryMed.GetStats()
				if processed != 2 || duplicates != 1 {
					t.Fatalf("release must actually encounter final MED duplicate refusal: processed=%d duplicates=%d", processed, duplicates)
				}
			} else {
				if f.ring.GetCount() != 2 || p.familySuppressor.totalEntries != 1 {
					t.Fatal("family hiding must preserve ring output without teaching hidden telnet history")
				}
				peerOriginalAssertFields(t, f.peer, []string{
					"14030", deniedCall, denied.Time.Format("02-Jan-2006"), denied.Time.Format("1504Z"),
					"denied peer gate", "N0AAA", "N0CALL-1", "192.0.2.1",
				})
			}
			// MED refusal stays shared; family hiding affects telnet alone. A
			// following source proves neither path leaves an extra peer frame.
			peerOriginalDeliveryBarrier(t, f, at)
		})
	}
}

func TestPeerOriginalRefusalKeepsActualLocalDelivery(t *testing.T) {
	for _, tc := range []struct{ name, comment string }{{"empty", ""}, {"caret", "CW^unrepresentable"}} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, at := peerOriginalCorrectionFixture(t, false)
			s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
			s.SourceType, s.Comment = spot.SourceManual, tc.comment
			f.pipeline.processSpot(s, nil)
			if f.pipeline.tracker.CallCorrections() != 1 {
				t.Fatal("peer refusal fixture must actually accept its local correction")
			}
			peerOriginalAssertTCP(t, f, "K1ABC", "C")
			assertLocal := func(label string, rows []*spot.Spot) {
				t.Helper()
				if len(rows) != 1 || rows[0].DXCall != "K1ABC" || rows[0].Confidence != "C" {
					t.Errorf("%s must retain the local corrected identity and glyph after peer refusal: %+v", label, rows)
				}
			}
			assertLocal("ring", f.ring.GetRecent(2))
			if !s.ConfidencePercentOK || s.ConfidencePercent != 75 {
				t.Errorf("peer refusal changed the accepted B3/A1 support share: available=%t percent=%d", s.ConfidencePercentOK, s.ConfidencePercent)
			}
			f.writer.Stop()
			reopened, err := archive.NewWriter(f.archiveCfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reopened.Stop)
			rows, err := reopened.Recent(2)
			if err != nil {
				t.Fatal(err)
			}
			assertLocal("reopened archive", rows)
			peerOriginalDeliveryBarrier(t, f, at)
		})
	}
}
