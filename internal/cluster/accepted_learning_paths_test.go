//go:build !qualification

// File role: Proves accepted-observation learning through loaded production
// construction, real delayed ownership and output thinning. Per-occurrence
// WhoSpotsMe controls distinguish duplicate recording from receiver overwrites.
package cluster

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"dxcluster/dedup"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func acceptedLearningPathsConfigSource(t *testing.T) string {
	t.Helper()
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func acceptedLearningPathsFixture(t *testing.T, source string, custom bool, delay, capacity int, withTelnet, supportFloor bool) familyDeliveryFixture {
	t.Helper()
	// Reuse private loaded storage/configuration without starting a telnet
	// owner. The actual constructor below receives the explicit fixture gates.
	f := newFamilyDeliveryFixture(t, source, custom, false, delay, capacity, stabilizerTimeoutRelease, false)
	base := f.pipeline
	cfg := base.correctionCfg
	if supportFloor {
		// The checked-in config disables the legacy S floor. This fixture
		// deliberately enables that existing feature before construction and
		// its positive eligibility control, rather than after learning.
		cfg.RecentBandBonusEnabled = true
	}
	var srv *telnet.Server
	if withTelnet {
		port := runtimeQualificationPort(t)
		srv = telnet.NewServer(telnet.ServerOptions{Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n",
			MaxConnections: 4, BroadcastWorkers: 1, BroadcastQueue: 8, WorkerQueue: 8, ClientBuffer: 8,
			DefaultDedupePolicy: "FAST", DedupeFastEnabled: true, DedupeMedEnabled: true, DedupeSlowEnabled: true,
			BroadcastBatchIntervalSet: true}, nil)
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.Stop)
		familyAdmissionJoinClients(t, srv)
		normal := familyAdmissionLogin(t, port, "DL1ABC", "OFF", "SLOW")
		diag := familyAdmissionLogin(t, port, "DL2ABC", "CONF", "SLOW")
		self := familyAdmissionLogin(t, port, "N2WQ", "OFF", "SLOW")
		for _, client := range []familyAdmissionClient{normal, diag, self} {
			familyAdmissionCommand(t, client, "SHOW DEDUPE", "Dedupe: SLOW")
		}
		f.normalConn, f.normalReader = normal.conn, normal.reader
		f.diagConn, f.diagReader = diag.conn, diag.reader
		f.selfConn, f.selfReader = self.conn, self.reader
	}
	f.pipeline = newOutputPipeline(dedup.NewDeduplicator(0, false, 8), nil, nil, nil, nil, nil,
		f.ring, srv, base.peerManager, base.tracker, nil, cfg, base.ctyLookup,
		nil, nil, base.harmonicCfg, nil, base.spotPolicy, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		f.recent, f.custom, f.who, false, f.writer, nil, nil, nil, nil, nil)
	if withTelnet && (!f.pipeline.stabilizerEnabled || f.pipeline.telnetStabilizer == nil || f.pipeline.familySuppressor == nil) {
		t.Fatal("loaded production construction omitted enabled fixture owners")
	}
	if f.pipeline.telnetStabilizer != nil {
		t.Cleanup(f.pipeline.telnetStabilizer.Stop)
	}
	return f
}

func acceptedLearningPathsWho(t *testing.T, f familyDeliveryFixture, call string, want int) {
	t.Helper()
	var expected map[string][]spot.WhoSpotsMeCountryCount
	if want > 0 {
		expected = map[string][]spot.WhoSpotsMeCountryCount{"NA": {{ADIF: 291, Count: want}}}
	}
	if got := f.who.CountryCountsByContinent(call, "20m", time.Now().UTC()); !reflect.DeepEqual(got, expected) {
		t.Fatalf("accepted occurrence oracle for %s=%+v want %+v", call, got, expected)
	}
}

func acceptedLearningPathsSupport(f familyDeliveryFixture, call string) int {
	if f.custom != nil {
		return f.custom.RecentSupportCountWithRequirements(call, "20m", "CW", spot.CustomSCPRequirements{MinScore: 5}, time.Now().UTC())
	}
	return f.recent.RecentSupportCount(call, "20m", "CW", time.Now().UTC())
}

// This control uses the same pipeline/store configuration before negative
// proofs. Valid country+continent, SNR, mode and current acceptance clock are
// observable eligibility, rather than assuming a silently refused recorder.
func acceptedLearningPathsControl(t *testing.T, f familyDeliveryFixture, at time.Time) familyDeliveryInput {
	t.Helper()
	control := familyDeliveryInput{"K1CTL", "K1CTL", "eligible learning control", "V", at}
	f.pipeline.processSpot(control.spot(), nil)
	acceptedLearningPathsWho(t, f, control.call, 1)
	if n := acceptedLearningPathsSupport(f, control.call); n != 1 {
		t.Fatalf("successful store control has %d eligible receivers, want 1", n)
	}
	if f.pipeline.telnet != nil {
		f.assertReceipts(t, []familyDeliveryInput{control}, []familyDeliveryInput{control}, []familyDeliveryInput{control}, control.call)
	} else {
		f.assertPeerReceipts(t, []familyDeliveryInput{control}, control.call)
	}
	return control
}

func acceptedLearningPathsRails(t *testing.T, f familyDeliveryFixture, incoming familyDeliveryInput, oneLane bool) []*dedup.SecondaryDeduper {
	t.Helper()
	lanes := []*dedup.SecondaryDeduper{
		dedup.NewSecondaryDeduper(time.Hour, false), dedup.NewSecondaryDeduper(time.Hour, false), dedup.NewSecondaryDeduper(time.Hour, false),
	}
	for lane, d := range lanes {
		if oneLane && lane == 2 {
			continue // Only SLOW passes; fixture clients explicitly select SLOW.
		}
		if !d.ShouldForward(incoming.spot()) {
			t.Fatal("actual secondary prefill did not establish its positive admission")
		}
	}
	f.pipeline.secondaryFast, f.pipeline.secondaryMed, f.pipeline.secondarySlow = lanes[0], lanes[1], lanes[2]
	f.pipeline.secondaryActive = true
	return lanes
}

func acceptedLearningPathsAssertRails(t *testing.T, lanes []*dedup.SecondaryDeduper, oneLane bool) {
	t.Helper()
	for lane, d := range lanes {
		processed, duplicates, _ := d.GetStats()
		wantProcessed, wantDuplicates := uint64(2), uint64(1)
		if oneLane && lane == 2 {
			wantProcessed, wantDuplicates = 1, 0
		}
		if processed != wantProcessed || duplicates != wantDuplicates {
			t.Fatalf("lane %d did not exercise actual denial/pass: processed=%d duplicates=%d want %d/%d", lane, processed, duplicates, wantProcessed, wantDuplicates)
		}
	}
}

func acceptedLearningPathsPromote(t *testing.T, f familyDeliveryFixture, envelope *telnetStabilizerEnvelope) {
	t.Helper()
	r, driver, _ := correctionLossResolver(t, 4, 8)
	key := envelope.resolverKey
	correctionLossAdmit(t, r, driver, key, "K1ACC", 14030, time.Now().UTC().Add(-time.Second), 2*time.Minute, "N0AAA", "N0BBB", "N0CCC", "N0DDD")
	snapshot, ok := r.Lookup(key)
	if !ok || snapshot.Winner != "K1ACC" || snapshot.State != spot.ResolverStateConfident || snapshot.TotalReporters != 4 {
		t.Fatal("actual delayed promotion did not publish settled four-receiver authority")
	}
	f.pipeline.signalResolver = r
}

func acceptedLearningPathsAssertCustomDisk(t *testing.T, f familyDeliveryFixture, learned []familyDeliveryInput) {
	t.Helper()
	if f.custom == nil {
		return
	}
	if err := f.custom.Close(); err != nil {
		t.Fatal(err)
	}
	// Learned inputs are deliberately separate from emitted/archive rows: an
	// accepted but fully thinned V must still have these literal persisted bytes.
	customSCPFreshnessAssertBytes(t, f.customOpts.Path, familyDeliveryExpectedSCPBytes(learned))
	reopened := customSCPFreshnessOpen(t, f.customOpts)
	for _, in := range learned {
		want := in.glyph == "V"
		if reopened.StaticContains(in.call) != want {
			t.Fatalf("reopened learned membership for %s does not match V-only admission", in.call)
		}
		count := reopened.RecentSupportCountWithRequirements(in.call, "20m", "CW", spot.CustomSCPRequirements{MinScore: 5}, time.Now().UTC())
		if want && count != 1 || !want && count != 0 {
			t.Fatalf("reopened accepted input %s receiver count=%d eligible=%t", in.call, count, want)
		}
	}
}

func TestAcceptedLearningPathsIndependentOfOutputLanes(t *testing.T) {
	source := acceptedLearningPathsConfigSource(t)
	for _, custom := range []bool{false, true} {
		for _, path := range []string{"immediate", "nonstabilized", "overflow", "delayed"} {
			for _, oneLane := range []bool{false, true} {
				t.Run(fmt.Sprintf("custom_%t/%s/one_lane_%t", custom, path, oneLane), func(t *testing.T) {
					temporalDeliveryDirectory(t)
					delay, capacity := 1, 8
					if path == "overflow" {
						delay, capacity = 3600, 1
					}
					f := acceptedLearningPathsFixture(t, source, custom, delay, capacity, path != "nonstabilized", false)
					at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
					control := acceptedLearningPathsControl(t, f, at)
					incoming := familyDeliveryInput{"K1ACC", "K1ACC", "accepted source", "V", at.Add(time.Second)}
					if path == "overflow" || path == "delayed" {
						incoming.receiver, incoming.glyph = "N0AAA", "?"
					}
					if path == "overflow" {
						filler := familyDeliveryInput{"K1HLD", "N0BBB", "held overflow control", "?", at}
						f.pipeline.processSpot(filler.spot(), nil)
						if f.pipeline.telnetStabilizer.Pending() != 1 || f.pipeline.tracker.StabilizerHeld() != 1 {
							t.Fatal("actual filler did not reserve the only pending slot")
						}
						acceptedLearningPathsWho(t, f, filler.call, 0)
					}
					lanes := acceptedLearningPathsRails(t, f, incoming, oneLane)
					f.pipeline.processSpot(incoming.spot(), nil)
					if path == "delayed" {
						if f.pipeline.tracker.StabilizerHeld() != 1 || f.ring.GetCount() != 1 {
							t.Fatal("initial delayed source did not remain owned without fanout")
						}
						acceptedLearningPathsWho(t, f, incoming.call, 0)
						if acceptedLearningPathsSupport(f, incoming.call) != 0 {
							t.Fatal("pending source taught receiver history")
						}
						envelope := familyDeliveryReceiveRelease(t, f.pipeline.telnetStabilizer)
						if envelope.spot.DXCallNorm != incoming.call || envelope.checksCompleted != 0 || !envelope.hasResolverKey || envelope.peerOriginal == nil {
							t.Fatal("actual scheduler lost delayed ownership or original tuple")
						}
						acceptedLearningPathsPromote(t, f, envelope)
						f.pipeline.handleStabilizerRelease(envelope)
						if envelope.spot.Confidence != "V" {
							t.Fatalf("actual accepted release did not promote V: %q", envelope.spot.Confidence)
						}
						incoming.glyph = "V"
					}
					if path == "overflow" && f.pipeline.tracker.StabilizerOverflowRelease() != 1 {
						t.Fatal("source never exercised actual full-capacity fail-open")
					}
					acceptedLearningPathsAssertRails(t, lanes, oneLane)
					acceptedLearningPathsWho(t, f, incoming.call, 1)
					wantSupport := 1
					if custom && path == "overflow" {
						wantSupport = 0 // Natural overflow remains non-V; safeguard wins.
					}
					if got := acceptedLearningPathsSupport(f, incoming.call); got != wantSupport {
						t.Fatalf("accepted source learned %d eligible receivers want %d", got, wantSupport)
					}
					wantRows := 1
					if oneLane {
						wantRows = 2
					}
					if f.ring.GetCount() != wantRows {
						t.Fatalf("actual output lanes retained %d rows want %d", f.ring.GetCount(), wantRows)
					}
					barrier := familyDeliveryInput{"K1BAR", "K1BAR", "learning output barrier", "V", at.Add(2 * time.Second)}
					f.pipeline.processSpot(barrier.spot(), nil)
					if f.pipeline.telnet != nil {
						tcp := []familyDeliveryInput{barrier}
						if oneLane {
							tcp = []familyDeliveryInput{incoming, barrier}
						}
						f.assertReceipts(t, tcp, tcp, []familyDeliveryInput{barrier}, barrier.call)
					} else {
						f.assertPeerReceipts(t, []familyDeliveryInput{barrier}, barrier.call)
					}
					acceptedLearningPathsAssertCustomDisk(t, f, []familyDeliveryInput{control, incoming, barrier})
				})
			}
		}
	}
}

func TestAcceptedLearningPathsDeniedEvidencePromotesLaterS(t *testing.T) {
	source := acceptedLearningPathsConfigSource(t)
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom_%t", custom), func(t *testing.T) {
			temporalDeliveryDirectory(t)
			f := acceptedLearningPathsFixture(t, source, custom, 1, 8, false, true)
			at := time.Now().UTC().Truncate(time.Second).Add(-5 * time.Second)
			acceptedLearningPathsControl(t, f, at)
			input := familyDeliveryInput{"K1ACC", "N0AAA", "denied learning source", "?", at.Add(time.Second)}
			if custom {
				input.receiver, input.glyph = "K1ACC", "V"
			}
			acceptedLearningPathsRails(t, f, input, false)
			receivers := []string{"N0AAA", "N0BBB", "N0CCC"}
			if custom {
				receivers = []string{"K1ACC"}
			}
			for _, receiver := range receivers {
				input.receiver = receiver
				f.pipeline.processSpot(input.spot(), nil)
			}
			acceptedLearningPathsWho(t, f, input.call, len(receivers))
			if f.ring.GetCount() != 1 || acceptedLearningPathsSupport(f, input.call) != len(receivers) {
				t.Fatal("fully denied accepted observations did not establish isolated learned history")
			}
			followup := familyDeliveryInput{"K1ACC", "N0DDD", "later support probe", "S", at.Add(2 * time.Second)}.spot()
			f.pipeline.processSpot(followup, nil)
			if followup.Confidence != "S" {
				t.Fatalf("later accepted observation did not use prior learned support: %q", followup.Confidence)
			}
			acceptedLearningPathsWho(t, f, input.call, len(receivers)+1)
			if custom && acceptedLearningPathsSupport(f, input.call) != 1 {
				t.Fatal("S-floor observation bypassed CustomSCP V-only admission")
			}
		})
	}
}
