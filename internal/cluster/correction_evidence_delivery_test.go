//go:build !qualification

// File role: Connects retained resolver evidence to actual TCP/peer output,
// sealed buffer storage, and reopened archive rows. Literal receiver sets and
// wire fields make bucket repetition and discarded dissent observable.
package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/peer"
	"dxcluster/spot"
)

type correctionEvidenceDeliveryCase struct {
	name, subject, dissent, wantCall, wantGlyph string
	third                                       string
	weights                                     spot.SpotterReliability
	aReceivers                                  []string
	minReports, wantPercent, wantWinnerWeight   int
	wantTotalWeight                             int
	wantCorrections                             uint64
}

func TestCallCorrectionMethodDeliveredAccounting(t *testing.T) {
	temporalDeliveryDirectory(t)
	weights := spot.SpotterReliability{"N0AAA": 0.900, "N0BBB": 0.950, "N0CCC": 1.000}
	pair := []string{"N0AAA", "N0BBB"}
	cases := []correctionEvidenceDeliveryCase{
		{
			name: "duplicate_receivers_do_not_authorize_correction", subject: "K1ABD", dissent: "K1ABD",
			aReceivers: pair, minReports: 3, wantCall: "K1ABD", wantGlyph: "P", wantPercent: 33,
			wantWinnerWeight: 2000, wantTotalWeight: 3000,
		},
		{
			name: "retained_call_uses_its_unequal_weight_share", subject: "K1ABD", dissent: "K1ABD", weights: weights,
			aReceivers: pair, minReports: 3, wantCall: "K1ABD", wantGlyph: "P", wantPercent: 35,
			wantWinnerWeight: 1850, wantTotalWeight: 2850,
		},
		{
			name: "losing_candidate_remains_in_denominator", subject: "K1ABC", dissent: "K1ABD",
			aReceivers: pair, minReports: 1, wantCall: "K1ABC", wantGlyph: "V", wantPercent: 66,
			wantWinnerWeight: 2000, wantTotalWeight: 3000,
		},
		{
			name: "ineligible_replacement_dissent_keeps_its_weight", subject: "K1ABC", dissent: "DL9ZZZ", weights: weights,
			aReceivers: pair, minReports: 1, wantCall: "K1ABC", wantGlyph: "V", wantPercent: 64,
			wantWinnerWeight: 1850, wantTotalWeight: 2850,
		},
		{
			name: "third_candidate_keeps_weight", subject: "K1ABC", dissent: "K1ABD", third: "DL9ZZZ", weights: weights,
			aReceivers: pair, minReports: 1, wantCall: "K1ABC", wantGlyph: "P", wantPercent: 48,
			wantWinnerWeight: 1850, wantTotalWeight: 3850,
		},
		{
			name: "genuine_third_receiver_authorizes_correction", subject: "K1ABD", dissent: "K1ABD",
			aReceivers: []string{"N0AAA", "N0BBB", "N0DDD"}, minReports: 3,
			wantCall: "K1ABC", wantGlyph: "C", wantPercent: 75,
			wantWinnerWeight: 3000, wantTotalWeight: 4000, wantCorrections: 1,
		},
	}
	for _, tc := range cases {
		bucketsToCheck := []int{1}
		if tc.name == "duplicate_receivers_do_not_authorize_correction" || tc.name == "ineligible_replacement_dissent_keeps_its_weight" {
			bucketsToCheck = append(bucketsToCheck, 2)
		}
		for _, buckets := range bucketsToCheck {
			t.Run(fmt.Sprintf("%s/buckets=%d", tc.name, buckets), func(t *testing.T) {
				checkCorrectionEvidenceDelivery(t, tc, buckets)
			})
		}
	}
}

func TestRuntimeTemporalPendingCompaction(t *testing.T) {
	at := time.Now().UTC()
	resolver, key := correctionEvidenceResolver(t, correctionEvidenceDeliveryCase{
		aReceivers: []string{"N0AAA", "N0BBB"}, dissent: "K1ABD",
	}, 1, at)
	snapshot, _ := resolver.Lookup(key)
	if snapshot.ReceiverEvidence().Len() != 3 {
		t.Fatal("missing receiver payload")
	}
	controller := newRuntimeTemporalController(runtimeTemporalTestConfig(0))
	ok, id, reason := controller.Observe(at, key, "K1ABD", runtimeTemporalPending{
		selection: correctionflow.ResolverPrimarySelection{Snapshot: snapshot, SnapshotOK: true},
	})
	if !ok {
		t.Fatalf("Observe rejected: %s", reason)
	}
	saved := controller.pending[id].selection.Snapshot
	if saved.ReceiverEvidence() != nil || len(saved.CandidateRanks) != 2 || saved.TotalReporters != 3 {
		t.Fatal("runtime pending did not compact evidence")
	}
}

func checkCorrectionEvidenceDelivery(t *testing.T, tc correctionEvidenceDeliveryCase, buckets int) {
	t.Helper()
	cfg := config.CallCorrectionConfig{
		Enabled: true, RecencySeconds: 120, MaxEditDistance: 6, MinConsensusReports: tc.minReports,
		MinAdvantage: 0, MinConfidencePercent: 0, FrequencyToleranceHz: 500,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot", InvalidAction: "broadcast",
		FreqGuardRunnerUpRatio: 0.6, ResolverNeighborhoodEnabled: true,
		ResolverNeighborhoodBucketRadius: 1, ResolverNeighborhoodMaxDistance: 1,
	}
	if tc.dissent == "DL9ZZZ" && spot.CallDistance("K1ABC", tc.dissent, "CW", "morse", "baudot") <= cfg.ResolverNeighborhoodMaxDistance {
		t.Fatal("unrelated dissent fixture unexpectedly qualifies as a replacement")
	}
	at := time.Now().UTC().Truncate(time.Second)
	resolver, key := correctionEvidenceResolver(t, tc, buckets, at)
	selection := correctionflow.SelectResolverPrimarySnapshotForCall(resolver, key, cfg, tc.subject)
	snapshot := selection.Snapshot
	wantTotal := len(tc.aReceivers) + 1
	if tc.third != "" {
		wantTotal++
	}
	if !selection.SnapshotOK || snapshot.Winner != "K1ABC" || snapshot.WinnerSupport != len(tc.aReceivers) ||
		snapshot.TotalReporters != wantTotal || snapshot.WinnerWeightedSupportMilli != tc.wantWinnerWeight ||
		snapshot.TotalWeightedSupportMilli != tc.wantTotalWeight {
		t.Errorf("published evidence accounting: available=%t winner=%q support=%d total=%d weighted=%d/%d; want A support=%d total=%d weighted=%d/%d",
			selection.SnapshotOK, snapshot.Winner, snapshot.WinnerSupport, snapshot.TotalReporters,
			snapshot.WinnerWeightedSupportMilli, snapshot.TotalWeightedSupportMilli,
			len(tc.aReceivers), wantTotal, tc.wantWinnerWeight, tc.wantTotalWeight)
	}
	f := newAmbiguityDeliveryFixture(t, resolver, cfg)
	pipeline := f.pipeline
	pipeline.spotterReliability = tc.weights
	s := newDeliveryTestSpot(tc.subject, "N0AAA", at)
	s.SourceType = spot.SourceManual // Non-self local input is eligible for peer publication.
	// The driver owns publication and is not concurrent with the output stage.
	// Its live enqueue uses an already-seeded receiver; this test deliberately
	// evaluates the latest published evidence without inventing an ingest barrier.
	pipeline.processSpotBody(s, nil)
	if tc.subject != "K1ABC" && tc.wantCorrections == 0 && pipeline.tracker.CorrectionDecisionReasons()["resolver_gate_min_reports"] != 1 {
		t.Errorf("duplicate population must reject specifically at min_reports: %v", pipeline.tracker.CorrectionDecisionReasons())
	}
	f.check(t, tc, at)
}

func correctionEvidenceResolver(t *testing.T, tc correctionEvidenceDeliveryCase, buckets int, at time.Time) (*spot.SignalResolver, spot.ResolverSignalKey) {
	t.Helper()
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Millisecond, SweepInterval: time.Millisecond,
		HysteresisWindows: 1, FreqGuardRunnerUpRatio: 0.6, MaxEditDistance: 3,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot", SpotterReliability: tc.weights,
	})
	driver, err := spot.NewSignalResolverDriver(resolver)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	for i := 0; i < buckets; i++ {
		frequency := 14020 + float64(i)*0.5
		neighborKey := spot.NewResolverSignalKey(frequency, "20m", "CW", 500)
		receivers := append(append([]string(nil), tc.aReceivers...), "N0CCC")
		if tc.third != "" {
			receivers = append(receivers, "N0EEE")
		}
		for _, receiver := range receivers {
			call := "K1ABC"
			if receiver == "N0CCC" {
				call = tc.dissent
			}
			if receiver == "N0EEE" {
				call = tc.third
			}
			if !resolver.Enqueue(spot.ResolverEvidence{
				ObservedAt: at, Key: neighborKey, DXCall: call, Spotter: receiver,
				FrequencyKHz: frequency, RecencyWindow: 2 * time.Minute,
			}) {
				t.Fatal("receiver evidence was not admitted")
			}
		}
	}
	wantPerBucket := len(tc.aReceivers) + 1
	if tc.third != "" {
		wantPerBucket++
	}
	if got, want := driver.Drain(), buckets*wantPerBucket; got != want {
		t.Fatalf("processed receiver evidence=%d want %d", got, want)
	}
	// The first report schedules the next evaluation; advance beyond that
	// interval so every seeded membership is in the immutable publication.
	driver.Sweep(at.Add(2 * time.Millisecond))
	return resolver, key
}

func correctionEvidenceLogin(t *testing.T, port int, call, diag string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, reader := runtimeQualificationLogin(t, fmt.Sprintf("127.0.0.1:%d", port), "", call)
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		line := customSCPDeliveryReadLine(t, reader)
		if strings.Contains(line, "ACCOUNTING_READY") {
			if _, err := io.WriteString(conn, "SET DIAG "+diag+"\r\n"); err != nil {
				t.Fatal(err)
			}
			for j := 0; j < 16; j++ {
				if strings.Contains(customSCPDeliveryReadLine(t, reader), "Diagnostic comments: "+diag) {
					return conn, reader
				}
			}
			t.Fatal("diagnostic command acknowledgement exceeded fixture bound")
		}
	}
	t.Fatal("login greeting exceeded fixture bound")
	return nil, nil
}

func correctionEvidencePeer(t *testing.T) (*peer.Manager, *peerAdmissionSocket) {
	t.Helper()
	cfg := peerRuntimeTestConfig().Peering
	cfg.MaxPeers, cfg.ListenPort, cfg.ForwardSpots = 1, runtimeQualificationPort(t), true
	cfg.TelnetTransport = config.TelnetTransportNative
	cfg.Timeouts = config.PeeringTimeouts{LoginSeconds: 5, InitSeconds: 5, IdleSeconds: 30}
	cfg.Peers = []config.PeeringPeer{{
		Enabled: true, Direction: config.PeeringPeerDirectionInbound,
		Family: config.PeeringPeerFamilyDXSpider, RemoteCallsign: "N2DST", PreferPC9x: true,
	}}
	manager, err := peer.NewManager(cfg, cfg.LocalCallsign, make(chan *spot.Spot, 1), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	if err := manager.SetBuildIdentity("test", "", "", "", ""); err != nil {
		t.Fatal(err)
	}
	manager.SetMembershipProvider(func() peer.LocalMembership { return peer.LocalMembership{Complete: true} })
	if err := manager.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	recipient := peerAdmissionLogin(t, fmt.Sprintf("127.0.0.1:%d", cfg.ListenPort), "N2DST", true)
	peerAdmissionSendAndAck(t, recipient, "") // Registration completed before publication.
	return manager, recipient
}

func correctionEvidenceReadDX(t *testing.T, conn net.Conn, reader *bufio.Reader) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		line := customSCPDeliveryReadLine(t, reader)
		if start := strings.Index(line, "DX de "); start >= 0 {
			return line[start:] // A login/command prompt can precede the DX prefix.
		}
	}
	t.Fatal("positive DX receipt exceeded fixture bound")
	return ""
}

func correctionEvidenceAssertOriginalPeer(t *testing.T, recipient *peerAdmissionSocket, originalCall string, at time.Time) {
	t.Helper()
	frame, err := peer.ParseFrame(peerAdmissionReadSpot(t, recipient))
	want := []string{"14020", originalCall, at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0AAA", "N0CALL-1", "192.0.2.1"}
	if err != nil || frame.Type != "PC61" || !reflect.DeepEqual(frame.Fields, want) {
		t.Errorf("actual peer fields must match independent original input: frame=%+v err=%v want=%q", frame, err, want)
	}
}

func correctionEvidenceAssertSpot(t *testing.T, destination string, s *spot.Spot, tc correctionEvidenceDeliveryCase, at time.Time, percentage bool) {
	t.Helper()
	if s == nil || s.DXCallNorm != tc.wantCall || s.DECallNorm != "N0AAA" || s.Frequency != 14020 ||
		s.ModeNorm != "CW" || s.Confidence != tc.wantGlyph || !s.Time.Equal(at) {
		t.Errorf("%s delivered fields=%+v want call=%s DE=N0AAA frequency=14020 mode=CW glyph=%s time=%s",
			destination, s, tc.wantCall, tc.wantGlyph, at.Format(time.RFC3339))
	}
	if percentage && s != nil && (!s.ConfidencePercentOK || s.ConfidencePercent != tc.wantPercent) {
		t.Errorf("%s percentage=%d available=%t want %d%%", destination, s.ConfidencePercent, s.ConfidencePercentOK, tc.wantPercent)
	}
}

// Build the wire oracle from documented 78-column positions, without calling
// the production formatter or deriving expectations from the emitted object.
func correctionEvidenceWire(call, glyph, comment string, at time.Time) string {
	line := []byte(strings.Repeat(" ", 78))
	copy(line, "DX de N0AAA:     14020.00")
	copy(line[27:], call)
	copy(line[39:], comment)
	line[71] = glyph[0]
	copy(line[73:], at.Format("1504Z"))
	return string(line) + "\r\n"
}
