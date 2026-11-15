package cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/floodcontrol"
	"dxcluster/peer"
	"dxcluster/spot"
	"dxcluster/telnet"
	"dxcluster/uls"
)

// Construct actual runtime validator/flood/admission wiring; a standalone peer
// manager with a plain channel cannot prove that relay obeys the flood verdict.
func floodPeerConfig(t *testing.T, action string, enabled bool) *config.Config {
	cfg := activityWiringConfig(enabled, action)
	cfg.Dedup.OutputBufferSize = 16
	cfg.Peering = peerRuntimeTestConfig().Peering
	cfg.Peering.MaxPeers, cfg.Peering.ListenPort, cfg.Peering.ForwardSpots = 3, runtimeQualificationPort(t), true
	cfg.Peering.TelnetTransport = config.TelnetTransportNative
	cfg.Peering.Timeouts = config.PeeringTimeouts{LoginSeconds: 5, InitSeconds: 5, IdleSeconds: 30}
	cfg.Peering.Peers = []config.PeeringPeer{
		{Enabled: true, Direction: config.PeeringPeerDirectionInbound, Family: config.PeeringPeerFamilyDXSpider, RemoteCallsign: "N1REM"},
		{Enabled: true, Direction: config.PeeringPeerDirectionInbound, Family: config.PeeringPeerFamilyDXSpider, RemoteCallsign: "N2DST", PreferPC9x: true},
		{Enabled: true, Direction: config.PeeringPeerDirectionInbound, Family: config.PeeringPeerFamilyDXSpider, RemoteCallsign: "N3OLD"},
	}
	cfg.FloodControl.Rails.DECall.ThresholdsPerSourceType = map[string]int{"PEER": 1, "UPSTREAM": 1, "MANUAL": 1}
	cfg.CTY.Enabled = true
	return cfg
}
func floodPeerRuntime(t *testing.T, action string, enabled bool) (*clusterRuntime, *peerAdmissionSocket, *peerAdmissionSocket, *peerAdmissionSocket) {
	return floodPeerRuntimeForward(t, action, enabled, true)
}
func floodPeerRuntimeForward(t *testing.T, action string, enabled, forward bool) (*clusterRuntime, *peerAdmissionSocket, *peerAdmissionSocket, *peerAdmissionSocket) {
	t.Helper()
	cfg := floodPeerConfig(t, action, enabled)
	cfg.Peering.ForwardSpots = forward
	return floodPeerRuntimeConfig(t, cfg)
}
func floodPeerRuntimeConfig(t *testing.T, cfg *config.Config) (*clusterRuntime, *peerAdmissionSocket, *peerAdmissionSocket, *peerAdmissionSocket) {
	t.Helper()
	r := newClusterRuntime(peerRuntimeTestBuild(), cfg, "", config.LoadDiagnostics{})
	r.ctyDB.Store(loadIngestCTY(t))
	if !r.initializePipelineCore() {
		t.Fatal("pipeline initialization failed")
	}
	return startFloodPeerSockets(t, r)
}

func startFloodPeerSockets(t *testing.T, r *clusterRuntime) (*clusterRuntime, *peerAdmissionSocket, *peerAdmissionSocket, *peerAdmissionSocket) {
	t.Helper()
	cfg := r.cfg
	if !r.initializePeerManager() {
		t.Fatal(r.startupErr)
	}
	if r.peerIngress == nil || r.ingestValidator.peerIngress != r.peerIngress || r.ingestValidator.peerAdmit == nil {
		t.Fatal("runtime peer policy not attached")
	}
	r.peerManager.SetMembershipProvider(func() peer.LocalMembership { return peer.LocalMembership{Complete: true} })
	t.Cleanup(func() {
		r.peerManager.Stop()
		r.stopIngestPipeline()
		r.retireOutputPipeline()
		if r.freqAverager != nil {
			r.freqAverager.StopCleanup()
		}
		if r.cancel != nil {
			r.cancel()
		}
	})
	if err := r.peerManager.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	address := fmt.Sprintf("127.0.0.1:%d", cfg.Peering.ListenPort)
	source := peerAdmissionLogin(t, address, "N1REM", false)
	modern := peerAdmissionLogin(t, address, "N2DST", true)
	legacy := peerAdmissionLogin(t, address, "N3OLD", false)
	for _, s := range []*peerAdmissionSocket{source, modern, legacy} {
		peerAdmissionSendAndAck(t, s, "")
	}
	return r, source, modern, legacy
}

// License denial uses the real validator/peer/controller consumers with its
// existing lookup seam fixed before Start. The factory path is proved above.
func consumerPeerRuntime(t *testing.T, rejectLicense, refusePrimary bool) (*clusterRuntime, *peerAdmissionSocket, *peerAdmissionSocket, *peerAdmissionSocket) {
	t.Helper()
	cfg := floodPeerConfig(t, config.FloodActionSuppress, true)
	r := newClusterRuntime(peerRuntimeTestBuild(), cfg, "", config.LoadDiagnostics{})
	r.ctyDB.Store(loadIngestCTY(t))
	r.peerIngress = peer.NewSpotIngress()
	r.deduplicator = dedup.NewDeduplicator(time.Minute, false, 16)
	r.deduplicator.Start()
	r.floodController = floodcontrol.New(cfg.FloodControl, r.deduplicator.GetInputChannel(), nil, nil)
	r.ingestValidator = newIngestValidator(r.ctyLookup, nil, nil, nil, r.floodController.Input(), nil, nil, true)
	r.ingestValidator.peerIngress = r.peerIngress
	r.ingestValidator.peerAdmit = func(s *spot.Spot) bool { return r.floodController.Admit(time.Now(), s) }
	r.ingestValidator.licenseChecksEnabled = func() bool { return rejectLicense }
	r.ingestValidator.lookupUS = func(call string) uls.LookupResult { return uls.LookupResult{Available: true, Found: call != "K1AAA"} }
	if refusePrimary {
		blocked := make(chan *spot.Spot, 1)
		blocked <- spot.NewSpot("K1BUSY", "DL1BUS", 14020, "CW")
		r.floodController.SetAdmissionSink(func(s *spot.Spot) bool {
			select {
			case blocked <- s:
				return true
			default:
				return false
			}
		})
	}
	r.ingestValidator.Start()
	r.ingestInput = r.ingestValidator.Input()
	return startFloodPeerSockets(t, r)
}

func TestPeerFloodSingleVerdictActualRecipients(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, action := range []string{config.FloodActionSuppress, config.FloodActionDrop, config.FloodActionObserve} {
			t.Run(kind+"/"+action, func(t *testing.T) {
				temporalDeliveryDirectory(t)
				r, source, modern, legacy := floodPeerRuntime(t, action, true)
				at := time.Now().UTC().Truncate(time.Minute)
				var input [][]string
				for _, dx := range []string{"K1ONE", "K1TWO", "K1DONE", "K1BACK"} {
					fields := peerAdmissionFields(kind, dx, at)
					fields[5] = "DL1AAA"
					input = append(input, fields)
				}
				input[2] = peerAdmissionFields("PC11", "K1DONE", at)
				input[2][5] = "DL1BAR"
				input[3] = peerAdmissionFields("PC11", "K1BACK", at)
				input[3][5] = "DL1SRC"
				// The first two distinct DX reports isolate flood policy from either
				// deduper. The accepted third report is an ordered output sentinel.
				peerAdmissionWrite(t, source, peerAdmissionSentence(kind, input[0], 5))
				peerAdmissionWrite(t, source, peerAdmissionSentence(kind, input[1], 5))
				peerAdmissionWrite(t, source, peerAdmissionSentence("PC11", input[2], 5))
				wantIndices := []int{0, 2}
				if action == config.FloodActionObserve {
					wantIndices = []int{0, 1, 2}
				}
				for _, i := range wantIndices {
					k := kind
					if i == 2 {
						k = "PC11"
					}
					if got, want := peerAdmissionReadSpot(t, modern), peerAdmissionSentence(k, input[i], 4); got != want {
						t.Fatalf("modern receipt %q want %q", got, want)
					}
					if kind != "PC26" || i == 2 {
						fields := input[i]
						if k == "PC61" {
							fields = fields[:7]
							k = "PC11"
						}
						if got, want := peerAdmissionReadSpot(t, legacy), peerAdmissionSentence(k, fields, 4); got != want {
							t.Fatalf("legacy receipt %q want %q", got, want)
						}
					}
				}
				// A second source's data sentinel proves source exclusion through
				// the data writer FIFO, rather than a priority PC51 response.
				peerAdmissionWrite(t, legacy, peerAdmissionSentence("PC11", input[3], 5))
				for _, dst := range []*peerAdmissionSocket{source, modern} {
					if got, want := peerAdmissionReadSpot(t, dst), peerAdmissionSentence("PC11", input[3], 4); got != want {
						t.Fatalf("source marker receipt %q want %q", got, want)
					}
				}
				wantIndices = append(wantIndices, 3)
				port := runtimeQualificationPort(t)
				srv := telnet.NewServer(telnet.ServerOptions{Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n", MaxConnections: 2, BroadcastWorkers: 1, BroadcastQueue: 8, WorkerQueue: 8, ClientBuffer: 8, BroadcastBatchIntervalSet: true}, nil)
				if err := srv.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(srv.Stop)
				familyAdmissionJoinClients(t, srv)
				conn, reader := correctionEvidenceLogin(t, port, "DL1ABC", "OFF")
				p := newDeliveryTestPipeline(buffer.NewRingBuffer(8), nil, srv)
				p.peerManager = r.peerManager
				for _, i := range wantIndices {
					var s *spot.Spot
					select {
					case s = <-r.deduplicator.GetOutputChannel():
					case <-time.After(3 * time.Second):
						t.Fatal("actual primary output missing")
					}
					if s.DXCall != input[i][1] {
						t.Fatalf("local policy output %s want %s", s.DXCall, input[i][1])
					}
					p.deliverSpot(&outputSpotContext{spot: s})
					if got := correctionEvidenceReadDX(t, conn, reader); !strings.Contains(got, input[i][1]) {
						t.Fatalf("local TCP receipt %q", got)
					}
				}
				processed, observed, suppressed, dropped, _, _ := r.floodController.GetStats()
				if processed != 4 || observed+suppressed+dropped != 1 {
					t.Fatalf("one charge invariant processed=%d decisions=%d/%d/%d", processed, observed, suppressed, dropped)
				}
				if action == config.FloodActionSuppress && suppressed != 1 || action == config.FloodActionDrop && dropped != 1 || action == config.FloodActionObserve && observed != 1 {
					t.Fatal("wrong verdict")
				}
			})
		}
	}
}

func TestPeerFloodValidationRejectTransitAndCancellation(t *testing.T) {
	for _, reject := range []string{"cty", "license"} {
		t.Run(reject, func(t *testing.T) {
			var r *clusterRuntime
			var source, modern *peerAdmissionSocket
			if reject == "license" {
				r, source, modern, _ = consumerPeerRuntime(t, true, false)
			} else {
				r, source, modern, _ = floodPeerRuntime(t, config.FloodActionSuppress, true)
			}
			at := time.Now().UTC().Truncate(time.Minute)
			fields := peerAdmissionFields("PC11", "K1REJ", at)
			fields[5] = "K1AAA"
			if reject == "cty" {
				fields[1] = "ZZ0MISS"
			}
			peerAdmissionWrite(t, source, peerAdmissionSentence("PC11", fields, 5))
			if got, want := peerAdmissionReadSpot(t, modern), peerAdmissionSentence("PC11", fields, 4); got != want {
				t.Fatalf("existing rejected transit %q want %q", got, want)
			}
			marker := peerAdmissionFields("PC11", "K1DONE", at)
			marker[5] = "DL1BAR"
			peerAdmissionWrite(t, source, peerAdmissionSentence("PC11", marker, 5))
			peerAdmissionReadSpot(t, modern)
			select {
			case s := <-r.deduplicator.GetOutputChannel():
				if s.DXCall != "K1DONE" {
					t.Fatal("validation reject reached primary")
				}
			case <-time.After(time.Second):
				t.Fatal("marker missing")
			}
			processed, _, _, _, _, _ := r.floodController.GetStats()
			if processed != 1 {
				t.Fatalf("validation reject charged: %d", processed)
			}
		})
	}
}

func TestPeerIngressCancelableValidatorRetirement(t *testing.T) {
	for _, generic := range []bool{false, true} {
		t.Run(fmt.Sprintf("generic=%t", generic), func(t *testing.T) {
			input := make(chan *spot.Spot, 4)
			entered := make(chan struct{}, 1)
			v := newIngestValidator(func() *cty.CTYDatabase {
				select {
				case entered <- struct{}{}:
				default:
				}
				return nil
			}, nil, nil, nil, input, nil, nil, true)
			v.peerIngress = peer.NewSpotIngress()
			v.peerAdmit = func(*spot.Spot) bool { t.Error("canceled validation admitted"); return false }
			v.Start()
			if generic {
				v.input <- spot.NewSpot("K1ABC", "W1ABC", 14020, "CW")
				<-entered
			}
			closed := make(chan struct{})
			go func() { v.peerIngress.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("peer close waited on generic CTY")
			}
			v.Stop()
			select {
			case <-v.done:
			default:
				t.Fatal("validator not joined")
			}
			v.Start() // A retired owner must not restart.
			if !v.started {
				t.Fatal("fixture never started owner")
			}
		})
	}
}

func TestPeerFloodActiveMissingCTYCancelsWithoutTransit(t *testing.T) {
	r, source, modern, _ := floodPeerRuntime(t, config.FloodActionSuppress, true)
	r.ctyDB.Store(nil) // Atomic production publication, not a late callback write.
	fields := peerAdmissionFields("PC11", "K1WAIT", time.Now().UTC().Truncate(time.Minute))
	fields[5] = "DL1AAA"
	peerAdmissionSendAndAck(t, source, peerAdmissionSentence("PC11", fields, 5))
	activityWiringWait(t, func() bool { return r.ingestValidator.IngestCount() == 1 })
	r.peerManager.Stop()
	if count, _ := r.peerIngress.Usage(); count != 0 {
		t.Fatal("active CTY item not retired")
	}
	processed, _, _, _, _, _ := r.floodController.GetStats()
	if processed != 0 {
		t.Fatal("canceled validation charged flood")
	}
	select {
	case s := <-r.deduplicator.GetOutputChannel():
		t.Fatalf("canceled spot locally admitted: %+v", s)
	default:
	}
	if err := modern.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		line, err := modern.reader.ReadString('\n')
		if strings.HasPrefix(line, "PC11^") {
			t.Fatal("cancellation became validation-denial transit")
		}
		if err != nil {
			return
		}
	}
	t.Fatal("retired peer transport did not terminate")
}

func TestPeerFloodPrimaryPressureStopsRelay(t *testing.T) {
	r, source, modern, _ := consumerPeerRuntime(t, false, true)
	fields := peerAdmissionFields("PC11", "K1FULL", time.Now().UTC().Truncate(time.Minute))
	fields[5] = "DL1AAA"
	peerAdmissionSendAndAck(t, source, peerAdmissionSentence("PC11", fields, 5))
	activityWiringWait(t, func() bool { n, _ := r.peerIngress.Usage(); return n == 0 })
	// A CTY-rejected original retains transit without needing the full primary
	// queue. Its data receipt is a FIFO marker for absence of the pressure drop.
	marker := peerAdmissionFields("PC11", "ZZ0DONE", time.Now().UTC().Truncate(time.Minute))
	marker[5] = "DL1BAR"
	peerAdmissionWrite(t, source, peerAdmissionSentence("PC11", marker, 5))
	if got, want := peerAdmissionReadSpot(t, modern), peerAdmissionSentence("PC11", marker, 4); got != want {
		t.Fatalf("pressure-refused relay preceded marker: %q", got)
	}
	processed, _, suppressed, dropped, _, _ := r.floodController.GetStats()
	if processed != 1 || suppressed+dropped != 0 {
		t.Fatal("pressure refusal changed policy charging")
	}
}

func TestIngestValidatorStopBeforeStart(t *testing.T) {
	v := newIngestValidator(nil, nil, nil, nil, make(chan *spot.Spot, 1), nil, nil, false)
	v.Stop()
	v.Start()
	if v.started {
		t.Fatal("retired validator started a later owner")
	}
}

func TestPeerFloodDisabledReceiveOnlyAndLocalHops(t *testing.T) {
	for _, tc := range []struct {
		name             string
		enabled, forward bool
		hop              int
	}{
		{"disabled", false, true, 5}, {"receive_only", true, false, 5}, {"hop_zero", true, true, 0}, {"hop_one", true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, source, modern, legacy := floodPeerRuntimeForward(t, config.FloodActionSuppress, tc.enabled, tc.forward)
			fields := peerAdmissionFields("PC11", "K1GATE", time.Now().UTC().Truncate(time.Minute))
			fields[5] = "DL1AAA"
			peerAdmissionSendAndAck(t, source, peerAdmissionSentence("PC11", fields, tc.hop))
			select {
			case s := <-r.deduplicator.GetOutputChannel():
				if s.DXCall != "K1GATE" {
					t.Fatal("local eligibility changed")
				}
			case <-time.After(time.Second):
				t.Fatal("local-only spot missing")
			}
			activityWiringWait(t, func() bool { n, _ := r.peerIngress.Usage(); return n == 0 })
			if tc.forward && tc.hop > 1 {
				for _, dst := range []*peerAdmissionSocket{modern, legacy} {
					if got := peerAdmissionReadSpot(t, dst); !strings.Contains(got, "^K1GATE^") {
						t.Fatal("disabled policy blocked relay")
					}
				}
			}
			// Existing local DX publication is allowed even in receive-only mode,
			// supplying a data-FIFO absence witness on all three peer connections.
			marker := spot.NewSpot("K1LOCAL", "DL1BAR", 14020, "CW")
			marker.Comment = "CQ local marker"
			marker.SpotterIP = "192.0.2.1"
			if !r.peerManager.PublishDX(marker) {
				t.Fatal("local marker publication refused")
			}
			for _, dst := range []*peerAdmissionSocket{source, modern, legacy} {
				if got := peerAdmissionReadSpot(t, dst); !strings.Contains(got, "^K1LOCAL^") {
					t.Fatalf("ineligible relay preceded data marker: %q", got)
				}
			}
		})
	}
}

func TestPeerIngressPressureKeepsControlAndRetiresBeforePrimary(t *testing.T) {
	r, source, modern, _ := floodPeerRuntime(t, config.FloodActionSuppress, true)
	r.ctyDB.Store(nil)
	for i := 0; i < 70; i++ {
		fields := peerAdmissionFields("PC11", fmt.Sprintf("K1P%03d", i), time.Now().UTC().Truncate(time.Minute))
		fields[5] = "DL1AAA"
		peerAdmissionWrite(t, source, peerAdmissionSentence("PC11", fields, 5))
	}
	peerAdmissionSendAndAck(t, source, "") // Actual reader/control progress under pressure.
	if n, b := r.peerIngress.Usage(); n != 64 || b > 8<<20 || r.peerIngress.Refusals() == 0 {
		t.Fatalf("pressure fixture missed bound: %d/%d", n, b)
	}
	before := r.peerManager.ProtocolStats().Freshness
	peerAdmissionWrite(t, modern, fmt.Sprintf("PC92^N2DST^%d^C^5N2DST^5N4NEW^H2^", time.Now().UTC().Unix()%86400))
	peerAdmissionSendAndAck(t, modern, "")
	activityWiringWait(t, func() bool { return r.peerManager.ProtocolStats().Freshness > before })
	r.retireOutputPipeline()
	select {
	case <-r.peerIngress.Done():
	default:
		t.Fatal("primary retired before peer admission")
	}
	if n, _ := r.peerIngress.Usage(); n != 0 {
		t.Fatal("retirement left peer work live")
	}
	processed, _, _, _, _, _ := r.floodController.GetStats()
	if processed != 0 {
		t.Fatal("CTY-blocked pressure charged policy")
	}
	// Peer control remains available for the later Manager.Stop withdrawal.
	peerAdmissionSendAndAck(t, source, "")
}

func TestPeerFloodDuplicateChargingUnchanged(t *testing.T) {
	cfg := floodPeerConfig(t, config.FloodActionSuppress, true)
	cfg.FloodControl.Rails.DECall.ThresholdsPerSourceType["PEER"] = 2
	r, source, modern, _ := floodPeerRuntimeConfig(t, cfg)
	at := time.Now().UTC().Truncate(time.Minute)
	first := peerAdmissionFields("PC11", "K1DUP", at)
	first[5] = "DL1AAA"
	third := peerAdmissionFields("PC11", "K1EXCESS", at)
	third[5] = "DL1AAA"
	marker := peerAdmissionFields("PC11", "K1DONE", at)
	marker[5] = "DL1BAR"
	for _, f := range [][]string{first, first, third, marker} {
		peerAdmissionWrite(t, source, peerAdmissionSentence("PC11", f, 5))
	}
	for _, f := range [][]string{first, marker} {
		if got, want := peerAdmissionReadSpot(t, modern), peerAdmissionSentence("PC11", f, 4); got != want {
			t.Fatalf("duplicate allowance/transit changed %q want %q", got, want)
		}
	}
	for _, dx := range []string{"K1DUP", "K1DONE"} {
		select {
		case s := <-r.deduplicator.GetOutputChannel():
			if s.DXCall != dx {
				t.Fatalf("local duplicate output %s want %s", s.DXCall, dx)
			}
		case <-time.After(time.Second):
			t.Fatal("local duplicate sentinel missing")
		}
	}
	processed, _, suppressed, _, _, _ := r.floodController.GetStats()
	primary, duplicates, _ := r.deduplicator.GetStats()
	if processed != 4 || suppressed != 1 || primary != 3 || duplicates != 1 || r.ingestValidator.IngestCount() != 4 {
		t.Fatalf("raw duplicate charging flood=%d/%d primary=%d/%d ingest=%d", processed, suppressed, primary, duplicates, r.ingestValidator.IngestCount())
	}
}
