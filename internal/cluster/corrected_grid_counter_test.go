//go:build !qualification

// File role: Counts corrected missing-grid output at common admission, using
// actual immediate/held corrections, secondary thinning and bounded queues.
package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

func TestCorrectedNoGridOutputCounter(t *testing.T) {
	requirePathReportH3Mappings(t)
	for _, delayed := range []bool{false, true} {
		for _, disposition := range []string{"missing", "reported", "queue_refused", "all_lanes_thinned", "age_after_acceptance", "age_before_acceptance"} {
			t.Run(fmt.Sprintf("delayed_%t/%s", delayed, disposition), func(t *testing.T) {
				var p *outputPipeline
				var envelope *telnetStabilizerEnvelope
				at := time.Now().UTC().Truncate(time.Second)
				if delayed {
					p, envelope = correctedGridHeldInput(t, func(p *outputPipeline, _ *spot.Spot) { p.tracker = stats.NewTracker() })
					at = envelope.spot.Time
					if p.tracker.CorrectedOutputWithoutGrid() != 0 || p.tracker.StabilizerHeld() != 1 || p.buf.GetCount() != 0 {
						t.Fatal("pending real hold counted as missing-grid output")
					}
				} else {
					r, d, key := correctionLossResolver(t, 4, 8)
					correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at.Add(-time.Second), time.Minute, "N0BBB", "N0CCC", "N0FFF")
					p = newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil))
					db := correctedGridCTY(t)
					p.ctyLookup = func() *cty.CTYDatabase { return db }
					p.signalResolver, p.correctionCfg, p.tracker = r, correctionLossConfig(), stats.NewTracker()
				}
				p.archiveWriter = newDeliveryTestArchiveWriter(t)
				p.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
				p.gridLookup = func(string) (string, bool, bool) {
					if disposition == "reported" {
						return "QF56", false, true
					}
					return "", false, false
				}
				p.spotPolicy.MaxAgeSeconds = 300
				now := at.Add(299 * time.Second)
				p.outputAgeNow = func() time.Time { return now }
				switch disposition {
				case "age_after_acceptance":
					p.gridUpdate = func(string, string) { now = at.Add(301 * time.Second) }
				case "age_before_acceptance":
					now = at.Add(301 * time.Second)
				case "queue_refused":
					for range 4 {
						if !p.telnet.TryBroadcastSpotOwned(newDeliveryTestSpot("K1FILL", "N0AAA", at).SealForAsync(), true, true, true) {
							t.Fatal("real bounded broadcast prefill failed")
						}
					}
				case "all_lanes_thinned":
					p.secondaryActive = true
					p.secondaryFast = dedup.NewSecondaryDeduper(time.Minute, false)
					p.secondaryMed = dedup.NewSecondaryDeduper(time.Minute, false)
					p.secondarySlow = dedup.NewSecondaryDeduper(time.Minute, false)
					seed := correctedGridSource(at, false)
					seed.DXCall, seed.DXCallNorm = "K1ABD", "K1ABD"
					for _, rail := range []*dedup.SecondaryDeduper{p.secondaryFast, p.secondaryMed, p.secondarySlow} {
						t.Cleanup(rail.Stop)
						if !rail.ShouldForward(seed) {
							t.Fatal("actual secondary positive prefill failed")
						}
					}
				}
				if delayed {
					p.handleStabilizerRelease(envelope)
				} else {
					p.processSpot(correctedGridSource(at, false), nil)
				}
				admitted := disposition != "all_lanes_thinned" && disposition != "age_after_acceptance" && disposition != "age_before_acceptance"
				want := uint64(0)
				if admitted && disposition != "reported" {
					want = 1
				}
				if got := p.tracker.CorrectedOutputWithoutGrid(); got != want {
					t.Fatalf("missing-grid admissions=%d want %d", got, want)
				}
				rows := p.buf.GetRecent(2)
				if admitted {
					if len(rows) != 1 || rows[0].DXCallNorm != "K1ABD" || rows[0].Confidence != "C" || (rows[0].DXMetadata.Grid == "") != (disposition != "reported") {
						t.Fatal("counter fixture did not deliver actual corrected grid outcome")
					}
					if archived, ok := tryReadArchiveQueuedSpot(p.archiveWriter); !ok || archived.DXCallNorm != "K1ABD" {
						t.Fatal("multi-destination admission did not reach archive")
					}
					if disposition == "queue_refused" {
						if drops, _, _ := p.telnet.BroadcastMetricSnapshot(); drops != 1 {
							t.Fatal("fixture did not exercise actual broadcast refusal")
						}
					} else if broadcast, ok := tryReadTelnetBroadcastSpot(p.telnet); !ok || broadcast != rows[0] {
						t.Fatal("multi-destination admission did not reach broadcast")
					}
				} else if len(rows) != 0 {
					t.Fatal("excluded ordinary output reached ring")
				}
				if disposition == "all_lanes_thinned" {
					for _, rail := range []*dedup.SecondaryDeduper{p.secondaryFast, p.secondaryMed, p.secondarySlow} {
						if processed, duplicates, _ := rail.GetStats(); processed != 2 || duplicates != 1 {
							t.Fatal("fixture did not thin every actual ordinary lane")
						}
					}
				}
				accepted := 0
				for _, countries := range p.whoSpotsMeStore.CountryCountsByContinent("K1ABD", "20m", time.Now().UTC()) {
					for _, country := range countries {
						accepted += country.Count
					}
				}
				wantAccepted := 1
				if disposition == "age_before_acceptance" {
					wantAccepted = 0
				}
				if accepted != wantAccepted {
					t.Fatalf("accepted history=%d want %d; final output exclusion must not move core learning", accepted, wantAccepted)
				}
			})
		}
	}
}

func TestCorrectedNoGridCounterExcludesUnchangedAndB(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(fmt.Sprintf("rejected_%t", rejected), func(t *testing.T) {
			at := time.Now().UTC().Truncate(time.Second)
			p := newDeliveryTestPipeline(buffer.NewRingBuffer(2), nil, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 2}, nil))
			p.tracker = stats.NewTracker()
			if rejected {
				r, d, key := correctionLossResolver(t, 4, 8)
				correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at.Add(-time.Second), time.Minute, "N0BBB", "N0CCC", "N0FFF")
				p.signalResolver, p.correctionCfg = r, correctionLossConfig()
				p.correctionCfg.InvalidAction = "broadcast"
				b32RejectCTY(t, p)
			}
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at) // Empty DX grid in both negative controls.
			p.processSpot(s, nil)
			rows := p.buf.GetRecent(1)
			if len(rows) != 1 || rows[0].DXMetadata.Grid != "" || rows[0].DXCallNorm != "K1ABC" || (rows[0].Confidence == "B") != rejected {
				t.Fatal("negative control failed to deliver the expected empty-grid unchanged/B identity")
			}
			if p.tracker.CorrectedOutputWithoutGrid() != 0 {
				t.Fatal("unchanged/rejected empty-grid output counted as correction")
			}
		})
	}
}
