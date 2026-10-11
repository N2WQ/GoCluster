//go:build !qualification

// File role: Checks the family's production fresh-selection consumer against
// timestamped resolver publications and real nonblocking broadcast admission.
package cluster

import (
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestTelnetFamilyFreshPublishedConsumer(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		name    string
		query   time.Time
		visible bool
	}{{"fresh", at.Add(time.Second), false}, {"inclusive_cutoff", at.Add(2 * time.Minute), false},
		{"expired", at.Add(2*time.Minute + time.Nanosecond), true}, {"future", at.Add(-time.Nanosecond), true}} {
		for _, keyed := range []bool{false, true} {
			name := tc.name + "/derived_key"
			if keyed {
				name = tc.name + "/existing_key"
			}
			t.Run(name, func(t *testing.T) {
				cfg := ambiguityDeliveryConfig(false)
				r := familyAdmissionResolver(t, cfg.FrequencyToleranceHz, at, "N2WQ/KP4", "N2WQ")
				key := spot.NewResolverSignalKey(14030, "20m", "CW", cfg.FrequencyToleranceHz)
				raw, ok := r.Lookup(key)
				if !ok || raw.State != spot.ResolverStateConfident || raw.TotalReporters != 4 {
					t.Fatal("raw publication must remain a settled four-receiver control")
				}
				p := &outputPipeline{signalResolver: r, correctionCfg: cfg, familySuppressor: newTestTelnetFamilySuppressor()}
				strong, weak := familyConsumerSpot("N2WQ/KP4", 14030), familyConsumerSpot("N2WQ", 14030)
				ctx := outputSpotContext{spot: weak, stabilizerResolverKey: key, hasStabilizerResolverKey: keyed}
				selected := p.selectFamilyEvidenceAt(&ctx, weak, tc.query)
				if !selected.SnapshotOK || (selected.Snapshot.State == spot.ResolverStateConfident) == tc.visible {
					t.Fatalf("eligible consumer authority does not match the query boundary: %+v", selected)
				}
				// Seed an admitted offer at this decision clock independently of
				// current evidence. Only the later fresh population grants authority.
				srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 2}, nil)
				all := familyLanes{true, true, true}
				p.familySuppressor.handoff(strong, cfg, tc.query, all, nil, func(l familyLanes) bool {
					return srv.TryBroadcastSpotOwned(strong, l.fast, l.med, l.slow)
				})
				if got, ok := tryReadTelnetBroadcastSpot(srv); !ok || got != strong {
					t.Fatal("strong control did not actually enter the broadcast queue")
				}
				mask := p.familySuppressor.handoff(weak, cfg, tc.query, all,
					func(now time.Time) correctionflow.ResolverPrimarySelection {
						return p.selectFamilyEvidenceAt(&ctx, weak, now)
					},
					func(l familyLanes) bool { return srv.TryBroadcastSpotOwned(weak, l.fast, l.med, l.slow) })
				got, offered := tryReadTelnetBroadcastSpot(srv)
				if mask.any() != tc.visible || offered != tc.visible || (offered && got != weak) {
					t.Fatalf("fresh family consumer mask=%+v queue=%t call=%v want visible=%t", mask, offered, got, tc.visible)
				}
			})
		}
	}
}

func TestTelnetFamilyFreshNeighborhoodConservesWeightedReceivers(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	tc := correctionEvidenceDeliveryCase{aReceivers: []string{"N0AAA", "N0BBB"}, dissent: "K1AB",
		weights: spot.SpotterReliability{"N0AAA": .900, "N0BBB": .950, "N0CCC": 1}}
	r, key := correctionEvidenceResolver(t, tc, 2, at)
	cfg := ambiguityDeliveryConfig(true)
	p := &outputPipeline{signalResolver: r, correctionCfg: cfg, telnet: telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 2}, nil),
		familySuppressor: newTestTelnetFamilySuppressor()}
	weak := familyConsumerSpot("K1AB", 14020)
	ctx := outputSpotContext{spot: weak, stabilizerResolverKey: key, hasStabilizerResolverKey: true}
	selection := p.selectFamilyEvidenceAt(&ctx, weak, time.Now().UTC())
	if !selection.SnapshotOK || !selection.UsedNeighborhood || selection.NeighborhoodSplit || selection.Snapshot.EvidenceLoss ||
		selection.Snapshot.State != spot.ResolverStateConfident || selection.Snapshot.TotalReporters != 3 ||
		selection.Snapshot.TotalWeightedSupportMilli != 2850 {
		t.Fatalf("two buckets must retain three distinct receivers and dissent: %+v", selection)
	}
	for _, check := range []struct {
		call string
		want int
	}{{"K1ABC", 64}, {"K1AB", 35}} {
		if got, ok := familyPercent(selection.Snapshot, check.call); !ok || got != check.want {
			t.Fatalf("independent unequal-weight oracle %s=%d available=%t want %d", check.call, got, ok, check.want)
		}
	}
	strong := familyConsumerSpot("K1ABC", 14020)
	p.emitSpot(&outputSpotContext{spot: strong, stabilizerResolverKey: key, hasStabilizerResolverKey: true},
		outputDeliveryPlan{allowFast: true, allowMed: true, allowSlow: true, telnetDeliverNow: true})
	if got, ok := tryReadTelnetBroadcastSpot(p.telnet); !ok || got.DXCall != "K1ABC" {
		t.Fatal("stronger actual queue offer was not established")
	}
	p.emitSpot(&ctx, outputDeliveryPlan{allowFast: true, allowMed: true, allowSlow: true, telnetDeliverNow: true})
	if _, ok := tryReadTelnetBroadcastSpot(p.telnet); ok || p.familySuppressor.totalEntries != 1 {
		t.Fatal("repeated buckets manufactured authority or hidden input taught offer history")
	}
}

func TestTelnetFamilyFreshExactSplitVetoesHistory(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	r, _, key := ambiguityDeliveryResolver(t, at, false)
	cfg := ambiguityDeliveryConfig(true)
	cfg.FamilyPolicy.TelnetSuppression.EditNeighborEnabled = true
	p := &outputPipeline{signalResolver: r, correctionCfg: cfg, telnet: telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil),
		familySuppressor: newTestTelnetFamilySuppressor()}
	for _, call := range []string{"K1ABC", "K1ABD"} {
		s := familyConsumerSpot(call, 14020)
		ctx := outputSpotContext{spot: s, stabilizerResolverKey: key, hasStabilizerResolverKey: true}
		selection := p.selectFamilyEvidenceAt(&ctx, s, time.Now().UTC())
		if !selection.SnapshotOK || selection.Snapshot.State != spot.ResolverStateSplit {
			t.Fatalf("exact split must survive the family consumer's configured neighborhood: %+v", selection)
		}
		p.emitSpot(&ctx, outputDeliveryPlan{allowFast: true, allowMed: true, allowSlow: true, telnetDeliverNow: true})
		if got, ok := tryReadTelnetBroadcastSpot(p.telnet); !ok || got.DXCall != call {
			t.Fatalf("protected fresh split lost ordinary queue output: %v available=%t", got, ok)
		}
	}
	if p.familySuppressor.totalEntries != 2 {
		t.Fatal("both admitted split identities must retain independent offer history")
	}
}

func familyConsumerSpot(call string, frequency float64) *spot.Spot {
	s := spot.NewSpot(call, "N0AAA", frequency, "CW")
	s.SourceType = spot.SourceManual
	return s.SealForAsync()
}

func TestTelnetFamilyFreshReceiverLossVetoesHistory(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	r, driver, key := correctionLossResolver(t, 4, 2)
	correctionLossAdmit(t, r, driver, key, "K1ABC", 14020, at, 2*time.Minute, "N0AAA", "N0BBB", "N0CCC")
	correctionLossAdmit(t, r, driver, key, "K1ABD", 14020, at, 2*time.Minute, "N0DDD")
	correctionLossEvictions(t, r, 0, 1)
	cfg := correctionLossConfig()
	cfg.FamilyPolicy.TelnetSuppression.EditNeighborEnabled = true
	p := &outputPipeline{signalResolver: r, correctionCfg: cfg, telnet: telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil),
		familySuppressor: newTestTelnetFamilySuppressor()}
	for _, call := range []string{"K1ABC", "K1ABD"} {
		s := familyConsumerSpot(call, 14020)
		ctx := outputSpotContext{spot: s, stabilizerResolverKey: key, hasStabilizerResolverKey: true}
		selection := p.selectFamilyEvidenceAt(&ctx, s, time.Now().UTC())
		if !selection.SnapshotOK || !selection.Snapshot.EvidenceLoss || selection.Snapshot.State != spot.ResolverStateUncertain {
			t.Fatalf("actual receiver eviction must veto fresh family authority: %+v", selection)
		}
		p.emitSpot(&ctx, outputDeliveryPlan{allowFast: true, allowMed: true, allowSlow: true, telnetDeliverNow: true})
		if got, ok := tryReadTelnetBroadcastSpot(p.telnet); !ok || got.DXCall != call {
			t.Fatalf("known incomplete evidence lost ordinary queue output: %v available=%t", got, ok)
		}
	}
	if p.familySuppressor.totalEntries != 2 {
		t.Fatal("admitted offers remain distinct while incomplete evidence has no hiding authority")
	}
}
