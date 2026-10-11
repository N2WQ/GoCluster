package correctionflow

import (
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestTemporalProposalDoesNotConfirmIdentity(t *testing.T) {
	cfg := config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
		Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 2,
		BeamSize: 4, MaxObsCandidates: 4, MaxPending: 8, MaxActiveKeys: 2, MaxEventsPerKey: 4,
	}}
	d := NewTemporalDecoder(cfg)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	if ok, reason := d.Observe(TemporalObservation{ID: 1, Key: key, ObservedAt: at, SubjectCall: "K1ABD",
		Selection: testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)}); !ok {
		t.Fatal(reason)
	}
	proposal := d.Evaluate(1, at.Add(time.Second), false)
	if proposal.Action != TemporalDecisionActionApply || proposal.Winner != "K1ABC" || proposal.Key != key || proposal.Reason != "proposal" {
		t.Fatalf("fixture did not produce an Apply proposal: %+v", proposal)
	}
	if len(d.lastCommitted) != 0 {
		t.Fatalf("proposal without an accepted identity mutation wrote confirmed history: %v", d.lastCommitted)
	}
	if d.PendingCount() != 0 || len(d.keyStates) != 0 {
		t.Fatal("proposal did not retire its bounded decoder request/event ownership")
	}
}

func temporalConfirmationDecoder(maxKeys int) *TemporalDecoder {
	return NewTemporalDecoder(config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
		Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 2,
		BeamSize: 4, MaxObsCandidates: 4, MaxPending: 8, MaxActiveKeys: maxKeys, MaxEventsPerKey: 4,
	}})
}

func temporalConfirmationProposal(t *testing.T, d *TemporalDecoder, id uint64, key spot.ResolverSignalKey, subject, winner string) {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC).Add(time.Duration(id) * time.Second)
	runner := "K1ABD"
	if winner == runner {
		runner = "K1ABC"
	}
	if ok, reason := d.Observe(TemporalObservation{ID: id, Key: key, ObservedAt: at, SubjectCall: subject,
		Selection: testTemporalSelection(key, spot.ResolverStateConfident, winner, 3000, runner, 1000)}); !ok {
		t.Fatal(reason)
	}
	proposal := d.Evaluate(id, at.Add(time.Second), false)
	if proposal.Action != TemporalDecisionActionApply || proposal.Winner != winner || proposal.Key != key || proposal.Reason != "proposal" {
		t.Fatalf("fixture did not produce its literal Apply proposal: %+v", proposal)
	}
}

func TestTemporalRejectedAndUnchangedProposalsPreserveConfirmedHistory(t *testing.T) {
	for _, tc := range []struct {
		name, confirmed, subject, proposed string
	}{
		{"rejected", "K1ABC", "K1ABC", "K1ABD"},
		{"unchanged", "K1ABD", "K1ABC", "K1ABC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := temporalConfirmationDecoder(2)
			key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
			if d.ConfirmCorrection(key, tc.confirmed) {
				t.Fatal("first accepted identity was reported as a path switch")
			}
			before := d.lastCommitted[key]
			// These are decoder proposals only. The caller either rejects its
			// current gates or sees no identity mutation and does not confirm.
			temporalConfirmationProposal(t, d, 1, key, tc.subject, tc.proposed)
			if after := d.lastCommitted[key]; after != before || d.confirmationSequence != 1 {
				t.Fatalf("%s proposal changed confirmed identity/order: before=%+v after=%+v", tc.name, before, after)
			}
			if metrics := d.ConfirmedHistoryMetrics(); metrics.Keys != 1 || metrics.Evictions != 0 || d.PendingCount() != 0 || len(d.keyStates) != 0 {
				t.Fatalf("proposal changed retained history or left request ownership: %+v", metrics)
			}
			if d.ConfirmCorrection(key, tc.confirmed) {
				t.Fatal("unconfirmed proposal invented a later same-call path switch")
			}
		})
	}
}

func TestTemporalConfirmCorrectionNormalizesAcceptedIdentity(t *testing.T) {
	d := temporalConfirmationDecoder(2)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	if d.ConfirmCorrection(key, " k1abc ") || d.lastCommitted[key].call != "K1ABC" || d.lastCommitted[key].sequence != 1 {
		t.Fatal("first normalized accepted identity was lost or reported as a switch")
	}
	if d.ConfirmCorrection(key, "K1ABC") || d.lastCommitted[key].sequence != 2 {
		t.Fatal("same accepted identity invented a switch or failed to refresh confirmation order")
	}
	if !d.ConfirmCorrection(key, " k1abd ") || d.lastCommitted[key].call != "K1ABD" || d.lastCommitted[key].sequence != 3 {
		t.Fatal("accepted identity mutation did not record one normalized path switch")
	}
	if metrics := d.ConfirmedHistoryMetrics(); metrics.Keys != 1 || metrics.Evictions != 0 || d.PendingCount() != 0 {
		t.Fatalf("confirmation depended on a pending request or changed the bound: %+v", metrics)
	}
}

func TestTemporalConfirmCorrectionRejectsInvalidAuthority(t *testing.T) {
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	d := temporalConfirmationDecoder(2)
	d.ConfirmCorrection(key, "K1ABC")
	before := d.lastCommitted[key]
	for _, tc := range []struct {
		key    spot.ResolverSignalKey
		winner string
	}{
		{key, ""}, {key, " \t\n"}, {spot.ResolverSignalKey{}, "K1ABC"},
		{spot.ResolverSignalKey{Band: "40m", Mode: " "}, "K1ABC"},
		{spot.ResolverSignalKey{Band: " ", Mode: "CW"}, "K1ABC"},
	} {
		if d.ConfirmCorrection(tc.key, tc.winner) {
			t.Fatal("invalid confirmation was reported as a switch")
		}
	}
	if d.lastCommitted[key] != before || d.confirmationSequence != 1 || d.ConfirmedHistoryMetrics().Keys != 1 || d.ConfirmedHistoryMetrics().Evictions != 0 {
		t.Fatal("invalid confirmation mutated accepted history")
	}
	var nilDecoder *TemporalDecoder
	disabled := NewTemporalDecoder(config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{MaxActiveKeys: 2}})
	for _, decoder := range []*TemporalDecoder{nilDecoder, disabled, temporalConfirmationDecoder(0), temporalConfirmationDecoder(-1)} {
		if decoder.ConfirmCorrection(key, "K1ABC") || decoder.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{}) {
			t.Fatal("nil/disabled/unbounded decoder retained confirmation authority")
		}
	}
}

func TestTemporalConfirmedHistoryEvictsOldestConfirmation(t *testing.T) {
	d := temporalConfirmationDecoder(2)
	a := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	b, c := a, a
	b.Bucket++
	c.Bucket += 2
	d.ConfirmCorrection(a, "K1ABC")
	d.ConfirmCorrection(b, "K1ABD")
	if d.ConfirmCorrection(a, "K1ABC") {
		t.Fatal("same-call refresh was reported as a switch")
	}
	d.ConfirmCorrection(c, "K1ABE")
	if _, exists := d.lastCommitted[b]; exists {
		t.Fatal("new key did not evict oldest confirmation after A refresh")
	}
	if d.lastCommitted[a].sequence != 3 || d.lastCommitted[c].sequence != 4 || d.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{Keys: 2, Evictions: 1}) {
		t.Fatalf("wrong confirmed history or eviction metrics: %+v", d.ConfirmedHistoryMetrics())
	}
	if d.ConfirmCorrection(b, "K1ABD") {
		t.Fatal("evicted identity supplied unavailable switch history")
	}
	if _, exists := d.lastCommitted[a]; exists {
		t.Fatal("recreated B did not deterministically evict now-oldest A")
	}
	if d.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{Keys: 2, Evictions: 2}) {
		t.Fatal("recreated key grew confirmed history beyond its independent cap")
	}
	if !d.ConfirmCorrection(b, "K1ABC") {
		t.Fatal("retained recreated B did not supply an accepted identity switch")
	}
}

func TestTemporalConfirmedHistoryChurnRemainsBounded(t *testing.T) {
	const capKeys = 3
	d := temporalConfirmationDecoder(capKeys)
	keys := make([]spot.ResolverSignalKey, 128)
	for i := range keys {
		keys[i] = spot.NewResolverSignalKey(7010+float64(i)*.5, "40m", "CW", 500)
		if d.ConfirmCorrection(keys[i], "K1ABC") {
			t.Fatal("fresh churn key invented a path switch")
		}
		metrics := d.ConfirmedHistoryMetrics()
		if metrics.Keys != min(i+1, capKeys) || metrics.Evictions != uint64(max(0, i+1-capKeys)) || d.PendingCount() != 0 || len(d.keyStates) != 0 {
			t.Fatalf("confirmation churn lost its independent bound at %d: %+v", i, metrics)
		}
		if i >= capKeys {
			if _, exists := d.lastCommitted[keys[i-capKeys]]; exists {
				t.Fatal("churn eviction was not oldest-confirmation deterministic")
			}
		}
	}
	for _, key := range keys[len(keys)-capKeys:] {
		if _, exists := d.lastCommitted[key]; !exists {
			t.Fatal("history lost one of its three newest confirmations")
		}
	}
}

func TestTemporalProposalAtConfirmedHistoryCapDoesNotEvict(t *testing.T) {
	d := temporalConfirmationDecoder(2)
	a := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	b, c := a, a
	b.Bucket++
	c.Bucket += 2
	d.ConfirmCorrection(a, "K1ABC")
	d.ConfirmCorrection(b, "K1ABD")
	temporalConfirmationProposal(t, d, 1, c, "K1ABD", "K1ABC")
	if d.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{Keys: 2}) || d.confirmationSequence != 2 ||
		d.lastCommitted[a].call != "K1ABC" || d.lastCommitted[b].call != "K1ABD" {
		t.Fatal("full-history proposal evicted or changed accepted identity history")
	}
	if _, exists := d.lastCommitted[c]; exists {
		t.Fatal("full-history proposal created a confirmed key")
	}
	if d.ConfirmCorrection(c, "K1ABC") || d.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{Keys: 2, Evictions: 1}) {
		t.Fatal("accepted first correction did not evict exactly once at the independent history cap")
	}
	if _, exists := d.lastCommitted[a]; exists {
		t.Fatal("first accepted C correction did not evict oldest confirmed A")
	}
}

func TestTemporalConfirmCorrectionConcurrent(t *testing.T) {
	d := temporalConfirmationDecoder(2)
	a := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	b := a
	b.Bucket++
	keys := []spot.ResolverSignalKey{a, b}
	calls := []string{" k1abc ", " k1abd "}
	const workers, confirmations = 4, 100
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			for i := 0; i < confirmations; i++ {
				if d.ConfirmCorrection(keys[index], calls[index]) {
					t.Error("concurrent same-call confirmation invented a switch")
				}
				if metrics := d.ConfirmedHistoryMetrics(); metrics.Keys > 2 || metrics.Evictions != 0 {
					t.Errorf("concurrent history crossed its cap: %+v", metrics)
				}
			}
		}(worker % 2)
	}
	group.Wait()
	if d.confirmationSequence != workers*confirmations || d.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{Keys: 2}) ||
		d.lastCommitted[a].call != "K1ABC" || d.lastCommitted[b].call != "K1ABD" || max(d.lastCommitted[a].sequence, d.lastCommitted[b].sequence) != workers*confirmations {
		t.Fatal("concurrent confirmation lost normalized identity or monotonic order")
	}
}

func TestTemporalDecisionCarriesTrackedRequestKey(t *testing.T) {
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		force  bool
		offset time.Duration
		action TemporalDecisionAction
	}{
		{"lag defer", false, 0, TemporalDecisionActionDefer},
		{"forced fallback", true, 0, TemporalDecisionActionFallbackResolver},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := temporalConfirmationDecoder(2)
			d.cfg.MinScore = 1_000_000
			if ok, reason := d.Observe(TemporalObservation{ID: 1, Key: key, ObservedAt: at, SubjectCall: "K1ABD",
				Selection: testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)}); !ok {
				t.Fatal(reason)
			}
			decision := d.Evaluate(1, at.Add(tc.offset), tc.force)
			if decision.Key != key || decision.Action != tc.action || d.ConfirmedHistoryMetrics().Keys != 0 {
				t.Fatalf("tracked action lost request key or wrote confirmation: %+v", decision)
			}
			missing := d.Evaluate(99, at, false)
			if missing.Key != (spot.ResolverSignalKey{}) || missing.Reason != "missing_request" {
				t.Fatal("missing request invented a resolver key")
			}
		})
	}
}
