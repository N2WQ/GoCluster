package reputation

import (
	"bytes"
	"fmt"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
)

func stationGate(t *testing.T) (*Gate, time.Time) {
	t.Helper()
	gate, err := NewGate(config.ReputationConfig{
		Enabled: true, ReputationDir: t.TempDir(), InitialWaitSeconds: 10,
		RampWindowSeconds: 60, PerBandStart: 1, PerBandCap: 3,
		TotalCapStart: 2, TotalCapPostRamp: 5, TotalCapRampDelaySeconds: 15,
		ResetOnNewASN: true, CountryFlipScope: "country",
		IPv4BucketSize: 10000, IPv4BucketRefillPerSec: 10000,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gate.Close)
	gate.lookupIPInfoFunc = func(addr netip.Addr, _ time.Time) LookupResult {
		result := LookupResult{ASN: "64500", CountryCode: "US", CountryName: "United States", ContinentCode: "NA", Source: "fixture"}
		if addr.String() == "203.0.113.2" {
			result.ASN = "64501"
		}
		if addr.String() == "203.0.113.3" {
			result.CountryCode, result.CountryName = "CA", "Canada"
		}
		return result
	}
	return gate, time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
}

func stationState(gate *Gate) callState {
	shard := gate.callShard("K1ABC")
	shard.mu.Lock()
	defer shard.mu.Unlock()
	state := *shard.calls["K1ABC"]
	state.perBandCounts = maps.Clone(state.perBandCounts)
	return state
}

func stationCheck(gate *Gate, call, band string, now time.Time) Decision {
	return gate.Check(Request{Call: call, Band: band, IP: "203.0.113.1", Now: now})
}

func TestStationReconnectPreservesAllowanceAndNaturalBoundary(t *testing.T) {
	gate, start := stationGate(t)
	gate.RecordLogin(" k1abc-1 ", "203.0.113.1", start)
	gate.RecordLogin("K1ABC-2", "203.0.113.1", start.Add(5*time.Second))
	if got := stationState(gate); !got.nextAllowedAt.Equal(start.Add(10 * time.Second)) {
		t.Fatalf("benign reconnect changed probation: %v", got.nextAllowedAt)
	}
	if decision := stationCheck(gate, "K1ABC-2", "20m", start.Add(10*time.Second-time.Nanosecond)); decision.Reason != DropProbation {
		t.Fatalf("probation ended early: %+v", decision)
	}
	now := start.Add(11 * time.Second)
	for _, band := range []string{"20m", "40m"} {
		if decision := stationCheck(gate, "K1ABC-1", band, now); !decision.Allow {
			t.Fatalf("positive allowance control: %+v", decision)
		}
	}
	before := stationState(gate)
	gate.RecordLogin("K1ABC-999", "203.0.113.1", start.Add(12*time.Second))
	after := stationState(gate)
	if after.totalCount != 2 || after.perBandCounts["20m"] != 1 || after.perBandCounts["40m"] != 1 ||
		!after.windowStart.Equal(before.windowStart) || !after.nextRampAt.Equal(before.nextRampAt) {
		t.Fatalf("reconnect replenished or rephased allowance: before=%+v after=%+v", before, after)
	}
	boundary := start.Add(70 * time.Second)
	gate.RecordLogin("K1ABC-2", "203.0.113.1", boundary.Add(-time.Nanosecond))
	if decision := stationCheck(gate, "K1ABC", "80m", boundary.Add(-time.Nanosecond)); decision.Reason != DropTotalCap {
		t.Fatalf("allowance replenished before boundary: %+v", decision)
	}
	gate.RecordLogin("K1ABC-2", "203.0.113.1", boundary)
	if decision := stationCheck(gate, "k1abc-3", "20m", boundary); !decision.Allow {
		t.Fatalf("natural boundary did not replenish: %+v", decision)
	}
	if got := stationState(gate); got.totalCount != 1 || got.perBandLimit != 2 || !got.windowStart.Equal(boundary) {
		t.Fatalf("wrong elapsed-window advancement: %+v", got)
	}
}

func TestStationRiskPreservesSpentCountsAndExtendsDeadlines(t *testing.T) {
	for _, ip := range []string{"203.0.113.2", "203.0.113.3"} {
		t.Run(ip, func(t *testing.T) {
			gate, start := stationGate(t)
			gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
			for _, band := range []string{"20m", "40m"} {
				if decision := stationCheck(gate, "K1ABC", band, start.Add(11*time.Second)); !decision.Allow {
					t.Fatal(decision)
				}
			}
			before := stationState(gate)
			gate.RecordLogin("K1ABC-2", ip, start.Add(12*time.Second))
			after := stationState(gate)
			flag := PenaltyASNReset
			if ip == "203.0.113.3" {
				flag = PenaltyGeoFlip
			}
			if !after.penaltyFlags.Has(flag) || after.totalCount != 2 || after.perBandCounts["20m"] != 1 ||
				!after.windowStart.Equal(before.windowStart) || !after.nextAllowedAt.Equal(start.Add(22*time.Second)) ||
				!after.nextRampAt.Equal(start.Add(82*time.Second)) {
				t.Fatalf("risk cleared allowance or failed to extend restrictions: %+v", after)
			}
			gate.RecordLogin("K1ABC-3", ip, start.Add(13*time.Second))
			if decision := stationCheck(gate, "K1ABC-4", "80m", start.Add(22*time.Second-time.Nanosecond)); decision.Reason != DropProbation {
				t.Fatalf("reconnect shortened penalty probation: %+v", decision)
			}
			if decision := stationCheck(gate, "K1ABC-4", "80m", start.Add(22*time.Second)); decision.Reason != DropTotalCap {
				t.Fatalf("risk replenished allowance at probation end: %+v", decision)
			}
		})
	}
}

func TestStationPenaltyWaitsForMaturityAndPreservesUpgradeFloor(t *testing.T) {
	for _, laterFloor := range []bool{false, true} {
		t.Run(map[bool]string{false: "scheduled upgrade", true: "later floor"}[laterFloor], func(t *testing.T) {
			gate, start := stationGate(t)
			gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
			if decision := stationCheck(gate, "K1ABC", "20m", start.Add(131*time.Second)); !decision.Allow {
				t.Fatal(decision)
			}
			shard := gate.callShard("K1ABC")
			if laterFloor {
				shard.mu.Lock()
				shard.calls["K1ABC"].totalCapUpgradeAt = start.Add(500 * time.Second)
				shard.mu.Unlock()
			}
			gate.RecordLogin("K1ABC-2", "203.0.113.2", start.Add(132*time.Second))
			stationCheck(gate, "K1ABC", "40m", start.Add(146*time.Second))
			if state := stationState(gate); state.perBandLimit != 1 || state.totalCap != 2 || state.totalCount != 2 {
				t.Fatalf("old total upgrade bypassed demoted maturity or restored spent counts: %+v", state)
			}
			stationCheck(gate, "K1ABC", "20m", start.Add(202*time.Second))
			stationCheck(gate, "K1ABC", "20m", start.Add(262*time.Second))
			deadline := start.Add(277 * time.Second)
			if laterFloor {
				deadline = start.Add(500 * time.Second)
			}
			if state := stationState(gate); state.perBandLimit != 3 || state.totalCap != 2 || !state.totalCapUpgradeAt.Equal(deadline) {
				t.Fatalf("maturity shortened upgrade floor/delay: %+v", state)
			}
			stationCheck(gate, "K1ABC", "40m", deadline.Add(-time.Nanosecond))
			if state := stationState(gate); state.totalCap != 2 {
				t.Fatal("upgrade occurred before its deadline")
			}
			stationCheck(gate, "K1ABC", "40m", deadline)
			if state := stationState(gate); state.totalCap != 5 {
				t.Fatal("upgrade failed at its deadline")
			}
		})
	}
}

func TestStationHistoryUsesBaselineWithoutMergingSSIDFiles(t *testing.T) {
	for _, existingBaseline := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing baseline", true: "existing baseline"}[existingBaseline], func(t *testing.T) {
			gate, start := stationGate(t)
			old := []byte("recent_asns: [OLD-AS]\nrecent_countries: [OLD-GEO]\n")
			oldPath := filepath.Join(gate.store.dir, "K1ABC-1.yaml")
			if err := os.WriteFile(oldPath, old, 0600); err != nil {
				t.Fatal(err)
			}
			if existingBaseline {
				if _, _, err := gate.store.Touch("K1ABC", "64500", "US", "NA"); err != nil {
					t.Fatal(err)
				}
			}
			gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
			state := stationState(gate)
			wait := start.Add(10 * time.Second)
			if existingBaseline {
				wait = start
			}
			if !state.nextAllowedAt.Equal(wait) || len(state.asnHistory) != 1 || state.asnHistory[0] != "64500" {
				t.Fatalf("wrong baseline history/probation: %+v", state)
			}
			if after, err := os.ReadFile(oldPath); err != nil || !bytes.Equal(after, old) {
				t.Fatal("old SSID history was merged or overwritten")
			}
			if _, err := gate.store.Load("K1ABC"); err != nil {
				t.Fatal("baseline record was not created", err)
			}
		})
	}
}

func waitLoginLock(t *testing.T, shard *callShard) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for shard.loginMu.TryLock() {
		shard.loginMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("login did not acquire its history owner")
		}
		runtime.Gosched()
	}
}

func TestStationHistoryStallDoesNotBlockCheckOrResurrectEvictedState(t *testing.T) {
	gate, start := stationGate(t)
	gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
	shard := gate.callShard("K1ABC")
	shard.mu.Lock()
	old := shard.calls["K1ABC"]
	shard.mu.Unlock()
	gate.store.mu.Lock()
	var release sync.Once
	unlock := func() { release.Do(gate.store.mu.Unlock) }
	done := make(chan struct{})
	t.Cleanup(func() {
		unlock()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("stalled login worker did not join")
		}
	})
	go func() {
		defer close(done)
		gate.RecordLogin("K1ABC-2", "203.0.113.2", start.Add(12*time.Second))
	}()
	waitLoginLock(t, shard)
	gate.cfg.stateTTL = time.Second
	gate.sweepCalls(start.Add(13 * time.Second))
	checked := make(chan Decision, 1)
	go func() { checked <- stationCheck(gate, "K1ABC-3", "20m", start.Add(14*time.Second)) }()
	select {
	case decision := <-checked:
		if decision.Reason != DropProbation {
			t.Fatalf("fresh state lost ordinary probation: %+v", decision)
		}
	case <-time.After(3 * time.Second):
		unlock()
		select {
		case <-checked:
		case <-time.After(3 * time.Second):
			t.Error("Check worker did not join after releasing stalled history")
		}
		t.Fatal("Check blocked on login history I/O")
	}
	unlock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("login did not publish")
	}
	stationCheck(gate, "K1ABC-4", "20m", start.Add(13*time.Second))
	shard.mu.Lock()
	current := shard.calls["K1ABC"]
	shard.mu.Unlock()
	if current == old || !current.lastSeen.Equal(start.Add(14*time.Second)) || !current.nextAllowedAt.Equal(start.Add(24*time.Second)) || current.currentASN != "64501" {
		t.Fatalf("login resurrected a retired state or shortened current deadlines: %+v", current)
	}
}

func TestStationConcurrentSSIDsKeepSpentAllowanceAndCoherentHistory(t *testing.T) {
	gate, start := stationGate(t)
	gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
	now := start.Add(11 * time.Second)
	for _, band := range []string{"20m", "40m"} {
		if decision := stationCheck(gate, "K1ABC", band, now); !decision.Allow {
			t.Fatal(decision)
		}
	}
	var workers sync.WaitGroup
	decisions := make(chan Decision, 40)
	for i := range 40 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ip := "203.0.113.1"
			if i%2 != 0 {
				ip = "203.0.113.2"
			}
			gate.RecordLogin(fmt.Sprintf("K1ABC-%d", i+2), ip, now)
			decisions <- stationCheck(gate, "K1ABC-3", "80m", now)
		}()
	}
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent login/check workers did not join")
	}
	close(decisions)
	for decision := range decisions {
		if decision.Allow || !decision.Drop {
			t.Fatalf("concurrent login replenished spent allowance: %+v", decision)
		}
	}
	record, err := gate.store.Load("K1ABC")
	state := stationState(gate)
	if err != nil || len(record.RecentASNs) != 2 || len(state.asnHistory) != 2 || state.currentASN != record.RecentASNs[0] || state.totalCount != 2 {
		t.Fatalf("history/publication was incoherent: record=%+v state=%+v err=%v", record, state, err)
	}
	for i := range gate.callShards {
		shard := &gate.callShards[i]
		shard.mu.Lock()
		for call := range shard.calls {
			if call != "K1ABC" {
				t.Errorf("SSID retained separate state: %s", call)
			}
		}
		shard.mu.Unlock()
	}
	gate.sweepCalls(now.Add(gate.cfg.stateTTL + time.Nanosecond))
	for i := range gate.callShards {
		if len(gate.callShards[i].calls) != 0 {
			t.Fatal("expired station state retained an alias or secondary entry")
		}
	}
}

func TestStationRefusedHistoryPreservesBytesCountsAndLaterDeadline(t *testing.T) {
	gate, start := stationGate(t)
	gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
	if decision := stationCheck(gate, "K1ABC", "20m", start.Add(11*time.Second)); !decision.Allow {
		t.Fatal(decision)
	}
	shard := gate.callShard("K1ABC")
	shard.mu.Lock()
	shard.calls["K1ABC"].nextAllowedAt = start.Add(50 * time.Second)
	shard.calls["K1ABC"].nextRampAt = start.Add(150 * time.Second)
	shard.mu.Unlock()
	path := filepath.Join(gate.store.dir, "K1ABC.yaml")
	broken := []byte("recent_asns: [\n")
	if err := os.WriteFile(path, broken, 0600); err != nil {
		t.Fatal(err)
	}
	gate.RecordLogin("K1ABC-2", "203.0.113.1", start.Add(12*time.Second))
	state := stationState(gate)
	if state.totalCount != 1 || state.perBandCounts["20m"] != 1 ||
		!state.nextAllowedAt.Equal(start.Add(50*time.Second)) || !state.nextRampAt.Equal(start.Add(150*time.Second)) {
		t.Fatalf("history failure reset counts or shortened probation: %+v", state)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, broken) {
		t.Fatal("failed history update overwrote refused bytes")
	}
}

func TestStationExtraDelayExtendsRampWithoutAddingProbation(t *testing.T) {
	gate, start := stationGate(t)
	gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
	if decision := stationCheck(gate, "K1ABC", "20m", start.Add(11*time.Second)); !decision.Allow {
		t.Fatal(decision)
	}
	gate.cfg.unknownPenalty = 30 * time.Second
	gate.RecordLogin("K1ABC-2", "", start.Add(12*time.Second))
	state := stationState(gate)
	if !state.penaltyFlags.Has(PenaltyUnknown) || state.totalCount != 1 ||
		!state.nextRampAt.Equal(start.Add(102*time.Second)) || state.nextAllowedAt.After(start.Add(12*time.Second)) {
		t.Fatalf("extra delay changed its role or replenished counts: %+v", state)
	}
	if decision := stationCheck(gate, "K1ABC-3", "40m", start.Add(12*time.Second)); !decision.Allow {
		t.Fatalf("extra ramp delay unexpectedly added probation: %+v", decision)
	}
}

func TestStationHistoryOwnerRemainsHeldThroughPublication(t *testing.T) {
	gate, start := stationGate(t)
	gate.RecordLogin("K1ABC-1", "203.0.113.1", start)
	shard := gate.callShard("K1ABC")
	shard.mu.Lock() // Stall publication after the disk history has changed.
	var release sync.Once
	unlock := func() { release.Do(shard.mu.Unlock) }
	done := make(chan struct{})
	t.Cleanup(func() {
		unlock()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("publication worker did not join")
		}
	})
	go func() {
		defer close(done)
		gate.RecordLogin("K1ABC-2", "203.0.113.2", start.Add(12*time.Second))
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		record, err := gate.store.Load("K1ABC")
		if err == nil && len(record.RecentASNs) != 0 && record.RecentASNs[0] == "64501" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("login did not complete history update before publication")
		}
		time.Sleep(time.Millisecond)
	}
	if shard.loginMu.TryLock() {
		shard.loginMu.Unlock()
		t.Fatal("history owner released before publication")
	}
	unlock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("login did not complete publication")
	}
	if state := stationState(gate); state.currentASN != "64501" || state.asnHistory[0] != "64501" {
		t.Fatal("published state differs from its serialized history")
	}
}

func TestStationSweepThresholdAndReplacementChurn(t *testing.T) {
	gate, start := stationGate(t)
	gate.cfg.stateMaxEntries = 3
	shard := &gate.callShards[0]
	for cycle := range 3 {
		inserted := 0
		for candidate := cycle * 1000; candidate < (cycle+1)*1000 && inserted < 5; candidate++ {
			call := fmt.Sprintf("K%dABC", candidate)
			if gate.callShard(call) != shard {
				continue
			}
			gate.Check(Request{Call: call, Band: "20m", Now: start})
			inserted++
		}
		if inserted != 5 || len(shard.calls) <= 3 {
			t.Fatal("fixture did not exceed the actual per-shard threshold")
		}
		gate.sweepCalls(start)
		if len(shard.calls) != 3 {
			t.Fatalf("replacement churn exceeded the existing sweep threshold: %d", len(shard.calls))
		}
	}
	gate.sweepCalls(start.Add(gate.cfg.stateTTL + time.Nanosecond))
	if len(shard.calls) != 0 || len(gate.callShards) != 32 {
		t.Fatal("expiry retained replacement state or expanded fixed lock ownership")
	}
}
