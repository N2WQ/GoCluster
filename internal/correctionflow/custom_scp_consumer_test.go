// File role: Locks consumer-specific historical requirements through the shared
// resolver and stabilizer entry points, using independently encoded evidence.
package correctionflow

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

type b12ConsumerRow struct {
	call, receiver string
	seen           time.Time
	cell           uint16
}

func b12ConsumerRows(call string, count int, seen time.Time) []b12ConsumerRow {
	rows := make([]b12ConsumerRow, count)
	for i := range rows {
		rows[i] = b12ConsumerRow{call, fmt.Sprintf("N0%03d", i), seen, uint16(101 + i%2)}
	}
	return rows
}

func b12ConsumerStore(t testing.TB, rows []b12ConsumerRow, coreScore, coreCells int) *spot.CustomSCPStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scp")
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		var value [10]byte
		binary.BigEndian.PutUint64(value[:8], uint64(row.seen.Unix()))
		binary.BigEndian.PutUint16(value[8:], row.cell)
		key := fmt.Sprintf("o|%s|20m|cw|%s", row.call, row.receiver)
		if err := db.Set([]byte(key), value[:], pebble.Sync); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := spot.OpenCustomSCPStore(spot.CustomSCPOptions{
		Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxSpottersPerKey: 8,
		CoreMinScore: coreScore, CoreMinH3Cells: coreCells,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if got := store.StatsSnapshot().ObservationSpotters; got != len(rows) {
		t.Fatalf("independent fixture must recover all receivers: got %d want %d", got, len(rows))
	}
	return store
}

func b12ConsumerConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		Enabled: true, MinConsensusReports: 3, MinAdvantage: 1, MinConfidencePercent: 60,
		MaxEditDistance: 1, DistanceModelCW: "plain",
		RecentBandRecordMinUniqueSpotters: 2,
		ResolverRecentPlus1Enabled:        true, ResolverRecentPlus1MinUniqueWinner: 1,
		ResolverRecentPlus1RequireSubjectWeaker: true, ResolverRecentPlus1MaxDistance: 1,
		StabilizerEnabled: true, StabilizerMaxChecks: 2,
		CustomSCP: config.CallCorrectionCustomSCPConfig{
			Enabled: true, ResolverMinScore: 5, ResolverMinUniqueSpotters: 4, ResolverMinUniqueH3Cells: 2,
			StabilizerMinScore: 5, StabilizerMinUniqueSpotters: 3, StabilizerMinUniqueH3Cells: 2,
		},
	}
}

func b12ConsumerGate(cfg config.CallCorrectionConfig, store spot.RecentSupportStore, now time.Time) spot.ResolverPrimaryGateResult {
	settings := BuildCorrectionSettings(BuildSettingsInput{
		Cfg: cfg, MinReports: 3, Window: time.Minute, FreqToleranceHz: 500, RecentBandStore: store,
	})
	return spot.EvaluateResolverPrimaryGates("K1ABC", "K1ABD", "20m", "CW", 0, 2, 100, 0, 2000,
		settings, now, spot.ResolverPrimaryGateOptions{})
}

func TestB12ResolverUsesOwnHistoryRequirements(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		age                  time.Duration
		coreScore, coreCells int
	}{
		{"score", 24 * time.Hour, 10, 2},
		{"cells", time.Minute, 5, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			store := b12ConsumerStore(t, b12ConsumerRows("K1ABD", 4, now.Add(-tc.age)), tc.coreScore, tc.coreCells)
			cfg := b12ConsumerConfig()
			cfg.CustomSCP.StabilizerMinScore, cfg.CustomSCP.StabilizerMinUniqueH3Cells = tc.coreScore, tc.coreCells
			gate := b12ConsumerGate(cfg, store, now)
			if !gate.Allow || !gate.RecentPlus1Applied || gate.RecentPlus1Winner != 4 {
				t.Fatalf("four resolver-eligible receivers must authorize the one-short rail independently of stricter core/stabilizer settings: %+v", gate)
			}
		})
	}
}

func TestB12ResolverQueryOrderPreservesConsumerRequirements(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := b12ConsumerStore(t, b12ConsumerRows("K1ABD", 4, now.Add(-24*time.Hour)), 10, 2)
	loose := b12ConsumerConfig()
	strict := loose
	strict.CustomSCP.ResolverMinScore = 10
	before := store.StatsSnapshot()
	for _, order := range [][]bool{{false, true, false}, {true, false, true}} {
		for _, useStrict := range order {
			cfg := loose
			if useStrict {
				cfg = strict
			}
			gate := b12ConsumerGate(cfg, store, now)
			if gate.Allow != !useStrict || gate.RecentPlus1Applied != !useStrict {
				t.Fatalf("query order must not change another consumer's minimum; strict=%t result=%+v", useStrict, gate)
			}
		}
	}
	if after := store.StatsSnapshot(); after != before {
		t.Fatalf("query requirements changed retained ownership: before=%+v after=%+v", before, after)
	}
}

func TestB12ResolverUniqueMinimums(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		common, resolver, rail, needed int
	}{
		{"resolver", 2, 4, 1, 4}, {"common", 5, 2, 1, 5}, {"rail", 2, 2, 6, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, count := range []int{tc.needed - 1, tc.needed} {
				now := time.Now().UTC().Truncate(time.Second)
				store := b12ConsumerStore(t, b12ConsumerRows("K1ABD", count, now.Add(-time.Minute)), 5, 2)
				cfg := b12ConsumerConfig()
				cfg.RecentBandRecordMinUniqueSpotters, cfg.CustomSCP.ResolverMinUniqueSpotters = tc.common, tc.resolver
				cfg.ResolverRecentPlus1MinUniqueWinner = tc.rail
				gate := b12ConsumerGate(cfg, store, now)
				if gate.Allow != (count == tc.needed) || gate.RecentPlus1Winner != count {
					t.Fatalf("manual config must apply the effective unique minimum %d while retaining partial count %d: %+v", tc.needed, count, gate)
				}
			}
		})
	}
}

func TestB12ResolverValidationUsesItsUniqueMinimum(t *testing.T) {
	for _, count := range []int{3, 4} {
		t.Run(fmt.Sprintf("receivers_%d", count), func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			store := b12ConsumerStore(t, b12ConsumerRows("K1ABC", count, now.Add(-time.Minute)), 5, 2)
			cfg := b12ConsumerConfig()
			cfg.MaxEditDistance, cfg.ResolverRecentPlus1Enabled = 2, false
			cfg.FamilyPolicy.Truncation.Enabled = true
			cfg.FamilyPolicy.Truncation.MaxLengthDelta = 2
			cfg.FamilyPolicy.Truncation.MinShorterLength = 3
			cfg.FamilyPolicy.Truncation.AllowPrefixMatch = true
			cfg.FamilyPolicy.Truncation.Delta2Rails.Enabled = true
			cfg.FamilyPolicy.Truncation.Delta2Rails.RequireCandidateValidated = true
			settings := BuildCorrectionSettings(BuildSettingsInput{Cfg: cfg, MinReports: 1, RecentBandStore: store})
			gate := spot.EvaluateResolverPrimaryGates("K1A", "K1ABC", "20m", "CW", 0, 1, 100, 0, 1000,
				settings, now, spot.ResolverPrimaryGateOptions{})
			if count == 3 && (gate.Allow || gate.Reason != "truncation_delta2_candidate_unvalidated") {
				t.Fatalf("three receivers cannot pass resolver validation minimum four: %+v", gate)
			}
			if count == 4 && !gate.Allow {
				t.Fatalf("four receivers must independently pass the validation gate: %+v", gate)
			}
		})
	}
}

func TestB12ResolverPreservesPartialSubjectCounts(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	rows := append(b12ConsumerRows("K1ABD", 4, now.Add(-24*time.Hour)), b12ConsumerRows("K1ABC", 3, now.Add(-24*time.Hour))...)
	store := b12ConsumerStore(t, rows, 10, 3)
	cfg := b12ConsumerConfig()
	cfg.BayesBonus = config.CallCorrectionBayesBonusConfig{
		Enabled: true, WeightedSmoothingMilli: 1000, RecentSmoothing: 1,
		ObsLogCapMilli: 350, PriorLogMaxMilli: 600,
		ReportThresholdDistance1Milli: 450, ReportThresholdDistance2Milli: 650,
		AdvantageThresholdDistance1Milli: 700, AdvantageThresholdDistance2Milli: 950,
	}
	gate := b12ConsumerGate(cfg, store, now)
	if !gate.Allow || gate.RecentPlus1Winner != 4 || gate.RecentPlus1Subject != 3 ||
		gate.BayesWinnerRecent != 4 || gate.BayesSubjectRecent != 3 {
		t.Fatalf("unique admission minimum four must not turn independently eligible 4:3 history into 4:0: %+v", gate)
	}
}

func TestB12StabilizerUsesOwnHistoryRequirements(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		age                  time.Duration
		coreScore, coreCells int
	}{
		{"score", 24 * time.Hour, 10, 2}, {"cells", time.Minute, 5, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			store := b12ConsumerStore(t, b12ConsumerRows("K1ABC", 3, now.Add(-tc.age)), tc.coreScore, tc.coreCells)
			cfg := b12ConsumerConfig()
			cfg.CustomSCP.ResolverMinScore, cfg.CustomSCP.ResolverMinUniqueH3Cells = tc.coreScore, tc.coreCells
			s := spot.NewSpot("K1ABC", "N0SRC", 14020, "CW")
			s.Confidence = "S"
			decision := EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
			if decision.ShouldDelay || decision.Reason != StabilizerDelayReasonNone {
				t.Fatalf("three stabilizer-eligible receivers must pass independently of stricter core/resolver requirements: %+v", decision)
			}
		})
	}
}

func TestB12StabilizerUniqueMinimums(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		common, stabilizer, needed int
	}{{"stabilizer", 2, 3, 3}, {"common", 4, 2, 4}} {
		t.Run(tc.name, func(t *testing.T) {
			for _, count := range []int{tc.needed - 1, tc.needed} {
				now := time.Now().UTC().Truncate(time.Second)
				store := b12ConsumerStore(t, b12ConsumerRows("K1ABC", count, now.Add(-time.Minute)), 5, 2)
				cfg := b12ConsumerConfig()
				cfg.RecentBandRecordMinUniqueSpotters, cfg.CustomSCP.StabilizerMinUniqueSpotters = tc.common, tc.stabilizer
				s := spot.NewSpot("K1ABC", "N0SRC", 14020, "CW")
				s.Confidence = "S"
				decision := EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
				if decision.ShouldDelay != (count < tc.needed) {
					t.Fatalf("stabilizer must apply effective unique minimum %d to count %d: %+v", tc.needed, count, decision)
				}
			}
		})
	}
}

func TestB12StabilizerEditNeighborUsesOwnRequirements(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		own, neighbor, common, minEdit int
		contested                      bool
	}{
		{"below_stabilizer", 3, 2, 2, 2, false}, {"at_stabilizer", 3, 3, 2, 2, true},
		{"below_common", 4, 3, 4, 2, false}, {"at_common", 4, 4, 4, 2, true},
		{"below_edit", 3, 3, 2, 4, false}, {"at_edit", 3, 4, 2, 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			rows := append(b12ConsumerRows("K1ABC", tc.own, now.Add(-20*24*time.Hour)),
				b12ConsumerRows("K1ABD", tc.neighbor, now.Add(-20*24*time.Hour))...)
			store := b12ConsumerStore(t, rows, 10, 3)
			cfg := b12ConsumerConfig()
			cfg.CustomSCP.StabilizerMinScore = 3
			cfg.RecentBandRecordMinUniqueSpotters, cfg.StabilizerEditNeighborMinSpotters = tc.common, tc.minEdit
			cfg.StabilizerEditNeighborEnabled = true
			cfg.StabilizerEditNeighborMaxChecks = 4
			s := spot.NewSpot("K1ABC", "N0SRC", 14020, "CW")
			s.Confidence = "S"
			decision := EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
			if decision.ShouldDelay != tc.contested {
				t.Fatalf("normalized edit-neighbor queries must honor the stabilizer's own complete requirement: %+v", decision)
			}
			if tc.contested && (decision.Reason != StabilizerDelayReasonEditNeighbor || decision.MaxChecks != 4) {
				t.Fatalf("a different delay gate must not mask the edit-neighbor result: %+v", decision)
			}
		})
	}
}

func BenchmarkB12StabilizerRequirements(b *testing.B) {
	now := time.Now().UTC().Truncate(time.Second)
	rows := append(b12ConsumerRows("K1ABC", 3, now.Add(-20*24*time.Hour)),
		b12ConsumerRows("K1ABD", 3, now.Add(-20*24*time.Hour))...)
	store := b12ConsumerStore(b, rows, 10, 3)
	cfg := b12ConsumerConfig()
	cfg.CustomSCP.StabilizerMinScore = 3
	cfg.StabilizerEditNeighborEnabled = true
	cfg.StabilizerEditNeighborMinSpotters = 2
	cfg.StabilizerEditNeighborMaxChecks = 4
	s := spot.NewSpot("K1ABC", "N0SRC", 14020, "CW")
	s.Confidence = "S"
	s.EnsureNormalized()
	decision := EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
	if !decision.ShouldDelay || decision.Reason != StabilizerDelayReasonEditNeighbor || decision.MaxChecks != 4 {
		b.Fatalf("benchmark must exercise the complete normalized edit-neighbor query: %+v", decision)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decision = EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
		if !decision.ShouldDelay || decision.Reason != StabilizerDelayReasonEditNeighbor || decision.MaxChecks != 4 {
			b.Fatalf("edit-neighbor query changed during measurement: %+v", decision)
		}
	}
}
