package pathreliability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const pathShardCampaignOperations = 1_000_000

var pathShardGridCorpus = []string{
	"FN31", "FN32", "FN30", "FM18", "EM12", "EM73", "DM79", "CM87",
	"CN87", "BL11", "FK68", "GG66", "GF05", "FF46", "IO91", "JO01",
	"JO31", "JN58", "JN49", "KM17", "KN34", "KO85", "LL55", "MM89",
	"MK82", "OM89", "PM95", "PM86", "PF94", "QF56", "RE78", "RF80",
}

var pathShardBands = []string{"40m", "20m", "10m"}

type pathShardEndpoint struct {
	Grid   string
	Fine   CellID
	Coarse CellID
}

type pathShardPath struct {
	Receiver pathShardEndpoint
	Sender   pathShardEndpoint
	Band     string
	BandID   uint16
}

type pathShardFixture struct {
	predictor           *Predictor
	cfg                 Config
	query               time.Time
	endpoints           []pathShardEndpoint
	queries             []pathShardPath
	reports             []pathShardPath
	coldReports         []pathShardPath
	hotKeys             map[uint64]bool
	allKeys             map[uint64]bool
	wantCounts          map[uint64]uint64
	hotPointers         map[uint64]*bucket
	inputHash           string
	configHash          string
	h3Hashes            [2]string
	seedReports         int
	maintenance         bool
	maintenanceRun      pathShardMaintenanceResult
	maintenanceEvidence pathShardMaintenanceEvidence
}

func newPathShardFixture(t testing.TB, mode string, maintenance bool) *pathShardFixture {
	t.Helper()
	configBytes, err := os.ReadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinObservationCount != 21 || cfg.BeaconMinObservationCount != 11 || cfg.MinEffectiveWeight != 0.5 || cfg.ReceiverFineSlots != 6 || cfg.ReceiverCoarseSlots != 12 || cfg.ReceiverMaxEffectiveCount != 8 {
		t.Fatal("shipped qualification policy changed; revise the frozen fixture before measuring")
	}
	cfg.ReceiverContributionMode = mode
	if err := InitH3MappingsFromDir(filepath.Join("..", "data", "h3")); err != nil {
		t.Fatal(err)
	}
	if fineMapper == nil || coarseMapper == nil || fineMapper.Res != 2 || coarseMapper.Res != 1 || len(fineMapper.ToID) != 5882 || len(coarseMapper.ToID) != 842 {
		t.Fatal("active H3 backend does not have the complete checked-in resolution tables")
	}
	f := &pathShardFixture{
		predictor: NewPredictor(cfg, cfg.AllowedBands), cfg: cfg,
		query:   time.Date(2026, 10, 14, 20, 0, 0, 0, time.UTC),
		hotKeys: make(map[uint64]bool), allKeys: make(map[uint64]bool),
		wantCounts: make(map[uint64]uint64), maintenance: maintenance,
	}
	configSum := sha256.Sum256(configBytes)
	f.configHash = hex.EncodeToString(configSum[:])
	for i, table := range []string{"res2.bin", "res1.bin"} {
		contents, err := os.ReadFile(filepath.Join("..", "data", "h3", table))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(contents)
		f.h3Hashes[i] = hex.EncodeToString(sum[:])
	}
	for _, grid := range pathShardGridCorpus {
		point := pathShardEndpoint{Grid: grid, Fine: EncodeCell(grid), Coarse: EncodeCoarseCell(grid)}
		if point.Fine == InvalidCell || point.Coarse == InvalidCell {
			t.Fatalf("checked-in H3 maps did not encode fixed grid %s: %+v", grid, point)
		}
		f.endpoints = append(f.endpoints, point)
	}
	for _, band := range pathShardBands {
		bandID, ok := f.predictor.combined.bandIndex.Lookup(band)
		if !ok {
			t.Fatalf("frozen workload band %s absent from shipped config", band)
		}
		for i, point := range f.endpoints {
			receiver := point
			if i%4 == 0 {
				receiver = f.endpoints[0]
			}
			path := pathShardPath{Receiver: receiver, Sender: f.endpoints[(i+7)%len(f.endpoints)], Band: band, BandID: bandID}
			f.queries = append(f.queries, path)
			f.reports = append(f.reports, path, path.reversed())
			for _, report := range []pathShardPath{path, path.reversed()} {
				for _, key := range report.keys() {
					f.hotKeys[key] = true
				}
			}
		}
		for _, receiver := range f.endpoints {
			for _, sender := range f.endpoints {
				f.coldReports = append(f.coldReports, pathShardPath{Receiver: receiver, Sender: sender, Band: band, BandID: bandID})
			}
		}
	}
	if maintenance {
		f.seedCold(t)
	}
	seedAt := f.query.Add(-60 * time.Second)
	for _, path := range f.reports {
		for report := 0; report < 48; report++ {
			hash := uint64(0x7200000000000000) | uint64(path.Receiver.Fine)<<8 | uint64(report%6+1)
			f.seedReport(t, path, ObservationTimes{ObservedAt: seedAt, ArrivedAt: seedAt}, hash)
		}
	}
	for key := range f.hotKeys {
		f.allKeys[key] = true
	}
	f.hotPointers = f.capturePointers(f.hotKeys)
	if len(f.hotPointers) != len(f.hotKeys) {
		t.Fatal("hot corpus did not materialize every expected physical key")
	}
	if maintenance && (len(f.allKeys) < 1000 || len(f.hotKeys)*2 >= len(f.allKeys)) {
		t.Fatalf("frozen real-grid corpus cannot exercise substantial contraction: hot=%d grown=%d", len(f.hotKeys), len(f.allKeys))
	}
	identity := struct {
		ConfigHash string
		H3Hashes   [2]string
		Endpoints  []pathShardEndpoint
		Queries    []pathShardPath
		Reports    []pathShardPath
		QueryUnix  int64
		Identities int
	}{f.configHash, f.h3Hashes, f.endpoints, f.queries, f.reports, f.query.Unix(), 24}
	identityBytes, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	inputSum := sha256.Sum256(identityBytes)
	f.inputHash = hex.EncodeToString(inputSum[:])
	f.assertPredictions(t)
	f.assertState(t, f.allKeys)
	return f
}

func (p pathShardPath) reversed() pathShardPath {
	p.Receiver, p.Sender = p.Sender, p.Receiver
	return p
}

// Identity arithmetic is deliberately independent of any implementation shard
// selector. Fine/coarse packing is the unchanged contract under measurement.
func (p pathShardPath) keys() [2]uint64 {
	return [2]uint64{
		uint64(p.BandID)<<48 | uint64(p.Receiver.Fine)<<32 | uint64(p.Sender.Fine)<<16,
		uint64(p.BandID)<<32 | uint64(p.Receiver.Coarse)<<16 | uint64(p.Sender.Coarse),
	}
}

func (f *pathShardFixture) seedReport(t testing.TB, path pathShardPath, times ObservationTimes, hash uint64) {
	t.Helper()
	if outcome := f.predictor.UpdateWithReceiverHash(BucketCombined, path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, -10.5, 1, times, false, hash); !outcome.Accepted() {
		t.Fatalf("valid fixture report rejected: %+v", outcome)
	}
	f.seedReports++
	for _, key := range path.keys() {
		f.wantCounts[key]++
		f.allKeys[key] = true
	}
}

func (f *pathShardFixture) seedCold(t testing.TB) {
	t.Helper()
	coldAt := f.query.Add(-7201 * time.Second)
	for _, path := range f.coldReports {
		f.seedReport(t, path, ObservationTimes{ObservedAt: coldAt, ArrivedAt: coldAt}, 0x7100000000000000|uint64(path.Receiver.Fine))
	}
}

func (f *pathShardFixture) reportFor(role, iteration int) pathShardPath {
	if role == 0 {
		return f.reports[iteration*13%len(f.reports)]
	}
	return f.reports[(iteration*17+97)%len(f.reports)]
}

func (f *pathShardFixture) update(role, iteration, operations int) bool {
	path := f.reportFor(role, iteration)
	arrived := f.query.Add(time.Duration(-60+iteration*60/operations) * time.Second)
	identity := uint64(iteration/len(f.reports)%24 + 1)
	hash := uint64(0x7300000000000000) | uint64(role)<<48 | uint64(path.Receiver.Fine)<<8 | identity
	return f.predictor.UpdateWithReceiverHash(BucketCombined, path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, -10.5, 1,
		ObservationTimes{ObservedAt: arrived.Add(-30 * time.Second), ArrivedAt: arrived}, false, hash).Accepted()
}

func (f *pathShardFixture) read(role, iteration int) Result {
	path := f.queries[(iteration*11+role*19)%len(f.queries)]
	if role == 2 {
		return f.predictor.Predict(path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, "FT8", 0, f.query)
	}
	return f.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, "FT8", 0, f.cfg.BeaconMinObservationCount, f.query)
}

func pathShardUsable(result Result, beacon bool) bool {
	return result.Source == SourceCombined && result.HasP50 && result.ObservedDistributionAvailable && result.P50DB == -10.5 && result.Glyph == ">" && result.BeaconRX == beacon && result.InsufficientReason == InsufficientNone
}

func (f *pathShardFixture) accountWork(operations int) {
	for role := 0; role < 2; role++ {
		for iteration := 0; iteration < operations; iteration++ {
			for _, key := range f.reportFor(role, iteration).keys() {
				f.wantCounts[key]++
			}
		}
	}
}

func (f *pathShardFixture) capturePointers(keys map[uint64]bool) map[uint64]*bucket {
	pointers := make(map[uint64]*bucket, len(keys))
	for i := range f.predictor.combined.shards {
		sh := &f.predictor.combined.shards[i]
		sh.mu.RLock()
		for key, value := range sh.buckets {
			if keys[key] {
				pointers[key] = value
			}
		}
		sh.mu.RUnlock()
	}
	return pointers
}

func (f *pathShardFixture) assertPredictions(t testing.TB) {
	t.Helper()
	for role := 2; role < 4; role++ {
		for i := range f.queries {
			if result := f.read(role, i); !pathShardUsable(result, role == 3) {
				t.Fatalf("role %d query %d must remain observed HIGH at -10.5 dB: %+v", role, i, result)
			}
		}
	}
}

func (f *pathShardFixture) assertState(t testing.TB, want map[uint64]bool) {
	t.Helper()
	seen := make(map[uint64]bool, len(want))
	for i := range f.predictor.combined.shards {
		sh := &f.predictor.combined.shards[i]
		sh.mu.RLock()
		for key, value := range sh.buckets {
			if !want[key] || seen[key] || value == nil || uint64(value.count) != f.wantCounts[key] {
				sh.mu.RUnlock()
				t.Fatalf("physical key %x missing, duplicated, unexpected, nil or wrong lifetime count", key)
			}
			if original := f.hotPointers[key]; original != nil && original != value {
				sh.mu.RUnlock()
				t.Fatalf("maintenance replaced live bucket %x", key)
			}
			seen[key] = true
		}
		sh.mu.RUnlock()
	}
	if len(seen) != len(want) {
		t.Fatalf("resident physical keys=%d, want %d", len(seen), len(want))
	}
	if stats := f.predictor.AdmissionStats(); stats != (AdmissionStats{}) {
		t.Fatalf("ordinary fixture input admitted through exceptional path: %+v", stats)
	}
}

func (f *pathShardFixture) signalTotals() (raw, capped, cappedCount float64) {
	for i := range f.predictor.combined.shards {
		sh := &f.predictor.combined.shards[i]
		sh.mu.RLock()
		for key, value := range sh.buckets {
			if f.hotKeys[key] {
				raw += value.weight
				capped += value.cappedWeight
				cappedCount += value.cappedCount
			}
		}
		sh.mu.RUnlock()
	}
	return
}

func (f *pathShardFixture) lifetimeRawCount() uint64 {
	var count uint64
	for i := range f.predictor.combined.shards {
		sh := &f.predictor.combined.shards[i]
		sh.mu.RLock()
		for key, value := range sh.buckets {
			if f.hotKeys[key] {
				count += uint64(value.count)
			}
		}
		sh.mu.RUnlock()
	}
	return count
}

func (f *pathShardFixture) assertActiveAdmission(t testing.TB, operations int, beforeRaw, beforeCapped, beforeCount float64) {
	t.Helper()
	raw, capped, count := f.signalTotals()
	minimumNewUnits := float64(4*operations) * 0.05
	if raw < beforeRaw*0.7+minimumNewUnits {
		t.Fatalf("raw usable mass failed active-admission witness: before=%g after=%g minimum-new=%g", beforeRaw, raw, minimumNewUnits)
	}
	if f.cfg.ReceiverContributionMode != ReceiverContributionOff && (capped < beforeCapped*0.7+minimumNewUnits || count < beforeCount*0.7+minimumNewUnits) {
		t.Fatalf("cap work settled into rejection: mass=%g→%g count=%g→%g minimum-new=%g", beforeCapped, capped, beforeCount, count, minimumNewUnits)
	}
}

type pathShardMassState struct {
	raw, capped, count float64
	reference          int64
	halfLife           int
}

type pathShardAddedMass struct{ raw, capped, count float64 }

func (f *pathShardFixture) captureMassStates() map[uint64]pathShardMassState {
	states := make(map[uint64]pathShardMassState, len(f.hotKeys))
	for i := range f.predictor.combined.shards {
		sh := &f.predictor.combined.shards[i]
		sh.mu.RLock()
		for key, value := range sh.buckets {
			if !f.hotKeys[key] {
				continue
			}
			bandID := uint16(key >> 48)
			if key&0xffff != 0 {
				bandID = uint16(key >> 32)
			}
			band := f.predictor.combined.bandIndex.idxToBand[bandID]
			halfLife := f.cfg.DefaultHalfLifeSec
			if override := f.cfg.BandHalfLifeSec[band]; override > 0 {
				halfLife = override
			}
			states[key] = pathShardMassState{value.weight, value.cappedWeight, value.cappedCount, value.lastUpdate, halfLife}
		}
		sh.mu.RUnlock()
	}
	return states
}

// Subtract each seed bucket at its final monotonic accounting reference. Raw
// signal, admitted capped signal and admitted report units stay separate.
func pathShardNewMass(before, after map[uint64]pathShardMassState) (pathShardAddedMass, bool) {
	var added pathShardAddedMass
	if len(before) != len(after) {
		return added, false
	}
	for key, seed := range before {
		final, ok := after[key]
		if !ok || seed.halfLife <= 0 || final.reference < seed.reference || final.halfLife != seed.halfLife {
			return added, false
		}
		factor := math.Exp2(-float64(final.reference-seed.reference) / float64(seed.halfLife))
		added.raw += final.raw - seed.raw*factor
		added.capped += final.capped - seed.capped*factor
		added.count += final.count - seed.count*factor
	}
	return added, true
}

func pathShardHasNewMass(added pathShardAddedMass, actualCalls int, mode string) bool {
	minimum := float64(2*actualCalls) * 0.05
	if actualCalls <= 0 || !(added.raw >= minimum) {
		return false
	}
	return mode == ReceiverContributionOff || added.capped >= minimum && added.count >= minimum
}

func pathShardError(format string, args ...any) error { return fmt.Errorf(format, args...) }
