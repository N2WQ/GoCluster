// File role: Shared, S3-compatible inputs and correctness oracles for S4's
// connected consumer campaigns. All grids use the real checked-in H3 tables;
// the fixture never chooses keys according to either implementation's shards.
package cluster

import (
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

const (
	pathShardingQueueSize          = 64
	pathShardingCampaignOps        = 1 << 20
	pathShardingReportDB           = -10
	pathShardingP50DB              = -9.5
	pathShardingApprovedExecutable = `C:\src\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`
)

var pathShardingModes = []string{"off", "shadow", "enforce"}

// Nearby endpoints intentionally retain geographic/coarse sharing. Global
// endpoints exercise other packed fields. This is a declared stress population,
// not an estimate of production traffic frequencies.
var pathShardingGrids = []string{
	"FN20", "FN31", "FN42", "EM12", "EM10", "DM04", "IO91", "JN58",
	"IN80", "KM25", "PM95", "QF56", "GF05", "FF46", "KG44", "OM89",
}

type pathShardingEndpoint struct {
	Grid   string                 `json:"grid"`
	Fine   pathreliability.CellID `json:"fine"`
	Coarse pathreliability.CellID `json:"coarse"`
}

type pathShardingPath struct {
	Receiver  pathShardingEndpoint `json:"receiver"`
	Sender    pathShardingEndpoint `json:"sender"`
	Band      string               `json:"band"`
	Frequency float64              `json:"frequency_khz"`
}

type pathShardingPhysicalKey struct {
	band             string
	receiver, sender pathreliability.CellID
	coarse           bool
}

// These are sums of public beacon-selected directional diagnostics, not a
// claim of independent samples or a replacement for the core admission oracle.
type pathShardingPublicSupport struct {
	AtUTC         time.Time `json:"at_utc"`
	RawCount      uint64    `json:"raw_count"`
	CappedCount   uint64    `json:"capped_count"`
	CappedWeight  float64   `json:"capped_weight"`
	ReceiverCount uint64    `json:"receiver_count"`
}

type pathShardingConnectedFixture struct {
	cfg       pathreliability.Config
	predictor *pathreliability.Predictor
	metrics   *pathReportMetrics
	pipeline  *outputPipeline
	pathStats pathOnlyStats
	allowed   map[string]struct{}
	paths     []pathShardingPath
	// Checker totals are merged only after the role owners have joined. They
	// never add shared cache-line traffic to the measured production work.
	counts         [][2]uint64
	completed      [4]uint64
	seedRaw        uint64
	hotBuckets     int
	metricsSeen    [2]uint64
	initialSupport pathShardingPublicSupport
	lastSupport    pathShardingPublicSupport
}

func pathShardingRepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func newPathShardingConnectedFixture(t testing.TB, mode string) *pathShardingConnectedFixture {
	t.Helper()
	requirePathReportH3Mappings(t)
	cfg, err := pathreliability.LoadFile(filepath.Join(pathShardingRepoRoot(), "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ReceiverContributionMode = mode
	if cfg.MinObservationCount != 21 || cfg.BeaconMinObservationCount != 11 || cfg.ReceiverMaxEffectiveCount != 8 || cfg.FineOnlyWeight != 20 {
		t.Fatal("shipped support/cap configuration changed; revise the frozen fixture before measurement")
	}
	f := &pathShardingConnectedFixture{cfg: cfg, metrics: newPathReportMetrics(), allowed: map[string]struct{}{"20m": {}, "40m": {}, "10m": {}}}
	f.predictor = pathreliability.NewPredictor(cfg, []string{"20m", "40m", "10m"})
	f.pipeline = &outputPipeline{pathPredictor: f.predictor, pathReport: f.metrics, allowedBands: f.allowed}
	f.paths = pathShardingHotPaths(t)
	f.counts = make([][2]uint64, len(f.paths))
	at := time.Now().UTC()
	for _, path := range f.paths {
		for _, reverse := range []bool{false, true} {
			if reverse {
				path = pathShardingReversed(path)
			}
			for receiver := 0; receiver < 12; receiver++ {
				for report := 0; report < 8; report++ {
					a := f.predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, pathShardingReportDB, 1,
						pathreliability.ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, pathreliability.ReceiverIdentityHash(pathShardingReceiver(path.Receiver, receiver)))
					if !a.Accepted() {
						t.Fatalf("warm admission failed: %+v", a)
					}
				}
			}
		}
	}
	f.seedRaw = 12 * 8
	f.hotBuckets = len(pathShardingKeySet(f.paths))
	f.guard(t)
	f.initialSupport = f.lastSupport
	return f
}

func pathShardingHotPaths(t testing.TB) []pathShardingPath {
	t.Helper()
	var endpoints []pathShardingEndpoint
	seen := make(map[pathreliability.CellID]bool)
	for _, grid := range pathShardingGrids {
		e := pathShardingEndpoint{grid, pathreliability.EncodeCell(grid), pathreliability.EncodeCoarseCell(grid)}
		if e.Fine == pathreliability.InvalidCell || e.Coarse == pathreliability.InvalidCell {
			t.Fatalf("unmapped real grid %s", grid)
		}
		if !seen[e.Fine] {
			endpoints = append(endpoints, e)
			seen[e.Fine] = true
		}
	}
	if len(endpoints) < 12 {
		t.Fatal("real endpoint population collapsed; do not substitute synthetic IDs")
	}
	var paths []pathShardingPath
	keys := make(map[pathShardingPhysicalKey]bool)
	for _, band := range []struct {
		name      string
		frequency float64
	}{{"20m", 14074}, {"40m", 7074}, {"10m", 28074}} {
		for i, receiver := range endpoints {
			path := pathShardingPath{receiver, endpoints[(i+5)%len(endpoints)], band.name, band.frequency}
			key := pathShardingPhysicalKey{path.Band, path.Receiver.Fine, path.Sender.Fine, false}
			reverse := pathShardingPhysicalKey{path.Band, path.Sender.Fine, path.Receiver.Fine, false}
			if keys[key] || keys[reverse] {
				continue
			}
			keys[key], keys[reverse] = true, true
			paths = append(paths, path)
		}
	}
	return paths
}

func pathShardingReversed(path pathShardingPath) pathShardingPath {
	path.Receiver, path.Sender = path.Sender, path.Receiver
	return path
}

func pathShardingReceiver(endpoint pathShardingEndpoint, slot int) string {
	return fmt.Sprintf("K1S%04d", int(endpoint.Fine)*64+slot)
}

func pathShardingKeySet(paths []pathShardingPath) map[pathShardingPhysicalKey]struct{} {
	keys := make(map[pathShardingPhysicalKey]struct{}, len(paths)*4)
	for _, path := range paths {
		for _, reverse := range []bool{false, true} {
			if reverse {
				path = pathShardingReversed(path)
			}
			keys[pathShardingPhysicalKey{path.Band, path.Receiver.Fine, path.Sender.Fine, false}] = struct{}{}
			keys[pathShardingPhysicalKey{path.Band, path.Receiver.Coarse, path.Sender.Coarse, true}] = struct{}{}
		}
	}
	return keys
}

// Every role uses this same frozen permutation; roles overlap the physical
// population instead of receiving disjoint private stores or shards.
func (f *pathShardingConnectedFixture) selection(sequence uint64, role int) (int, int, pathShardingPath) {
	index := int((sequence*17 + uint64(role*7)) % uint64(len(f.paths)))
	direction := int((sequence / uint64(len(f.paths))) & 1)
	path := f.paths[index]
	if direction != 0 {
		path = pathShardingReversed(path)
	}
	return index, direction, path
}

func (f *pathShardingConnectedFixture) report(sequence uint64, role int) *spot.Spot {
	_, _, path := f.selection(sequence, role)
	mode, report := "FT8", pathShardingReportDB
	if role == 1 {
		mode, report = "WSPR", int(pathShardingReportDB-f.cfg.ModeOffsets.WSPR)
	}
	// Twelve warm identities occupy slots 0..11. Each actual producer then
	// rotates its own bounded population of 24 fresh identities so the workload
	// cannot settle into same-receiver cap rejection for the entire campaign.
	slot := 12 + role*24 + int(sequence/(2*uint64(len(f.paths)))%24)
	s := spot.NewSpot("W2S4DX", pathShardingReceiver(path.Receiver, slot), path.Frequency, mode)
	s.Time, s.HasReport, s.Report = time.Now().UTC(), true, report
	s.SourceType = spot.SourceRBN
	if role == 1 {
		s.SourceType = spot.SourcePSKReporter
	}
	s.DEMetadata.Grid, s.DXMetadata.Grid = path.Receiver.Grid, path.Sender.Grid
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
	return s
}

func (f *pathShardingConnectedFixture) prediction(sequence uint64, role int) pathreliability.Result {
	_, _, path := f.selection(sequence, role)
	if role == 3 {
		return f.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, "FT8", 0, f.cfg.BeaconMinObservationCount, time.Now().UTC())
	}
	return f.predictor.Predict(path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, "FT8", 0, time.Now().UTC())
}

func (f *pathShardingConnectedFixture) valid(result pathreliability.Result, beacon bool) bool {
	return result.Source == pathreliability.SourceCombined && result.HasP50 && result.P50DB == pathShardingP50DB && result.Class == "HIGH" && result.Glyph == f.cfg.GlyphSymbols.High &&
		result.ObservedDistributionAvailable && result.HasObservedSamplingTime && result.BeaconRX == beacon && result.InsufficientReason == pathreliability.InsufficientNone &&
		result.Weight >= f.cfg.MinEffectiveWeight && !math.IsInf(result.Weight, 0) && !math.IsNaN(result.Weight)
}

func (f *pathShardingConnectedFixture) guard(t testing.TB) {
	f.guardPopulation(t, f.hotBuckets, true)
}

func (f *pathShardingConnectedFixture) guardPopulation(t testing.TB, expectedBuckets int, checkSources bool) {
	t.Helper()
	if got := f.predictor.TotalBuckets(); got != expectedBuckets {
		t.Fatalf("resident physical buckets=%d want %d", got, expectedBuckets)
	}
	support := pathShardingPublicSupport{AtUTC: time.Now().UTC()}
	for index, path := range f.paths {
		for direction := 0; direction < 2; direction++ {
			query := path
			if direction != 0 {
				query = pathShardingReversed(query)
			}
			at := time.Now().UTC()
			normal := f.predictor.Predict(query.Receiver.Fine, query.Sender.Fine, query.Receiver.Coarse, query.Sender.Coarse, query.Band, "FT8", 0, at)
			beacon := f.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(query.Receiver.Fine, query.Sender.Fine, query.Receiver.Coarse, query.Sender.Coarse, query.Band, "FT8", 0, f.cfg.BeaconMinObservationCount, at)
			wantNormal := 2*f.seedRaw + f.counts[index][0] + f.counts[index][1]
			wantBeacon := f.seedRaw + f.counts[index][direction]
			if !f.valid(normal, false) || !f.valid(beacon, true) || uint64(normal.RawCount) != wantNormal || uint64(beacon.RawCount) != wantBeacon {
				t.Fatalf("path %d direction %d lost admission/distribution: normal=%+v beacon=%+v want raw=%d/%d", index, direction, normal, beacon, wantNormal, wantBeacon)
			}
			support.RawCount += uint64(beacon.RawCount)
			support.CappedCount += uint64(beacon.CappedCount)
			support.CappedWeight += beacon.CappedWeight
			support.ReceiverCount += uint64(beacon.ReceiverCount)
		}
	}
	f.lastSupport = support
	if f.cfg.ReceiverContributionMode != pathreliability.ReceiverContributionOff && f.completed[0]+f.completed[1] > 0 && support.CappedWeight <= f.initialSupport.CappedWeight {
		t.Fatalf("producer turnover did not add public capped signal mass beyond the warm seed: before=%+v after=%+v", f.initialSupport, support)
	}
	if a := f.predictor.AdmissionStats(); a.ClampedFuture != 0 || a.RejectedTime != 0 || a.RejectedWeight != 0 {
		t.Fatalf("unexpected input admission: %+v", a)
	}
	if p := snapshotPathOnly(&f.pathStats); p.updates != f.completed[1] || p.drops != 0 {
		t.Fatalf("path-only population/route mismatch: %+v completed=%d", p, f.completed[1])
	}
	if checkSources {
		counts := f.metrics.SnapshotSources()
		for role, label := range []string{"RBN", "PSK"} {
			total := f.completed[role]
			if counts[label] != total-f.metricsSeen[role] {
				t.Fatalf("%s source population=%d want %d", label, counts[label], total-f.metricsSeen[role])
			}
			f.metricsSeen[role] = total
		}
	}
}

func (f *pathShardingConnectedFixture) channelWitness(t testing.TB) {
	t.Helper()
	const reports = 64
	input := make(chan *spot.Spot, reports)
	base := f.completed[1]
	for i := 0; i < reports; i++ {
		s := f.report(base+uint64(i), 1)
		input <- s
	}
	close(input)
	processPSKRPathOnlyChannel(input, f.predictor, f.metrics, &f.pathStats, config.SpotPolicy{}, f.allowed)
	// This unchanged channel owner has returned; only now merge its actual
	// completed population into checker state. The guard checks production
	// admissions and source accounting against these expected sequence totals.
	for i := 0; i < reports; i++ {
		index, direction, _ := f.selection(base+uint64(i), 1)
		f.counts[index][direction]++
	}
	f.completed[1] += reports
	f.guard(t)
}
