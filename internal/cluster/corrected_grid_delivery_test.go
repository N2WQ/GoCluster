//go:build !qualification

// File role: Falsifies corrected-call grid retention and cache provenance using
// real resolver/scheduler output and a reopened grid store, not grid text alone.
package cluster

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/gridstore"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/telnet"
)

type correctedGridCase struct {
	name, target, want                     string
	oldDerived, targetDerived, wantDerived bool
	percent                                int
	ctySameGrid, invalidCTY, rotateCTY     bool
}

func correctedGridCTY(t testing.TB) *cty.CTYDatabase {
	return correctedGridCTYAt(t, 48, -11) // JN58; CTY longitude is west-positive.
}

func correctedGridCTYAt(t testing.TB, latitude, westLongitude float64) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(fmt.Sprintf(`<plist><dict>
<key>K1ABC</key><dict><key>Country</key><string>Alpha</string><key>ADIF</key><integer>230</integer><key>Continent</key><string>EU</string><key>CQZone</key><integer>1</integer><key>ITUZone</key><integer>2</integer></dict>
<key>K1ABD</key><dict><key>Country</key><string>Beta</string><key>ADIF</key><integer>281</integer><key>Continent</key><string>NA</string><key>CQZone</key><integer>9</integer><key>ITUZone</key><integer>15</integer><key>Latitude</key><real>%g</real><key>Longitude</key><real>%g</real></dict>
<key>N0AAA</key><dict><key>Country</key><string>Alpha</string><key>ADIF</key><integer>230</integer><key>Continent</key><string>EU</string><key>CQZone</key><integer>1</integer><key>ITUZone</key><integer>2</integer></dict>
</dict></plist>`, latitude, westLongitude)))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func correctedGridSource(at time.Time, derived bool) *spot.Spot {
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.SourceType, s.SourceNode = spot.SourcePSKReporter, "PSKREPORTER"
	s.DXMetadata = spot.CallMetadata{Country: "Alpha", ADIF: 230, Continent: "EU", CQZone: 1, ITUZone: 2, Grid: "FN31", GridDerived: derived}
	s.DEMetadata = spot.CallMetadata{Country: "Alpha", ADIF: 230, Continent: "EU", CQZone: 1, ITUZone: 2, Grid: "FN20", State: "AA"}
	s.InvalidateMetadataCache()
	hydrateSpotPathCells(s)
	return s
}

func TestCorrectedGridPolicyAndWriteback(t *testing.T) {
	requirePathReportH3Mappings(t)
	cases := []correctedGridCase{
		{name: "different_reported", target: "QF56", want: "QF56"},
		{name: "different_derived", target: "EM12", targetDerived: true, want: "JN58", wantDerived: true},
		{name: "same_target_derived", target: "FN31", targetDerived: true, want: "FN31", wantDerived: true, ctySameGrid: true},
		{name: "same_target_reported", target: "FN31", oldDerived: true, want: "FN31"},
		{name: "full_locator_precision", target: "FN31AB", want: "FN31AB"},
		{name: "miss_reported"},
		{name: "miss_derived", oldDerived: true},
		{name: "invalid_current_cty", target: "EM12", targetDerived: true, invalidCTY: true},
		{name: "captured_snapshot", target: "EM12", targetDerived: true, want: "JN58", wantDerived: true, rotateCTY: true},
	}
	for _, delayed := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("delayed_%t/%s", delayed, tc.name), func(t *testing.T) { runCorrectedGridCase(t, tc, delayed) })
		}
	}
}

func runCorrectedGridCase(t *testing.T, tc correctedGridCase, delayed bool) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Second)
	r, d, key := correctionLossResolver(t, 4, 8)
	if !delayed {
		correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at.Add(-time.Second), 2*time.Minute, "N0BBB", "N0CCC", "N0FFF")
	}
	path := t.TempDir()
	store, err := gridstore.Open(path, gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(64, 0)
	if tc.target != "" {
		rec := gridstore.Record{Call: "K1ABD", Grid: sql.NullString{String: tc.target, Valid: true}, GridDerived: tc.targetDerived,
			Observations: 1, FirstSeen: at, UpdatedAt: at}
		if err := store.Upsert(rec); err != nil {
			t.Fatal(err)
		}
		cache.ApplyRecord(rec)
	}
	update, _, _, stop, lookup, syncLookup := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, nil)
	closed := false
	t.Cleanup(func() {
		stop()
		if !closed {
			_ = store.Close()
		}
	})
	archiveCfg := config.ArchiveConfig{DBPath: t.TempDir(), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1}
	writer, err := archive.NewWriter(archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	writer.Start()
	t.Cleanup(writer.Stop)
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), writer, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil))
	db := correctedGridCTY(t)
	if tc.ctySameGrid {
		db = correctedGridCTYAt(t, 41, 73) // FN31, equal text but a new derived origin.
	}
	if tc.invalidCTY {
		db = correctedGridCTYAt(t, 91, -11)
	}
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	p.correctionCfg = correctionLossConfig()
	p.signalResolver = r
	p.correctionCfg.StabilizerEnabled, p.correctionCfg.StabilizerMaxChecks = true, 1
	p.gridLookup, p.gridLookupSync = lookup, syncLookup
	if tc.rotateCTY {
		next := correctedGridCTYAt(t, 41, 73)
		// Finish fixture B before publishing it; runtime CTY images stay immutable.
		info := next.Data["K1ABD"]
		info.Country, info.ADIF, info.CQZone = "Gamma", 282, 10
		next.Data["K1ABD"] = info
		p.gridLookup = func(call string) (string, bool, bool) { db = next; return lookup(call) }
		p.gridLookupSync = func(call string) (string, bool, bool) { db = next; return syncLookup(call) }
	}
	var updates []string
	p.gridUpdate = func(call, grid string) { updates = append(updates, call); update(call, grid) }
	s := correctedGridSource(at, tc.oldDerived)
	if s.DXGridNorm != "FN31" || s.DXGrid2 != "FN" || s.DXCellID == 0 || s.DXCoarseCellID == 0 || s.DECellID == 0 || s.DECoarseCellID == 0 {
		t.Fatal("positive control: source geographic caches were not populated")
	}
	if delayed {
		p.stabilizerEnabled = true
		p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 2)
		p.telnetStabilizer.Start()
		t.Cleanup(p.telnetStabilizer.Stop)
		p.gridLookupSync = func(string) (string, bool, bool) {
			t.Error("release waited on synchronous lookup")
			return "", false, false
		}
		p.processSpot(s, nil)
		if p.buf.GetCount() != 0 || p.telnetStabilizer.Pending() != 1 {
			t.Fatal("initial source did not enter actual scheduler")
		}
		envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
		seedCorrectedGridWinner(t, r, d, key)
		p.handleStabilizerRelease(envelope)
		tc.percent = 75 // Three replacement receivers versus one original receiver.
	} else {
		p.processSpot(s, nil)
		tc.percent = 100 // Only three replacement receivers have been published.
	}
	assertCorrectedGridOutput(t, p, writer, archiveCfg, tc, at)
	if len(updates) != 1 || updates[0] != "N0AAA" {
		t.Fatalf("corrected DX became a grid observation: writes=%v", updates)
	}
	stop()
	closed = true
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := gridstore.Open(path, gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertCorrectedGridPersistence(t, reopened, cache, tc)
}

func assertCorrectedGridOutput(t *testing.T, p *outputPipeline, writer *archive.Writer, archiveCfg config.ArchiveConfig, tc correctedGridCase, at time.Time) {
	t.Helper()
	rows := p.buf.GetRecent(2)
	if len(rows) != 1 {
		t.Fatalf("final ring cardinality=%d", len(rows))
	}
	out := rows[0]
	if out.DXCallNorm != "K1ABD" || out.Confidence != "C" || !out.ConfidencePercentOK || out.ConfidencePercent != tc.percent ||
		out.DXMetadata.Grid != tc.want || out.DXMetadata.GridDerived != tc.wantDerived || out.DXMetadata.Country != "Beta" ||
		out.DXMetadata.ADIF != 281 || out.DXMetadata.CQZone != 9 || out.DXMetadata.ITUZone != 15 || out.DXMetadata.State != "" {
		t.Fatalf("replacement output/classification/provenance mismatch: %+v", out)
	}
	wantPrefix := ""
	if len(tc.want) >= 2 {
		wantPrefix = tc.want[:2]
	}
	if out.DXGridNorm != tc.want || out.DXGrid2 != wantPrefix || out.DXCellID != uint16(pathreliability.EncodeCell(tc.want)) ||
		out.DXCoarseCellID != uint16(pathreliability.EncodeCoarseCell(tc.want)) || out.DEMetadata.Grid != "FN20" || out.DEMetadata.State != "AA" ||
		out.DECellID != uint16(pathreliability.EncodeCell("FN20")) || out.DECoarseCellID != uint16(pathreliability.EncodeCoarseCell("FN20")) ||
		out.DEGridNorm != "FN20" || out.DEGrid2 != "FN" || !out.Time.Equal(at) || out.SourceType != spot.SourcePSKReporter {
		t.Fatalf("location caches or unrelated source facts disagree: %+v", out)
	}
	if broadcast, ok := tryReadTelnetBroadcastSpot(p.telnet); !ok || broadcast != out {
		t.Fatal("local broadcast did not use corrected immutable snapshot")
	}
	writer.Stop()
	reopened, err := archive.NewWriter(archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	stored, err := reopened.Recent(2)
	if err != nil || len(stored) != 1 || stored[0].DXCallNorm != "K1ABD" || stored[0].DXMetadata.Grid != tc.want || stored[0].DXMetadata.GridDerived != tc.wantDerived {
		t.Fatalf("reopened local archive lost final grid contract: %+v/%v", stored, err)
	}
}

func assertCorrectedGridPersistence(t *testing.T, store *gridstore.Store, cache *callMetaCache, tc correctedGridCase) {
	t.Helper()
	grid, derived, ok := cache.LookupGrid("K1ABD", nil)
	if tc.target == "" {
		if ok {
			t.Fatalf("inherited miss grid poisoned target cache: %s/%t", grid, derived)
		}
	} else if !ok || grid != tc.target || derived != tc.targetDerived {
		t.Fatal("target cache provenance was promoted using the inherited flag")
	}
	rec, err := store.Get("K1ABD")
	if err != nil || tc.target == "" && rec != nil || tc.target != "" && (rec == nil || rec.Grid.String != tc.target || rec.GridDerived != tc.targetDerived) {
		t.Fatalf("target durable provenance changed: %+v/%v", rec, err)
	}
	if de, err := store.Get("N0AAA"); err != nil || de == nil || de.Grid.String != "FN20" || de.GridDerived {
		t.Fatal("DX write suppression also suppressed DE observation")
	}
}

func TestCorrectedGridUnchangedIdentityAndMissingCTY(t *testing.T) {
	requirePathReportH3Mappings(t)
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted_%t", accepted), func(t *testing.T) {
			at := time.Now().UTC().Truncate(time.Second)
			r, d, key := correctionLossResolver(t, 4, 8)
			correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at.Add(-time.Second), time.Minute, "N0BBB", "N0CCC", "N0FFF")
			p := newDeliveryTestPipeline(buffer.NewRingBuffer(2), nil, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 2}, nil))
			p.signalResolver, p.correctionCfg = r, correctionLossConfig()
			if !accepted {
				p.correctionCfg.MinConsensusReports = 100
				db := correctedGridCTY(t)
				p.ctyLookup = func() *cty.CTYDatabase { return db }
			}
			p.gridLookup = func(string) (string, bool, bool) {
				if !accepted {
					t.Error("existing unchanged grid was looked up again")
				}
				return "", false, false
			}
			var writes []string
			p.gridUpdate = func(call, _ string) { writes = append(writes, call) }
			p.processSpot(correctedGridSource(at, false), nil)
			out := p.buf.GetRecent(1)
			want := "FN31"
			if accepted {
				want = ""
			}
			if len(out) != 1 || out[0].DXMetadata.Grid != want {
				t.Fatal("lookup miss/rejected replacement violated grid ownership")
			}
			if accepted {
				if out[0].DXCallNorm != "K1ABD" || out[0].DXMetadata.Country != "" || out[0].DXMetadata.ADIF != 0 || len(writes) != 1 || writes[0] != "N0AAA" {
					t.Fatal("missing CTY retained old classification or taught inherited DX grid")
				}
			} else if out[0].DXCallNorm != "K1ABC" || out[0].DXMetadata.Country != "Alpha" || len(writes) != 2 || writes[0] != "K1ABC" {
				t.Fatal("rejected replacement changed identity/classification or disabled normal DX observations")
			}
		})
	}
}

func TestCorrectedGridDelayedLookupDoesNotWaitForWorker(t *testing.T) {
	requirePathReportH3Mappings(t)
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(64, 0)
	cache.UpdateGrid("K2XYZ", "QF56", false)
	workerEntered, releaseWorker := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
	workerDB := gridRetirementCTY(t)
	update, _, _, stop, lookup, syncLookup := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, func() *cty.CTYDatabase {
		enterOnce.Do(func() { close(workerEntered) })
		<-releaseWorker
		return workerDB
	})
	t.Cleanup(func() { unblock(); stop(); _ = store.Close() })
	p, envelope := correctedGridHeldInput(t, nil)
	p.gridLookup, p.gridUpdate = lookup, update
	var syncCalls atomic.Int64
	p.gridLookupSync = func(call string) (string, bool, bool) {
		if call == "K1ABD" {
			t.Error("delayed target used worker-waiting synchronous lookup")
		}
		syncCalls.Add(1)
		return syncLookup(call)
	}
	delivered := make(chan struct{})
	go func() { p.handleStabilizerRelease(envelope); close(delivered) }()
	t.Cleanup(func() { unblock(); waitGridRetirement(t, delivered) })
	waitGridRetirement(t, workerEntered)
	waitGridRetirement(t, delivered) // Must finish while the real lookup worker is blocked.
	first := p.buf.GetRecent(1)
	if len(first) != 1 || first[0].DXCallNorm != "K1ABD" || first[0].DXMetadata.Grid != "" || syncCalls.Load() != 0 {
		t.Fatal("cold delayed miss waited, inherited source grid, or failed actual correction")
	}
	mainDone := make(chan struct{})
	go func() {
		self := newDeliveryTestSpot("K2XYZ", "K2XYZ", time.Now().UTC())
		self.SourceType, self.IsHuman = spot.SourceManual, true
		p.processSpot(self, nil)
		close(mainDone)
	}()
	t.Cleanup(func() { unblock(); waitGridRetirement(t, mainDone) })
	waitGridRetirement(t, mainDone)
	if p.buf.GetCount() != 2 || syncCalls.Load() == 0 {
		t.Fatal("unrelated main output failed cached lookup while release backfill was blocked")
	}
	unblock()
	stop()
	if grid, derived, ok := cache.LookupGrid("K1ABD", nil); !ok || grid != "FN20" || !derived {
		t.Fatalf("late owned backfill did not complete: %s/%t/%t", grid, derived, ok)
	}
	if first[0].DXMetadata.Grid != "" || first[0].DXMetadata.GridDerived {
		t.Fatal("late lookup mutated already sealed output")
	}
}

func TestCorrectedGridMainLookupTimeoutDoesNotUseCTY(t *testing.T) {
	requirePathReportH3Mappings(t)
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(64, 0)
	workerEntered, releaseWorker := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
	workerDB := gridRetirementCTY(t)
	_, _, _, stop, lookup, syncLookup := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, func() *cty.CTYDatabase {
		enterOnce.Do(func() { close(workerEntered) })
		<-releaseWorker
		return workerDB
	})
	t.Cleanup(func() { unblock(); stop(); _ = store.Close() })
	at := time.Now().UTC().Truncate(time.Second)
	r, d, key := correctionLossResolver(t, 4, 8)
	correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at.Add(-time.Second), time.Minute, "N0BBB", "N0CCC", "N0FFF")
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(2), nil, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 2}, nil))
	db := correctedGridCTY(t) // Independent valid pipeline image; only the worker is blocked.
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	p.signalResolver, p.correctionCfg = r, correctionLossConfig()
	p.gridLookup = lookup
	var targetSyncCalls atomic.Int64
	p.gridLookupSync = func(call string) (string, bool, bool) {
		if call == "K1ABD" {
			targetSyncCalls.Add(1)
		}
		return syncLookup(call)
	}
	delivered := make(chan struct{})
	go func() { defer close(delivered); p.processSpot(correctedGridSource(at, false), nil) }()
	t.Cleanup(func() { unblock(); waitGridRetirement(t, delivered) })
	waitGridRetirement(t, workerEntered)
	waitGridRetirement(t, delivered) // The existing synchronous deadline must expire first.
	rows := p.buf.GetRecent(1)
	if len(rows) != 1 || rows[0].DXCallNorm != "K1ABD" || rows[0].Confidence != "C" || targetSyncCalls.Load() != 1 ||
		rows[0].DXMetadata.Grid != "" || rows[0].DXMetadata.GridDerived || rows[0].DXGridNorm != "" || rows[0].DXGrid2 != "" ||
		rows[0].DXCellID != 0 || rows[0].DXCoarseCellID != 0 || rows[0].DEMetadata.Grid != "FN20" || rows[0].DECellID == 0 {
		t.Fatal("real main timeout used CTY, retained old location, or failed correction")
	}
	unblock()
	stop()
	if grid, derived, ok := cache.LookupGrid("K1ABD", nil); !ok || grid != "FN20" || !derived {
		t.Fatalf("owned late backfill did not complete: %s/%t/%t", grid, derived, ok)
	}
	if rows[0].DXMetadata.Grid != "" || rows[0].DXMetadata.GridDerived {
		t.Fatal("late worker mutated sealed timeout output")
	}
}

func TestCorrectedGridDerivedLookupWithoutCTYClearsLocation(t *testing.T) {
	requirePathReportH3Mappings(t)
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(2), nil, nil)
	p.gridLookup = func(string) (string, bool, bool) { return "EM12", true, true }
	s := correctedGridSource(time.Now().UTC(), false)
	if s.DXCellID == 0 || s.DECellID == 0 {
		t.Fatal("positive control: source cells missing")
	}
	p.prepareOutputLocation(&outputSpotContext{spot: s, dxIdentityChanged: true}, false)
	if s.DXMetadata.Grid != "" || s.DXMetadata.GridDerived || s.DXGridNorm != "" || s.DXGrid2 != "" ||
		s.DXCellID != 0 || s.DXCoarseCellID != 0 || s.DEMetadata.Grid != "FN20" || s.DECellID == 0 || s.DECoarseCellID == 0 {
		t.Fatal("unusable CTY retained location or damaged DE")
	}
}

func TestCorrectedGridDelayedDoesNotRepeatPathLearning(t *testing.T) {
	requirePathReportH3Mappings(t)
	predictor, reports := pathTimestampPredictor(), newPathReportMetrics()
	p, envelope := correctedGridHeldInput(t, func(p *outputPipeline, s *spot.Spot) {
		p.pathPredictor, p.pathReport = predictor, reports
		p.allowedBands = pathTimestampAllowedBands()
		s.HasReport, s.Report = true, -12
	})
	if n := reports.SnapshotSources()["PSK"]; n != 1 {
		t.Fatalf("positive control: initial ingress learned %d reports, want 1", n)
	}
	original := envelope.spot.Clone()
	if result := pathTimestampPrediction(predictor, original, time.Now().UTC()); result.RawCount != 1 {
		t.Fatalf("positive control: original path was not learned once: %+v", result)
	}
	p.gridLookup = func(string) (string, bool, bool) { return "JN58", false, true }
	p.handleStabilizerRelease(envelope)
	recent := p.buf.GetRecent(1)
	if len(recent) != 1 || recent[0].DXCallNorm != "K1ABD" || recent[0].DXMetadata.Grid != "JN58" {
		t.Fatal("delayed correction did not rebuild the new location")
	}
	if n := reports.SnapshotSources()["PSK"]; n != 0 {
		t.Fatalf("delayed location preparation repeated %d path reports", n)
	}
	if result := pathTimestampPrediction(predictor, original, time.Now().UTC()); result.RawCount != 1 {
		t.Fatalf("correction changed original path learning: %+v", result)
	}
	if result := pathTimestampPrediction(predictor, recent[0], time.Now().UTC()); result.RawCount != 0 {
		t.Fatalf("delayed correction learned a second path: %+v", result)
	}
}

func correctedGridHeldInput(t *testing.T, configure func(*outputPipeline, *spot.Spot)) (*outputPipeline, *telnetStabilizerEnvelope) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Second)
	r, d, key := correctionLossResolver(t, 4, 8)
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil))
	db := correctedGridCTY(t)
	p.ctyLookup = func() *cty.CTYDatabase { return db }
	p.correctionCfg = correctionLossConfig()
	p.signalResolver = r
	p.correctionCfg.StabilizerEnabled, p.correctionCfg.StabilizerMaxChecks = true, 1
	p.stabilizerEnabled = true
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 2)
	p.telnetStabilizer.Start()
	t.Cleanup(p.telnetStabilizer.Stop)
	s := correctedGridSource(at, false)
	if configure != nil {
		configure(p, s)
	}
	p.processSpot(s, nil)
	envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
	seedCorrectedGridWinner(t, r, d, key)
	return p, envelope
}

func seedCorrectedGridWinner(t *testing.T, r *spot.SignalResolver, d *spot.SignalResolverDriver, key spot.ResolverSignalKey) {
	t.Helper()
	if d.Drain() != 1 {
		t.Fatal("initial held source was not genuinely enqueued to resolver")
	}
	initial := correctionLossPopulation(t, r, key, 1, map[string]int{"K1ABC": 1})
	at := retentionClockAfter(t, initial.EvaluatedAt.Add(time.Nanosecond))
	correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at, time.Minute, "N0BBB", "N0CCC", "N0FFF")
	correctionLossPopulation(t, r, key, 4, map[string]int{"K1ABC": 1, "K1ABD": 3})
}
