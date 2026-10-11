//go:build !qualification

// File role: Exposes receiver-level historical freshness through production
// stabilizer admission and S-floor output, actual TCP/peer receipt and reopened
// archive rows. Raw database bytes remain an independent persistence oracle.
package cluster

import (
	"encoding/binary"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

func customSCPFreshnessSeed(t *testing.T, path string, now time.Time, static, allFresh bool) map[string][]byte {
	t.Helper()
	want := make(map[string][]byte)
	if static {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(now.Unix()))
		want["m|"+customSCPDeliveryCall] = value
	}
	ages := []time.Duration{55 * 24 * time.Hour, 54 * 24 * time.Hour, time.Minute}
	if allFresh {
		ages = []time.Duration{3 * time.Minute, 2 * time.Minute, time.Minute}
	}
	for i, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		value := make([]byte, 10)
		binary.BigEndian.PutUint64(value[:8], uint64(now.Add(-ages[i]).Unix()))
		binary.BigEndian.PutUint16(value[8:], uint16(101*(i+1)))
		want["o|"+customSCPDeliveryCall+"|20m|cw|"+receiver] = value
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	for key, value := range want {
		if err := db.Set([]byte(key), value, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	return want
}

// Compare bytes before opening the decoding store. Readback through its public
// methods alone could hide a changed timestamp or cell representation.
func customSCPFreshnessAssertBytes(t *testing.T, path string, want map[string][]byte) {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			t.Error(err)
		}
	}()
	got := make(map[string][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		got[string(iter.Key())] = append([]byte(nil), iter.Value()...)
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("freshness queries/output must leave raw persisted records unchanged: got=%x want=%x", got, want)
	}
}

func customSCPFreshnessOpen(t *testing.T, options spot.CustomSCPOptions) *spot.CustomSCPStore {
	t.Helper()
	store, err := spot.OpenCustomSCPStore(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func customSCPFreshnessFixture(t *testing.T, at time.Time, static, allFresh, stabilize bool) (ambiguityDeliveryFixture, *spot.CustomSCPStore, spot.CustomSCPOptions, map[string][]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scp")
	raw := customSCPFreshnessSeed(t, path, at, static, allFresh)
	customSCPFreshnessAssertBytes(t, path, raw)
	options := spot.CustomSCPOptions{
		Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxSpottersPerKey: 8,
		CoreMinScore: 5, CoreMinH3Cells: 2,
		SFloorMinScore: 5, SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2,
	}
	store := customSCPFreshnessOpen(t, options)
	if got := store.StatsSnapshot().ObservationSpotters; got != 3 {
		t.Fatalf("all three encoded observations must remain retained: got %d", got)
	}
	if store.StaticContains(customSCPDeliveryCall) != static {
		t.Fatal("raw static presence must establish the independent recognition control")
	}
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled: stabilize, StabilizerMaxChecks: 1, StabilizerTimeoutAction: stabilizerTimeoutRelease,
		RecentBandRecordMinUniqueSpotters: 2,
		CustomSCP: config.CallCorrectionCustomSCPConfig{
			Enabled: true, ResolverMinScore: 5, ResolverMinUniqueSpotters: 2, ResolverMinUniqueH3Cells: 2,
			StabilizerMinScore: 5, StabilizerMinUniqueSpotters: 2, StabilizerMinUniqueH3Cells: 2,
			SFloorMinScore: 5, SFloorMinUniqueSpottersExact: 2, SFloorMinUniqueH3CellsExact: 2,
			SFloorMinUniqueSpottersFamily: 2, SFloorMinUniqueH3CellsFamily: 2,
		},
	}
	f := newAmbiguityDeliveryFixture(t, nil, cfg)
	f.pipeline.recentBandStore, f.pipeline.customSCPStore = store, store
	f.pipeline.stabilizerEnabled = stabilize
	if stabilize {
		// Do not start the scheduler: its real bounded input owner reserves a
		// held spot until explicit Stop, with no wall-clock release race.
		f.pipeline.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 4)
		t.Cleanup(f.pipeline.telnetStabilizer.Stop)
	}
	return f, store, options, raw
}

func customSCPFreshnessAssertPeer(t *testing.T, f ambiguityDeliveryFixture, call string, at time.Time) {
	t.Helper()
	peerOriginalAssertFields(t, f.peer, []string{
		"14020", call, at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0AAA", "N0CALL-1", "192.0.2.1",
	})
}

func customSCPFreshnessCheckStored(t *testing.T, f ambiguityDeliveryFixture, want map[string]string) {
	t.Helper()
	assertRows := func(label string, rows []*spot.Spot) {
		t.Helper()
		if len(rows) != len(want) {
			t.Errorf("%s count=%d want %d; rows=%+v", label, len(rows), len(want), rows)
		}
		seen := make(map[string]bool)
		for _, row := range rows {
			glyph, expected := want[row.DXCall]
			if !expected || seen[row.DXCall] || row.Confidence != glyph {
				t.Errorf("%s must preserve the independent local identity/glyph population: call=%s glyph=%s", label, row.DXCall, row.Confidence)
			}
			seen[row.DXCall] = true
		}
	}
	assertRows("ring", f.ring.GetRecent(4))
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(4)
	if err != nil {
		t.Fatal(err)
	}
	assertRows("reopened archive", rows)
}

func customSCPFreshnessCheckReopen(t *testing.T, store *spot.CustomSCPStore, options spot.CustomSCPOptions, raw map[string][]byte, at time.Time, static, allFresh bool) {
	t.Helper()
	if got := store.StatsSnapshot().ObservationSpotters; got != 3 {
		t.Errorf("query/output changed retained observations: got %d want 3", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	customSCPFreshnessAssertBytes(t, options.Path, raw)
	reopened := customSCPFreshnessOpen(t, options)
	if got := reopened.StatsSnapshot().ObservationSpotters; got != 3 || reopened.StaticContains(customSCPDeliveryCall) != static {
		t.Errorf("independent reopened recovery changed retained observations/static membership: count=%d static=%t", got, reopened.StaticContains(customSCPDeliveryCall))
	}
	if got := reopened.HasRecentSupport(customSCPDeliveryCall, "20m", "CW", 2, at); got != allFresh {
		t.Errorf("reopened eligibility=%t want %t; static recognition must not replace fresh receivers/cells", got, allFresh)
	}
}

func TestCustomSCPFreshnessDeliveredStabilizer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		allFresh bool
	}{{"old_receivers_remain_history", false}, {"fresh_receivers_authorize_delivery", true}} {
		t.Run(tc.name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			f, store, options, raw := customSCPFreshnessFixture(t, at, true, tc.allFresh, true)
			s := newDeliveryTestSpot(customSCPDeliveryCall, "N0AAA", at)
			s.SourceType, s.Comment = spot.SourceManual, " "
			f.pipeline.processSpot(s, nil)
			if s.Confidence != "S" {
				t.Errorf("same learned static membership must recognize both populations: glyph=%s", s.Confidence)
			}
			wantStored := map[string]string{"K1BAR": "P"}
			if tc.allFresh {
				if f.pipeline.telnetStabilizer.Pending() != 0 || f.ring.GetCount() != 1 {
					t.Errorf("genuinely fresh history must deliver without a hold: pending=%d ring=%d", f.pipeline.telnetStabilizer.Pending(), f.ring.GetCount())
				}
				peerOriginalAssertTCP(t, f, customSCPDeliveryCall, "S")
				customSCPFreshnessAssertPeer(t, f, customSCPDeliveryCall, at)
				wantStored[customSCPDeliveryCall] = "S"
			} else if f.pipeline.telnetStabilizer.Pending() != 1 || f.ring.GetCount() != 0 {
				// Continue to the positive FIFO oracle so a defective baseline
				// reports the actual incorrectly delivered TCP/peer identity.
				t.Errorf("one fresh receiver must not refresh the two old receivers/cells: pending=%d ring=%d", f.pipeline.telnetStabilizer.Pending(), f.ring.GetCount())
			}
			barrier := newDeliveryTestSpot("K1BAR", "N0AAA", at)
			barrier.SourceType, barrier.Comment, barrier.Confidence = spot.SourceManual, " ", "P"
			f.pipeline.processSpot(barrier, nil)
			// Receiving the independent P source first establishes absence of
			// early held output without a negative TCP timeout.
			peerOriginalAssertTCP(t, f, "K1BAR", "P")
			customSCPFreshnessAssertPeer(t, f, "K1BAR", at)
			customSCPFreshnessCheckStored(t, f, wantStored)
			wantPending := int64(1)
			if tc.allFresh {
				wantPending = 0
			}
			if f.pipeline.telnetStabilizer.Pending() != wantPending {
				t.Errorf("FIFO control altered subject hold ownership: pending=%d want %d", f.pipeline.telnetStabilizer.Pending(), wantPending)
			}
			f.pipeline.telnetStabilizer.Stop()
			if f.pipeline.telnetStabilizer.Pending() != 0 {
				t.Fatal("explicit retirement did not release held ownership")
			}
			customSCPFreshnessCheckReopen(t, store, options, raw, at, true, tc.allFresh)
		})
	}
}

func TestCustomSCPFreshnessDeliveredSFloor(t *testing.T) {
	for _, tc := range []struct {
		name, glyph string
		allFresh    bool
	}{{"old_history_does_not_create_floor", "?", false}, {"fresh_history_creates_floor", "S", true}} {
		t.Run(tc.name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			f, store, options, raw := customSCPFreshnessFixture(t, at, false, tc.allFresh, false)
			s := newDeliveryTestSpot(customSCPDeliveryCall, "N0AAA", at)
			s.SourceType, s.Comment = spot.SourceManual, " "
			f.pipeline.processSpot(s, nil)
			peerOriginalAssertTCP(t, f, customSCPDeliveryCall, tc.glyph)
			customSCPFreshnessAssertPeer(t, f, customSCPDeliveryCall, at)
			if s.ConfidencePercentOK || store.StaticContains(customSCPDeliveryCall) {
				t.Error("observation-only history must not invent a current-support percentage or static membership")
			}
			customSCPFreshnessCheckStored(t, f, map[string]string{customSCPDeliveryCall: tc.glyph})
			customSCPFreshnessCheckReopen(t, store, options, raw, at, false, tc.allFresh)
		})
	}
}
