//go:build !qualification

// File role: Exposes future learned membership through real local/peer delivery
// while independently encoded observation history can still provide an S floor.
// Raw record comparisons precede decoding and follow queries, delivery and reopen.
package cluster

import (
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

type customSCPStaticFutureCase struct {
	name, glyph                 string
	markerAge                   time.Duration
	voteCount, baseCount        int
	exactMinimum, familyMinimum int
	staticNow, exact, family    bool
}

func customSCPStaticFutureSeed(t *testing.T, path string, at, marker time.Time, tc customSCPStaticFutureCase) map[string][]byte {
	t.Helper()
	raw := customSCPFamilySeed(t, path, at, customSCPFamilyDeliveryCase{
		voteCount: tc.voteCount, baseCount: tc.baseCount,
	})
	// Add precisely one independently encoded membership marker. Observation
	// seeding does not learn membership for either the full call or its base.
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(marker.Unix()))
	raw["m|W1AW/KH6"] = value
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("m|W1AW/KH6"), value, pebble.Sync); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return raw
}

func customSCPStaticFutureAssertQueries(t *testing.T, store *spot.CustomSCPStore, at, marker time.Time, tc customSCPStaticFutureCase) {
	t.Helper()
	if got := store.StaticContains("W1AW/KH6"); got != tc.staticNow {
		t.Errorf("process-clock membership=%t want %t for marker %s", got, tc.staticNow, marker)
	}
	if store.StaticContains("W1AW") {
		t.Error("fixture must never recognize base static membership")
	}
	if stats := store.StatsSnapshot(); stats.StaticCalls != 1 || stats.ObservationSpotters != tc.voteCount+tc.baseCount {
		t.Errorf("future quarantine must preserve the retained population: stats=%+v want static=1 observations=%d", stats, tc.voteCount+tc.baseCount)
	}
	wantNow := 0
	if tc.staticNow {
		wantNow = 1
	}
	if got := store.StaticCallCount(at); got != wantNow {
		t.Errorf("eligible membership count at query clock=%d want %d; retained membership is a separate count", got, wantNow)
	}
	for _, boundary := range []struct {
		name string
		now  time.Time
		want int
	}{
		{"before", marker.Add(-time.Second), 0},
		{"equality", marker, 1},
		{"after", marker.Add(time.Second), 1},
	} {
		if got := store.StaticCallCount(boundary.now); got != boundary.want {
			t.Errorf("%s stored-second membership boundary: count=%d want %d", boundary.name, got, boundary.want)
		}
	}
	if got := store.HasSFloorSupportExact("W1AW/KH6", "20m", "CW", tc.exactMinimum, at); got != tc.exact {
		t.Errorf("independent observation-only exact qualification=%t want %t", got, tc.exact)
	}
	if got := store.HasSFloorSupportFamilyKeyPair("W1AW/KH6", "W1AW", "20m", "CW", tc.familyMinimum, at); got != tc.family {
		t.Errorf("independent observation-only family qualification=%t want %t", got, tc.family)
	}
}

func TestCustomSCPStaticFutureDeliveredMembership(t *testing.T) {
	for _, tc := range []customSCPStaticFutureCase{
		{name: "future_membership_without_history", markerAge: 24 * time.Hour, exactMinimum: 3, familyMinimum: 3, glyph: "?"},
		{name: "past_membership_without_history", markerAge: -time.Hour, exactMinimum: 3, familyMinimum: 3, staticNow: true, glyph: "S"},
		{name: "future_membership_with_independent_exact_history", markerAge: 24 * time.Hour, voteCount: 3, exactMinimum: 3, familyMinimum: 4, exact: true, glyph: "S"},
		{name: "future_membership_with_independent_family_history", markerAge: 24 * time.Hour, baseCount: 3, exactMinimum: 4, familyMinimum: 3, family: true, glyph: "S"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			marker := at.Add(tc.markerAge)
			path := filepath.Join(t.TempDir(), "scp")
			raw := customSCPStaticFutureSeed(t, path, at, marker, tc)
			customSCPFreshnessAssertBytes(t, path, raw)
			options := spot.CustomSCPOptions{
				Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxKeys: 8, MaxSpottersPerKey: 8,
				CoreMinScore: 3, CoreMinH3Cells: 2,
				SFloorMinScore: 3, SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2,
			}
			queried := customSCPFreshnessOpen(t, options)
			// Keep behavioral failures nonfatal: the defective baseline must
			// proceed to show whether future recognition reaches delivered output.
			customSCPStaticFutureAssertQueries(t, queried, at, marker, tc)
			if err := queried.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
			store := customSCPFreshnessOpen(t, options)
			cfg := config.CallCorrectionConfig{CustomSCP: config.CallCorrectionCustomSCPConfig{
				Enabled: true, SFloorMinScore: 3,
				SFloorMinUniqueSpottersExact: tc.exactMinimum, SFloorMinUniqueH3CellsExact: 2,
				SFloorMinUniqueSpottersFamily: tc.familyMinimum, SFloorMinUniqueH3CellsFamily: 2,
			}}
			f := newAmbiguityDeliveryFixture(t, nil, cfg)
			f.pipeline.recentBandStore, f.pipeline.customSCPStore = store, store
			// No resolver, temporal decoder or stabilizer supplies current support.
			s := newDeliveryTestSpot("W1AW/KH6", "N0AAA", at)
			s.SourceType = spot.SourceManual
			f.pipeline.processSpot(s, nil)
			for _, output := range []struct {
				name, got, want string
			}{
				{"normal", correctionEvidenceReadDX(t, f.normalConn, f.normalReader), correctionEvidenceWire("W1AW/KH6", tc.glyph, "CW", at)},
				{"DIAG", correctionEvidenceReadDX(t, f.diagConn, f.diagReader), correctionEvidenceWire("W1AW/KH6", tc.glyph, "CW --%", at)},
			} {
				if output.got != output.want {
					t.Errorf("actual %s TCP must carry the retained identity, historical glyph and unavailable percentage:\n got %q\nwant %q", output.name, output.got, output.want)
				}
			}
			peerOriginalAssertFields(t, f.peer, []string{
				"14020", "W1AW/KH6", at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0AAA", "N0CALL-1", "192.0.2.1",
			})
			if s.DXCall != "W1AW/KH6" || s.ConfidencePercentOK || f.pipeline.tracker.CallCorrections() != 0 {
				t.Errorf("historical floor must preserve identity without current percentage or correction: call=%s percentOK=%t corrections=%d", s.DXCall, s.ConfidencePercentOK, f.pipeline.tracker.CallCorrections())
			}
			for _, row := range f.ring.GetRecent(4) {
				if row.ConfidencePercentOK {
					t.Error("ring acquired a current-consensus percentage from historical membership")
				}
			}
			customSCPFreshnessCheckStored(t, f, map[string]string{"W1AW/KH6": tc.glyph})
			customSCPStaticFutureAssertQueries(t, store, at, marker, tc)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
			reopened := customSCPFreshnessOpen(t, options)
			customSCPStaticFutureAssertQueries(t, reopened, at, marker, tc)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
		})
	}
}
