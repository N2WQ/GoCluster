//go:build !qualification

// File role: Exercises independent family-history qualification through actual
// local and peer delivery. Literal Pebble records keep history and persistence
// expectations independent of runtime observation admission and normalization.
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

const customSCPFamilyDeliveryCall = "W1AW/KH6"

type customSCPFamilyDeliveryCase struct {
	name                          string
	voteCount, baseCount          int
	exactMinimum, familyMinimum   int
	static, wantExact, wantFamily bool
	wantGlyph                     string
}

func customSCPFamilySeed(t *testing.T, path string, at time.Time, tc customSCPFamilyDeliveryCase) map[string][]byte {
	t.Helper()
	raw := make(map[string][]byte)
	if tc.static {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(at.Unix()))
		raw["m|W1AW/KH6"] = value
	}
	// The fresh vote member scores 10; the older base member scores 5.
	// Each member has its own disjoint receiver population and two cells.
	// Three receivers still supply only two cells, avoiding accidental pooling.
	for _, member := range []struct {
		call      string
		count     int
		age       time.Duration
		receivers [3]string
	}{
		{"W1AW/KH6", tc.voteCount, 0, [3]string{"N0AAA", "N0BBB", "N0CCC"}},
		{"W1AW", tc.baseCount, 5 * 24 * time.Hour, [3]string{"N0DDD", "N0EEE", "N0FFF"}},
	} {
		for i := 0; i < member.count; i++ {
			value := make([]byte, 10)
			stamp := at.Add(-member.age - time.Duration(i+1)*time.Minute)
			binary.BigEndian.PutUint64(value[:8], uint64(stamp.Unix()))
			binary.BigEndian.PutUint16(value[8:], []uint16{101, 202, 101}[i])
			raw["o|"+member.call+"|20m|cw|"+member.receivers[i]] = value
		}
	}
	// Observation-only cases have no learned static membership for either
	// identity. The positive static control adds only the explicit full call.
	if _, exists := raw["m|W1AW"]; exists {
		t.Fatal("family fixture must not introduce base static membership")
	}
	if _, exists := raw["m|W1AW/KH6"]; exists != tc.static {
		t.Fatal("literal fixture static membership disagrees with its control")
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
	for key, value := range raw {
		if err := db.Set([]byte(key), value, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	return raw
}

func customSCPFamilyAssertQueries(t *testing.T, store *spot.CustomSCPStore, at time.Time, tc customSCPFamilyDeliveryCase) {
	t.Helper()
	if full, base := store.StaticContains("W1AW/KH6"), store.StaticContains("W1AW"); full != tc.static || base {
		t.Errorf("static recognition must remain separate from family observations: full=%t base=%t want full=%t base=false", full, base, tc.static)
	}
	wantStaticCount := 0
	if tc.static {
		wantStaticCount = 1
	}
	if stats := store.StatsSnapshot(); stats.ObservationSpotters != tc.voteCount+tc.baseCount || stats.StaticCalls != wantStaticCount {
		t.Errorf("family queries must preserve independently encoded retained populations: stats=%+v want observations=%d static=%t", stats, tc.voteCount+tc.baseCount, tc.static)
	}
	if exact := store.HasSFloorSupportExact("W1AW/KH6", "20m", "CW", tc.exactMinimum, at); exact != tc.wantExact {
		t.Errorf("exact-call qualification=%t want %t", exact, tc.wantExact)
	}
	for _, members := range [][2]string{{"W1AW/KH6", "W1AW"}, {"W1AW", "W1AW/KH6"}} {
		if family := store.HasSFloorSupportFamilyKeyPair(members[0], members[1], "20m", "CW", tc.familyMinimum, at); family != tc.wantFamily {
			t.Errorf("family pair %q qualification=%t want %t; members must independently satisfy all gates", members, family, tc.wantFamily)
		}
		if family := store.HasSFloorSupportFamily(members[:], "20m", "CW", tc.familyMinimum, at); family != tc.wantFamily {
			t.Errorf("family slice %q qualification=%t want %t; member order and pooled support must not select eligibility", members, family, tc.wantFamily)
		}
	}
}

func TestCustomSCPFamilyDeliveredIndependentQualification(t *testing.T) {
	for _, tc := range []customSCPFamilyDeliveryCase{
		{
			name: "older_base_independently_qualifies", voteCount: 2, baseCount: 3,
			exactMinimum: 3, familyMinimum: 3, wantFamily: true, wantGlyph: "S",
		},
		{
			name: "individually_failing_members_do_not_pool", voteCount: 2, baseCount: 2,
			exactMinimum: 3, familyMinimum: 3, wantGlyph: "?",
		},
		{
			name: "exact_qualifies_despite_stricter_family", voteCount: 3,
			exactMinimum: 3, familyMinimum: 4, wantExact: true, wantGlyph: "S",
		},
		{
			name: "static_recognition_without_family_support", static: true,
			exactMinimum: 3, familyMinimum: 3, wantGlyph: "S",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			path := filepath.Join(t.TempDir(), "scp")
			raw := customSCPFamilySeed(t, path, at, tc)
			// Inspect exact record bytes before any production decoder opens them.
			customSCPFreshnessAssertBytes(t, path, raw)
			options := spot.CustomSCPOptions{
				Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxKeys: 8, MaxSpottersPerKey: 8,
				CoreMinScore: 3, CoreMinH3Cells: 2,
				SFloorMinScore: 3, SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2,
			}
			store := customSCPFreshnessOpen(t, options)
			// Query failures remain nonfatal so the defective baseline also
			// demonstrates the actual user-visible consequence on TCP and storage.
			customSCPFamilyAssertQueries(t, store, at, tc)
			cfg := config.CallCorrectionConfig{CustomSCP: config.CallCorrectionCustomSCPConfig{
				Enabled: true, SFloorMinScore: 3,
				SFloorMinUniqueSpottersExact: tc.exactMinimum, SFloorMinUniqueH3CellsExact: 2,
				SFloorMinUniqueSpottersFamily: tc.familyMinimum, SFloorMinUniqueH3CellsFamily: 2,
			}}
			// No current resolver population or stabilizer is involved: only
			// historical recognition can upgrade the original unknown identity.
			f := newAmbiguityDeliveryFixture(t, nil, cfg)
			f.pipeline.recentBandStore, f.pipeline.customSCPStore = store, store
			s := newDeliveryTestSpot(customSCPFamilyDeliveryCall, "N0AAA", at)
			s.SourceType = spot.SourceManual
			f.pipeline.processSpot(s, nil)
			peerOriginalAssertTCP(t, f, "W1AW/KH6", tc.wantGlyph)
			peerOriginalAssertFields(t, f.peer, []string{
				"14020", "W1AW/KH6", at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0AAA", "N0CALL-1", "192.0.2.1",
			})
			if s.DXCall != "W1AW/KH6" || s.ConfidencePercentOK || f.pipeline.tracker.CallCorrections() != 0 {
				t.Errorf("historical recognition must retain the input identity without current-support percentage or correction: call=%s percentOK=%t corrections=%d", s.DXCall, s.ConfidencePercentOK, f.pipeline.tracker.CallCorrections())
			}
			customSCPFreshnessCheckStored(t, f, map[string]string{"W1AW/KH6": tc.wantGlyph})
			customSCPFamilyAssertQueries(t, store, at, tc)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
			// Independently recover the same literal population, repeat both
			// query orders, then inspect bytes once more after decoder shutdown.
			reopened := customSCPFreshnessOpen(t, options)
			customSCPFamilyAssertQueries(t, reopened, at, tc)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
		})
	}
}
