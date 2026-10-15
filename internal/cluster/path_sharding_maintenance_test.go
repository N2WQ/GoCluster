// File role: Fixed, real-mapped cold cohorts used by S4's controlled maintenance
// profiles and real-cadence soak. Cold setup uses existing timestamp admission;
// it never accelerates production tickers or mutates private Store ownership.
package cluster

import (
	"fmt"
	"math"
	"testing"
	"time"

	"dxcluster/pathreliability"
)

const pathShardingGrowthBuckets = 20000

func (f *pathShardingConnectedFixture) coldCohort(t testing.TB) []pathShardingPath {
	t.Helper()
	var endpoints []pathShardingEndpoint
	seen := make(map[pathreliability.CellID]bool)
	for i := 0; i < 18*18 && len(endpoints) < 192; i++ {
		cell := (i * 73) % (18 * 18)
		grid := fmt.Sprintf("%c%c55", 'A'+rune(cell%18), 'A'+rune(cell/18))
		e := pathShardingEndpoint{grid, pathreliability.EncodeCell(grid), pathreliability.EncodeCoarseCell(grid)}
		if e.Fine == pathreliability.InvalidCell || e.Coarse == pathreliability.InvalidCell {
			t.Fatalf("unmapped cold grid %s", grid)
		}
		if !seen[e.Fine] {
			seen[e.Fine] = true
			endpoints = append(endpoints, e)
		}
	}
	if len(endpoints) < 128 {
		t.Fatal("cold real-grid population is too small")
	}
	hot := pathShardingKeySet(f.paths)
	all := pathShardingKeySet(f.paths)
	cohort := make([]pathShardingPath, 0, pathShardingGrowthBuckets/2)
	for _, band := range []struct {
		name      string
		frequency float64
	}{{"20m", 14074}, {"40m", 7074}, {"10m", 28074}} {
		for _, receiver := range endpoints {
			for _, sender := range endpoints {
				if receiver.Fine == sender.Fine {
					continue
				}
				path := pathShardingPath{receiver, sender, band.name, band.frequency}
				key := pathShardingPhysicalKey{path.Band, receiver.Fine, sender.Fine, false}
				reverse := pathShardingPhysicalKey{path.Band, sender.Fine, receiver.Fine, false}
				if _, found := hot[key]; found {
					continue
				}
				if _, found := hot[reverse]; found {
					continue
				}
				if _, found := all[key]; found {
					continue
				}
				cohort = append(cohort, path)
				for physical := range pathShardingKeySet([]pathShardingPath{path}) {
					all[physical] = struct{}{}
				}
				if len(all) >= pathShardingGrowthBuckets {
					return cohort
				}
			}
		}
	}
	t.Fatal("fixed mapped cold cohort did not reach growth target")
	return nil
}

func (f *pathShardingConnectedFixture) seedCold(t testing.TB, cohort []pathShardingPath) int {
	return f.seedCohort(t, cohort, false)
}

func (f *pathShardingConnectedFixture) seedCohort(t testing.TB, cohort []pathShardingPath, fresh bool) int {
	t.Helper()
	arrival := time.Now().UTC()
	expected := pathShardingKeySet(f.paths)
	for _, path := range cohort {
		halfLife := f.cfg.BandHalfLifeSec[path.Band]
		if halfLife <= 0 {
			halfLife = f.cfg.DefaultHalfLifeSec
		}
		stale := int64(math.Ceil(float64(halfLife) * f.cfg.StaleAfterHalfLifeMultiplier))
		if f.cfg.StaleAfterHalfLifeMultiplier <= 0 {
			stale = int64(f.cfg.StaleAfterSeconds)
		}
		observed := arrival.Add(-time.Duration(stale+2) * time.Second)
		if fresh {
			observed = arrival
		}
		for _, reverse := range []bool{false, true} {
			if reverse {
				path = pathShardingReversed(path)
			}
			a := f.predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, path.Receiver.Fine, path.Sender.Fine, path.Receiver.Coarse, path.Sender.Coarse, path.Band, pathShardingReportDB, 1,
				pathreliability.ObservationTimes{ObservedAt: observed, ArrivedAt: arrival}, false, pathreliability.ReceiverIdentityHash(pathShardingReceiver(path.Receiver, 0)))
			if !a.Accepted() {
				t.Fatalf("timestamp-valid cold setup rejected: %+v", a)
			}
		}
		for physical := range pathShardingKeySet([]pathShardingPath{path}) {
			expected[physical] = struct{}{}
		}
	}
	if got := f.predictor.TotalBuckets(); got != len(expected) {
		t.Fatalf("cold physical population=%d want %d", got, len(expected))
	}
	return len(expected)
}
