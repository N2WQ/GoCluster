package pathreliability

import (
	"sync/atomic"
	"testing"
	"time"
)

// The richer source owns availability. Its false result must never be
// replaced by a separate, potentially contradictory thin SSN getter.
type unavailableCoherentSolarFixture struct {
	*v4SolarProvider
	thinCalls atomic.Int64
}

func (p *unavailableCoherentSolarFixture) CurrentSSN(time.Time) (int, bool) {
	p.thinCalls.Add(1)
	return 112, true
}

func TestVOACAPUnavailableCoherentInputIsAuthoritative(t *testing.T) {
	f, provider, now := v4Fixture(t, fakeClosedForecaster{})
	provider.set(112, false)
	source := &unavailableCoherentSolarFixture{v4SolarProvider: provider}
	f.ssnProvider = source
	startV4Fixture(t, f)
	req := testClosedRequest()
	if got := f.CheckForecastDetailed(req, now); got.Status != VOACAPForecastCheckSSNUnavailable {
		t.Fatalf("unknown coherent input admitted work: %+v", got)
	}
	f.mu.Lock()
	f.addCacheLocked(f.cacheKey(req), v4Entry(f, req, now.Truncate(time.Hour), now, 0))
	f.mu.Unlock()
	got := f.CheckForecastDetailed(req, now)
	if got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 0 {
		t.Fatalf("unknown current input withdrew a valid zero-SSN forecast: %+v", got)
	}
	if snap := f.Snapshot(); snap.InflightEntries != 0 || snap.QueueDepth != 0 || snap.WorkerDurationCount != 0 || source.thinCalls.Load() != 0 {
		t.Fatalf("coherent false fell through or queued: thin calls=%d snapshot=%+v", source.thinCalls.Load(), snap)
	}
}
