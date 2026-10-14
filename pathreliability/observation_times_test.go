package pathreliability

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestObservationTimesAdmission(t *testing.T) {
	arrival := time.Unix(0, 345678901).UTC()
	otherZone := time.FixedZone("fixture", 3600)
	for _, tc := range []struct {
		name   string
		times  ObservationTimes
		status AdmissionStatus
		want   time.Time
	}{
		{"equal", ObservationTimes{arrival, arrival}, AdmissionAccepted, arrival},
		{"late", ObservationTimes{arrival.Add(-200 * time.Second), arrival}, AdmissionAccepted, arrival.Add(-200 * time.Second)},
		{"future_nanosecond", ObservationTimes{arrival.Add(time.Nanosecond), arrival}, AdmissionClampedFuture, arrival},
		{"future_120_seconds_minus_nanosecond", ObservationTimes{arrival.Add(120*time.Second - time.Nanosecond), arrival}, AdmissionClampedFuture, arrival},
		{"future_120_seconds", ObservationTimes{arrival.Add(120 * time.Second), arrival}, AdmissionClampedFuture, arrival},
		{"future_120_seconds_plus_nanosecond", ObservationTimes{arrival.Add(120*time.Second + time.Nanosecond), arrival}, AdmissionRejectedTime, time.Time{}},
		{"missing_observation", ObservationTimes{ArrivedAt: arrival}, AdmissionAccepted, arrival},
		{"missing_arrival", ObservationTimes{ObservedAt: arrival}, AdmissionRejectedTime, time.Time{}},
		{"missing_arrival_other_zone", ObservationTimes{arrival, time.Time{}.In(otherZone)}, AdmissionRejectedTime, time.Time{}},
		{"equal_other_zone", ObservationTimes{arrival.In(otherZone), arrival.In(otherZone)}, AdmissionAccepted, arrival},
		{"epoch_is_not_missing", ObservationTimes{time.Unix(0, 0).UTC(), arrival}, AdmissionAccepted, time.Unix(0, 0).UTC()},
		{"pre_epoch", ObservationTimes{time.Unix(-1, 0).UTC(), arrival}, AdmissionAccepted, time.Unix(-1, 0).UTC()},
		{"huge_future_duration", ObservationTimes{time.Date(4000, 1, 1, 0, 0, 0, 0, time.UTC), arrival}, AdmissionRejectedTime, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := validateReport(&tc.times, 1)
			if got.Status != tc.status || !got.EffectiveObservation.Equal(tc.want) {
				t.Fatalf("admission=%+v, want status=%d observation=%s", got, tc.status, tc.want)
			}
			if got.Accepted() && got.EffectiveObservation.Location() != time.UTC {
				t.Fatal("effective observation must be UTC")
			}
		})
	}
}

func TestObservationTimesOriginalWeight(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	for _, weight := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		cfg := DefaultConfig()
		cfg.BeaconWeightCap = .5
		p := NewPredictor(cfg, []string{"20m"})
		got := p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -10, weight, ObservationTimes{now, now}, true, 1)
		if got.Status != AdmissionRejectedWeight || p.TotalBuckets() != 0 || p.AdmissionStats().RejectedWeight != 1 {
			t.Fatalf("weight %v passed original-weight admission: %+v counters=%+v", weight, got, p.AdmissionStats())
		}
		s := NewStore(cfg, []string{"20m"})
		if a := s.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -10, weight, ObservationTimes{now, now}, 1); a.Accepted() || s.TotalBuckets() != 0 {
			t.Fatalf("direct Store admitted invalid weight %v", weight)
		}
	}
	cfg := DefaultConfig()
	cfg.BeaconWeightCap = .5
	p := NewPredictor(cfg, []string{"20m"})
	got := p.Update(BucketCombined, 1, 2, 3, 4, "20m", -10, 1e300, ObservationTimes{now, now}, true)
	fine, _ := p.combined.Lookup(1, 2, 3, 4, "20m", now)
	if !got.Accepted() || fine.RawWeight != .5 {
		t.Fatalf("finite beacon weight was not limited before storage: %+v sample=%+v", got, fine)
	}
}

func TestObservationTimesCounters(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	p := NewPredictor(DefaultConfig(), []string{"20m"})
	a := p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -10, 1, ObservationTimes{now.Add(30 * time.Second), now}, false, 1)
	if a.Status != AdmissionClampedFuture || !a.EffectiveObservation.Equal(now) || p.TotalBuckets() != 2 {
		t.Fatalf("clamped admission=%+v buckets=%d", a, p.TotalBuckets())
	}
	p.Update(BucketCombined, 1, 2, 3, 4, "20m", -10, 1, ObservationTimes{now.Add(121 * time.Second), now}, false)
	p.Update(BucketCombined, 1, 2, 3, 4, "20m", -10, 1, ObservationTimes{ObservedAt: now}, false)
	p.Update(BucketCombined, 1, 2, 3, 4, "20m", -10, 0, ObservationTimes{now, now}, false)
	if got := p.AdmissionStats(); got != (AdmissionStats{ClampedFuture: 1, RejectedTime: 2, RejectedWeight: 1}) {
		t.Fatalf("report counters=%+v", got)
	}
	fine, coarse := p.combined.Lookup(1, 2, 3, 4, "20m", now)
	if fine.RawCount != 1 || coarse.RawCount != 1 || fine.RawWeight != 1 || coarse.RawWeight != 1 {
		t.Fatalf("rejected reports mutated evidence: fine=%+v coarse=%+v", fine, coarse)
	}
	direct := NewStore(DefaultConfig(), []string{"20m"})
	if got := direct.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -10, 1, ObservationTimes{now.Add(121 * time.Second), now}, 1); got.Accepted() || direct.TotalBuckets() != 0 {
		t.Fatal("direct Store bypassed timestamp rejection")
	}
	if got := direct.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -10, 1, ObservationTimes{now.Add(time.Second), now}, 1); got.Status != AdmissionClampedFuture || !got.EffectiveObservation.Equal(now) {
		t.Fatalf("direct Store failed to clamp: %+v", got)
	}
}

func TestObservationTimesConcurrentCounters(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	p := NewPredictor(DefaultConfig(), []string{"20m"})
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for range 50 {
				p.Update(BucketCombined, 1, 2, 3, 4, "20m", -10, 1, ObservationTimes{now.Add(time.Second), now}, false)
				p.Update(BucketCombined, 1, 2, 3, 4, "20m", -10, 1, ObservationTimes{now.Add(121 * time.Second), now}, false)
			}
		})
	}
	workers.Wait()
	if got := p.AdmissionStats(); got.ClampedFuture != 800 || got.RejectedTime != 800 || got.RejectedWeight != 0 {
		t.Fatalf("concurrent report counters=%+v", got)
	}
	if got := (*Predictor)(nil).AdmissionStats(); got != (AdmissionStats{}) {
		t.Fatalf("nil predictor counters=%+v", got)
	}
}
