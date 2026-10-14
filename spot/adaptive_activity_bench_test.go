package spot

import (
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

type activityAdmissionMeasurement interface {
	AdmitObservation(string, string, func() time.Time, func() bool) bool
}

func TestAdaptiveActivityMeasurementClockResolution(t *testing.T) {
	minimum := int64(1<<63 - 1)
	zeros := 0
	previous := activityLatencyNow()
	for range 10000 {
		current := activityLatencyNow()
		delta := current - previous
		if delta < 0 {
			t.Fatal("latency measurement clock regressed")
		}
		if delta == 0 {
			zeros++
		} else if delta < minimum {
			minimum = delta
		}
		previous = current
	}
	if zeros == 10000 {
		t.Fatal("measurement clock cannot resolve the instrumented samples")
	}
	t.Logf("clock=%s minimum_observed_positive_delta=%dns zero_deltas=%d/10000", activityLatencyClock, minimum, zeros)
}

func activityMeasurementController(population int) *AdaptiveMinReports {
	cfg := adaptiveActivityConfig()
	// Keep all seeded receivers fresh while the cadence clock advances for
	// measurements. This isolates scan cost from eviction/cardinality changes.
	cfg.AdaptiveMinReports.WindowMinutes = 1000000
	a := NewAdaptiveMinReports(cfg)
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	for i := range population {
		a.Observe("20m", fmt.Sprintf("N0R%06d", i), at)
	}
	a.MinReportsForBand("20m", at)
	return a
}

// These controls are compiled before and after the repair. raw intentionally
// has no activity accounting; legacyLate calls the existing Observe API after
// sending (the old implementation is captured by the pre-repair binary).
// The final legacyLate control also includes Observe's new maintenance.
func BenchmarkAdaptiveActivityAdmission(b *testing.B) {
	for _, population := range []int{1000, 10000, 100000} {
		for _, workload := range []string{"normal", "prune", "queries", "refusal"} {
			for _, mechanism := range []string{"raw", "legacyLate", "transaction"} {
				b.Run(fmt.Sprintf("%d/%s/%s", population, workload, mechanism), func(b *testing.B) {
					a := activityMeasurementController(population)
					transaction, available := any(a).(activityAdmissionMeasurement)
					if mechanism == "transaction" && !available {
						b.Skip("pre-repair controller has no admission transaction")
					}
					at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
					var queryClock atomic.Int64
					queryClock.Store(at.UnixNano())
					queue := make(chan *Spot, 1)
					s := &Spot{}
					if workload == "refusal" {
						queue <- s
					}
					sink := func() bool {
						select {
						case queue <- s:
							return true
						default:
							return false
						}
					}
					clock := func() time.Time { return at }
					stop, done := make(chan struct{}), make(chan struct{})
					if workload == "queries" {
						go func() {
							defer close(done)
							for {
								select {
								case <-stop:
									return
								default:
									a.MinReportsForBand("20m", time.Unix(0, queryClock.Load()))
								}
							}
						}()
					}
					b.ReportAllocs()
					for b.Loop() {
						if workload == "prune" {
							at = at.Add(time.Second)
						}
						admitted := false
						switch mechanism {
						case "raw":
							admitted = sink()
						case "legacyLate":
							admitted = sink()
							if admitted {
								a.Observe("20m", "N0R000000", at)
								if workload == "prune" {
									a.MinReportsForBand("20m", at)
								}
							}
						case "transaction":
							admitted = transaction.AdmitObservation("20m", "N0R000000", clock, sink)
						}
						if admitted == (workload == "refusal") {
							b.Fatal("bounded sink changed its admission result")
						}
						if admitted {
							<-queue
						}
					}
					if workload == "queries" {
						close(stop)
						<-done
					}
					a.mu.Lock()
					retained := len(a.reporterSeen["20m"])
					a.mu.Unlock()
					if retained != population {
						b.Fatalf("measurement lost fresh receivers: %d != %d", retained, population)
					}
				})
			}
		}
	}
}

// Instrumented latency distributions are local controlled measurements. Timer
// overhead is shared by every mechanism; they are not production p99 evidence.
func TestAdaptiveActivityAdmissionLatencyMeasurement(t *testing.T) {
	for _, population := range []int{1000, 10000, 100000} {
		for _, workload := range []string{"normal", "prune", "queries", "refusal"} {
			for _, mechanism := range []string{"raw", "legacyLate", "transaction"} {
				t.Run(fmt.Sprintf("%d/%s/%s", population, workload, mechanism), func(t *testing.T) {
					a := activityMeasurementController(population)
					transaction, available := any(a).(activityAdmissionMeasurement)
					if mechanism == "transaction" && !available {
						t.Skip("pre-repair controller has no admission transaction")
					}
					at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
					queue := make(chan *Spot, 1)
					s := &Spot{}
					if workload == "refusal" {
						queue <- s
					}
					sink := func() bool {
						select {
						case queue <- s:
							return true
						default:
							return false
						}
					}
					clock := func() time.Time { return at }
					stop, done := make(chan struct{}), make(chan struct{})
					if workload == "queries" {
						go func() {
							defer close(done)
							fixed := at
							for {
								select {
								case <-stop:
									return
								default:
									a.MinReportsForBand("20m", fixed)
								}
							}
						}()
					}
					count := 5000
					if workload == "prune" {
						count = 50
					}
					latencies := make([]int64, 0, count)
					for range count {
						if workload == "prune" {
							at = at.Add(time.Second)
						}
						started := activityLatencyNow()
						admitted := false
						switch mechanism {
						case "raw":
							admitted = sink()
						case "legacyLate":
							admitted = sink()
							if admitted {
								a.Observe("20m", "N0R000000", at)
								if workload == "prune" {
									a.MinReportsForBand("20m", at)
								}
							}
						case "transaction":
							admitted = transaction.AdmitObservation("20m", "N0R000000", clock, sink)
						}
						latencies = append(latencies, activityLatencyNow()-started)
						if admitted == (workload == "refusal") {
							t.Fatal("measurement failed admission progress guard")
						}
						if admitted {
							<-queue
						}
					}
					if workload == "queries" {
						close(stop)
						<-done
					}
					sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
					t.Logf("go=%s clock=%s GOMAXPROCS=%d samples=%d p50=%dns p95=%dns p99=%dns max=%dns", runtime.Version(), activityLatencyClock, runtime.GOMAXPROCS(0), count,
						latencies[count/2], latencies[(count*95)/100], latencies[(count*99)/100], latencies[count-1])
				})
			}
		}
	}
}
