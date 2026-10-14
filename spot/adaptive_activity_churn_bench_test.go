package spot

import (
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

func activityChurnController(population int) (*AdaptiveMinReports, []string) {
	a := NewAdaptiveMinReports(adaptiveActivityConfig())
	at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	for i := range population {
		a.Observe("20m", fmt.Sprintf("OLD%06d", i), at)
	}
	a.MinReportsForBand("20m", at)
	// Received identities already exist before activity admission. Cycling only
	// after 200 seconds keeps keys disjoint within the 60-second window.
	reporters := make([]string, 2000)
	for i := range reporters {
		reporters[i] = fmt.Sprintf("NEW%06d", i)
	}
	return a, reporters
}

func BenchmarkAdaptiveActivityChurn(b *testing.B) {
	for _, population := range []int{1000, 10000, 100000} {
		for _, mechanism := range []string{"raw", "legacyLate", "transaction"} {
			b.Run(fmt.Sprintf("%d/%s", population, mechanism), func(b *testing.B) {
				a, reporters := activityChurnController(population)
				transaction, available := any(a).(activityAdmissionMeasurement)
				if mechanism == "transaction" && !available {
					b.Skip("pre-repair controller has no admission transaction")
				}
				at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
				queue := make(chan *Spot, 1)
				s := &Spot{}
				sink := func() bool {
					select {
					case queue <- s:
						return true
					default:
						return false
					}
				}
				clock := func() time.Time { return at }
				i := 0
				b.ReportAllocs()
				for b.Loop() {
					at = at.Add(100 * time.Millisecond)
					r := reporters[i%len(reporters)]
					i++
					admitted := false
					switch mechanism {
					case "raw":
						admitted = sink()
					case "legacyLate":
						admitted = sink()
						if admitted {
							a.Observe("20m", r, at)
							a.MinReportsForBand("20m", at)
						}
					case "transaction":
						admitted = transaction.AdmitObservation("20m", r, clock, sink)
					}
					if !admitted {
						b.Fatal("empty churn sink refused successful progress")
					}
					<-queue
				}
				if mechanism != "raw" && i >= 2000 {
					a.mu.Lock()
					retained := len(a.reporterSeen["20m"])
					a.mu.Unlock()
					if retained < 601 || retained > 610 {
						b.Fatalf("60-second window plus 1-second cadence retained%d want601..610", retained)
					}
				}
			})
		}
	}
}

func TestAdaptiveActivityChurnLatencyMeasurement(t *testing.T) {
	for _, population := range []int{1000, 10000, 100000} {
		for _, queries := range []bool{false, true} {
			for _, mechanism := range []string{"raw", "legacyLate", "transaction"} {
				t.Run(fmt.Sprintf("%d/queries%t/%s", population, queries, mechanism), func(t *testing.T) {
					a, reporters := activityChurnController(population)
					transaction, available := any(a).(activityAdmissionMeasurement)
					if mechanism == "transaction" && !available {
						t.Skip("pre-repair controller has no admission transaction")
					}
					at := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
					var queryClock atomic.Int64
					queryClock.Store(at.UnixNano())
					queue := make(chan *Spot, 1)
					s := &Spot{}
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
					if queries {
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
					const count = 2000
					latencies := make([]int64, 0, count)
					for i := range count {
						at = at.Add(100 * time.Millisecond)
						queryClock.Store(at.UnixNano())
						r := reporters[i]
						started := activityLatencyNow()
						admitted := false
						switch mechanism {
						case "raw":
							admitted = sink()
						case "legacyLate":
							admitted = sink()
							if admitted {
								a.Observe("20m", r, at)
								a.MinReportsForBand("20m", at)
							}
						case "transaction":
							admitted = transaction.AdmitObservation("20m", r, clock, sink)
						}
						latencies = append(latencies, activityLatencyNow()-started)
						if !admitted {
							t.Fatal("empty churn sink refused successful progress")
						}
						<-queue
					}
					if queries {
						close(stop)
						<-done
					}
					a.mu.Lock()
					retained := len(a.reporterSeen["20m"])
					a.mu.Unlock()
					if mechanism != "raw" && (retained < 601 || retained > 610) {
						t.Fatalf("retained%d after200 simulatedseconds, want601..610", retained)
					}
					sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
					t.Logf("go=%s clock=%s GOMAXPROCS=%d samples=%d population=%d simulated=200s window=60s prune=1s retained=%d p50=%dns p95=%dns p99=%dns max=%dns", runtime.Version(), activityLatencyClock, runtime.GOMAXPROCS(0), count, population, retained, latencies[count/2], latencies[count*95/100], latencies[count*99/100], latencies[count-1])
				})
			}
		}
	}
}
