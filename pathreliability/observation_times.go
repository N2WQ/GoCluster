// File role: Defines path-report timestamp admission independently of storage.
// Crawler notes: Observation time ages evidence; arrival validates source-clock
// skew. Normalization never changes the shared spot or its delivery timestamp.
// Related docs: pathreliability/README.md, data/config/PATH_PREDICTIONS.md.
// Related tests: observation_times_test.go.
package pathreliability

import (
	"math"
	"sync/atomic"
	"time"
)

const maxFutureObservationSkew = 120 * time.Second

// ObservationTimes keeps the source observation and path-ingestion arrival
// explicit. A missing observation falls back to arrival; arrival is required.
type ObservationTimes struct {
	ObservedAt time.Time
	ArrivedAt  time.Time
}

// AdmissionStatus describes input validation, not whether a receiver cap admitted
// mass or whether an eligible bucket existed for the report.
type AdmissionStatus uint8

const (
	AdmissionInactive AdmissionStatus = iota
	AdmissionAccepted
	AdmissionClampedFuture
	AdmissionRejectedTime
	AdmissionRejectedWeight
)

// ReportAdmission carries the authoritative timestamp for accepted path-report
// metrics. Rejected input must not enter storage or those metrics.
type ReportAdmission struct {
	EffectiveObservation time.Time
	Status               AdmissionStatus
}

// Accepted reports passed input validation, independently of capped admission.
func (a ReportAdmission) Accepted() bool {
	return a.Status == AdmissionAccepted || a.Status == AdmissionClampedFuture
}

// AdmissionStats contains fixed-cardinality cumulative report counters. Fine and
// coarse bucket writes do not independently increment these counters.
type AdmissionStats struct {
	ClampedFuture  uint64
	RejectedTime   uint64
	RejectedWeight uint64
}

// admissionCounters belongs to the store entry, before bucket fanout. Ordinary
// accepted reports bypass atomic writes; only the three exceptional outcomes
// increment counters.
type admissionCounters struct {
	clampedFuture  atomic.Uint64
	rejectedTime   atomic.Uint64
	rejectedWeight atomic.Uint64
}

func (c *admissionCounters) record(status AdmissionStatus) {
	switch status {
	case AdmissionClampedFuture:
		c.clampedFuture.Add(1)
	case AdmissionRejectedTime:
		c.rejectedTime.Add(1)
	case AdmissionRejectedWeight:
		c.rejectedWeight.Add(1)
	}
}

func (c *admissionCounters) snapshot() AdmissionStats {
	return AdmissionStats{c.clampedFuture.Load(), c.rejectedTime.Load(), c.rejectedWeight.Load()}
}

func validateReport(times *ObservationTimes, weight float64) ReportAdmission {
	if !(weight > 0) || weight > math.MaxFloat64 {
		return ReportAdmission{Status: AdmissionRejectedWeight}
	}
	// Nonzero UTC observations at or before arrival need no normalization.
	// Strip arrival's monotonic clock for a source-wall-clock comparison. Other
	// zones and noncanonical zero values still enter the full validator.
	if times.ObservedAt.Location() == time.UTC && times.ArrivedAt.Location() == time.UTC && times.ObservedAt != (time.Time{}) && times.ArrivedAt != (time.Time{}) && !times.ObservedAt.After(times.ArrivedAt.UTC()) {
		return ReportAdmission{EffectiveObservation: times.ObservedAt, Status: AdmissionAccepted}
	}
	return validateObservationTimes(times)
}

func validateObservationTimes(times *ObservationTimes) ReportAdmission {
	if times.ArrivedAt.IsZero() {
		return ReportAdmission{Status: AdmissionRejectedTime}
	}
	arrived := times.ArrivedAt.UTC()
	observed := times.ObservedAt.UTC()
	if observed.IsZero() {
		observed = arrived
	}
	status := AdmissionAccepted
	if observed.After(arrived) {
		// Sub saturates huge durations safely. Compare before Unix-second
		// normalization so +120s+1ns cannot enter the permitted interval.
		if observed.Sub(arrived) > maxFutureObservationSkew {
			return ReportAdmission{Status: AdmissionRejectedTime}
		}
		observed = arrived
		status = AdmissionClampedFuture
	}
	return ReportAdmission{EffectiveObservation: observed, Status: status}
}
