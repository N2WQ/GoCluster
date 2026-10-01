//go:build qualification

package cluster

import (
	"fmt"
	"time"
)

func (o *qualificationOracle) measurementNow() time.Time {
	if o.clockNow != nil {
		return o.clockNow()
	}
	return time.Now()
}

func (o *qualificationOracle) measurementBase() time.Time {
	if o.clockNow != nil {
		return o.measurementEpoch
	}
	return o.epoch
}

// Raw QPC ticks are carried through the framing oracle without conversion.
// Only interval accounting converts them. One extra counter tick and ceiling
// division conservatively cover documented cross-thread ordering uncertainty.
func qualificationTickDuration(ticks time.Duration, frequency int64, upper bool) time.Duration {
	if frequency == 0 {
		return ticks
	}
	n := int64(ticks)
	if upper {
		n++
	}
	seconds, remainder := n/frequency, n%frequency
	ns := remainder * int64(time.Second)
	if upper {
		ns += frequency - 1
	}
	return time.Duration(seconds)*time.Second + time.Duration(ns/frequency)
}

func (h *qualificationHistogram) observeCounter(ticks time.Duration, frequency int64) {
	upper := qualificationTickDuration(ticks, frequency, true)
	h.observe(upper)
	if frequency == 0 {
		return
	}
	// These extra diagnostics never relax acceptance. They distinguish an
	// actual threshold exceedance from a case where only the mandatory QPC
	// uncertainty allowance crosses the threshold. Compare ticks as integers.
	if upper > 5*time.Millisecond && int64(ticks) <= frequency*5/1000 {
		h.marginCross5.Add(1)
	}
	if upper > 25*time.Millisecond && int64(ticks) <= frequency*25/1000 {
		h.marginCross25.Add(1)
	}
}

// The child independently checks every published spot's required enqueue bit.
// Only bounded cohort summaries cross RPC; there is no lossy event channel.
func (o *qualificationOracle) enqueueResults(used int) ([]qualificationRecipientResult, error) {
	if used < 0 || used > len(o.inputs) {
		return nil, fmt.Errorf("invalid final input count %d", used)
	}
	out := make([]qualificationRecipientResult, len(o.clients))
	for i, recipient := range o.clients {
		r := &out[i]
		r.Name, r.Renamed = recipient.name, recipient.renamed.Load()
		r.Cohorts = make([]qualificationCohort, len(recipient.enqueueLatency))
		for cohort := range r.Cohorts {
			r.Cohorts[cohort] = qualificationCohort{InputMinute: cohort - 1, Enqueue: recipient.enqueueLatency[cohort].result()}
		}
		for id := 0; id < used; id++ {
			in := &o.inputs[id]
			stamp := in.started.Load()
			if stamp <= 0 {
				return nil, fmt.Errorf("unpublished declared input %d", id)
			}
			if !in.spot || (in.client >= 0 && in.client != i) {
				continue
			}
			r.Required++
			if qualificationHas(recipient.enqueue, id) {
				r.Received++
			} else {
				r.MissingEnqueue++
			}
			cohort := int(qualificationTickDuration(time.Duration(stamp-1), o.clockFrequency, false)/time.Minute) + 1
			if cohort < 1 || cohort >= len(r.Cohorts) {
				return nil, fmt.Errorf("input cohort overflow %d", id)
			}
			r.Cohorts[0].RequiredSpots++
			r.Cohorts[cohort].RequiredSpots++
		}
	}
	return out, nil
}

func (o *qualificationOracle) acceptEnqueueResults(reply qualificationReply) error {
	if len(reply.Enqueue) != len(o.clients) {
		return fmt.Errorf("child enqueue recipient count differs")
	}
	for i, row := range reply.Enqueue {
		if row.Name != o.clients[i].name || len(row.Cohorts) != len(o.clients[i].enqueueLatency) || row.MissingEnqueue < 0 || row.Required-row.Received != row.MissingEnqueue {
			return fmt.Errorf("child enqueue accounting invalid for client%d", i)
		}
		for j, cohort := range row.Cohorts {
			if cohort.InputMinute != j-1 || cohort.RequiredSpots < 0 || int(cohort.Enqueue.Count) > cohort.RequiredSpots {
				return fmt.Errorf("child enqueue cohort invalid for client%d", i)
			}
		}
	}
	o.remoteEnqueue = reply.Enqueue
	o.failures.Add(reply.Failures)
	o.failureMu.Lock()
	defer o.failureMu.Unlock()
	for _, example := range reply.Examples {
		if len(o.examples) == 32 {
			break
		}
		o.examples = append(o.examples, "child: "+example)
	}
	return nil
}
