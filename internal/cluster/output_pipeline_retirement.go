// File role: Owns the output shutdown boundary and bounded classifier grace.
// Crawler notes: Runtime stop starts expiry even if transferred processing is
// blocked. Completion joins that processing before downstream resources close.
// Related docs: docs/command-fix-scope-ledger-v3.md, docs/decisions/ADR-0274-terminal-classifier-retirement.md.
package cluster

import (
	"log"
	"time"

	"dxcluster/internal/toxicity"
)

// initLifecycle also supports package-local pipelines constructed directly by
// tests. Runtime construction installs these channels before launching run.
func (p *outputPipeline) initLifecycle() {
	p.lifecycleOnce.Do(func() {
		p.stopCh = make(chan struct{})
		p.done = make(chan struct{})
		p.graceFinished = make(chan struct{})
		p.classifierStopped = make(chan struct{})
	})
}

// beginRetirement publishes one stop and starts one immutable grace budget.
// Its timer owner only cancels/joins the classifier; it never applies results.
func (p *outputPipeline) beginRetirement() {
	p.initLifecycle()
	p.retirementOnce.Do(func() {
		p.drainDeadline = time.Now().Add(p.toxicityClassifier.DrainTimeout())
		if p.toxicityClassifier == nil {
			close(p.classifierStopped)
		} else {
			go func() {
				timer := time.NewTimer(time.Until(p.drainDeadline))
				defer timer.Stop()
				select {
				case <-p.graceFinished:
				case <-timer.C:
					log.Printf("toxicity classifier shutdown grace expired with pending=%d result_backlog=%d",
						p.toxicityClassifier.Pending(), p.toxicityClassifier.ResultBacklog())
				}
				p.toxicityClassifier.Stop()
				close(p.classifierStopped)
			}()
		}
		close(p.stopCh)
	})
}

func (p *outputPipeline) stop() {
	p.beginRetirement()
	<-p.done
}

// retire runs on the sole output consumer. A received result can finish after
// expiry; remaining HTTP work is canceled independently by the timer owner.
func (p *outputPipeline) retire() {
	p.beginRetirement()
	if p.toxicityClassifier != nil {
		p.drainToxicityPending(p.toxicityClassifier, p.drainDeadline)
	}
	close(p.graceFinished)
	<-p.classifierStopped
	p.releaseDueTemporal(time.Now().UTC(), true)
	p.releaseDueFT(time.Now().UTC(), true)
	p.stopTemporalTimer()
	p.stopFTTimer()
	if p.stabilizerEnabled {
		p.telnetStabilizer.Stop()
		<-p.stabilizerReleaseDone
	}
}

type toxicityDrainSource interface {
	Pending() int64
	ResultBacklog() int
	PendingIdle() <-chan struct{}
	Results() <-chan toxicity.Result
}

func (p *outputPipeline) drainToxicityPending(source toxicityDrainSource, deadline time.Time) {
	if !time.Now().Before(deadline) {
		return
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	idle, results := source.PendingIdle(), source.Results()
	for source.Pending() > 0 || source.ResultBacklog() > 0 {
		// Check the clock again after synchronous processing. Terminal Stop may
		// still be joining a client, so its completion alone cannot gate transfer.
		if !time.Now().Before(deadline) {
			return
		}
		select {
		case <-timer.C:
			return
		case <-p.classifierStopped:
			return
		case <-idle:
			// The last result can be consumed before its worker clears pending.
			// Recheck both counts after the worker completes.
		case result := <-results:
			if !p.processToxicityResult(result) {
				return
			}
		}
	}
}

// canProcessToxicityResult is the post-receive processing admission boundary.
// A select can wake after expiry and choose a ready result instead of a timer;
// that value is consumer-owned but is discarded before applying it to a spot.
func (p *outputPipeline) canProcessToxicityResult(now time.Time) bool {
	select {
	case <-p.stopCh:
		return now.Before(p.drainDeadline)
	default:
		return true
	}
}

func (p *outputPipeline) processToxicityResult(result toxicity.Result) bool {
	if !p.canProcessToxicityResult(time.Now()) {
		return false
	}
	// Once admitted, processing/handoff owns this result even after expiry.
	toxicity.ApplyResult(result)
	p.processSpot(result.Spot, nil)
	return true
}

// retireOutputPipeline is shared by normal shutdown and partial-startup close.
// Primary dedup Stop is not idempotent and does not close output; do not use
// that channel as a completion signal or close it from its consumer.
func (r *clusterRuntime) retireOutputPipeline() {
	r.outputRetirement.Do(func() {
		// A stopped primary still has an open input buffer. Retire peer admission
		// first so a successful late enqueue cannot authorize transit into a dead
		// local pipeline. Control withdrawal remains owned by Manager.Stop.
		if r.peerIngress != nil {
			r.peerIngress.Close()
		}
		if r.deduplicator != nil {
			r.deduplicator.Stop()
		}
		if r.outputPipeline != nil {
			r.outputPipeline.stop()
		} else if r.toxicityClassifier != nil {
			r.toxicityClassifier.Stop()
		}
	})
}
