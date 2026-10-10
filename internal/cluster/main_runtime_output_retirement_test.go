package cluster

import (
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/dedup"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestRuntimeOutputShutdownJoinsOpenDedupOutput(t *testing.T) {
	r := runtimeRetirementRuntime(t, nil, 4)
	received := make(chan struct{})
	var once sync.Once
	r.gridUpdater = func(call, _ string) {
		if call == "K1OPEN" {
			once.Do(func() { close(received) })
		}
	}
	runtimeRetirementStart(r)
	s := runtimeRetirementSpot("K1OPEN", "CW", "open dedup output")
	runtimeRetirementSend(t, r, s)
	runtimeRetirementWait(t, "ordinary pipeline processing", received)
	stopped := runtimeRetirementCall(t, nil, r.shutdown)
	runtimeRetirementWait(t, "actual runtime shutdown", stopped)
	runtimeRetirementAssertDone(t, r)
	runtimeRetirementAssertHandoffs(t, r, s)
	select {
	case _, ok := <-r.deduplicator.GetOutputChannel():
		if !ok {
			t.Fatal("test required the production dedup output to remain open")
		}
		t.Fatal("unexpected unprocessed output after the one admitted spot")
	default:
	}
	// These converge on the same runtime owner instead of closing dedup's
	// non-idempotent Stop channel a second time.
	for range 3 {
		done := runtimeRetirementCall(t, nil, r.retireOutputPipeline)
		runtimeRetirementWait(t, "repeated retirement", done)
	}
}

func TestRuntimeOutputRetirementProcessesLateFTAfterGraceWitness(t *testing.T) {
	classifier, client := runtimeRetirementClassifier(t, "retirement alpha", "retirement bravo", "retirement ft")
	r := runtimeRetirementRuntime(t, classifier, 4)
	t.Cleanup(client.releaseAll)
	a := newRuntimeRetirementBarrier(t)
	b := newRuntimeRetirementBarrier(t)
	r.gridUpdater = func(call, _ string) {
		switch call {
		case "K1ALPHA":
			a.enter()
		case "K1BRAVO":
			b.enter()
		}
	}
	release := func() { client.releaseAll(); a.release(); b.release() }
	runtimeRetirementStart(r)
	first := runtimeRetirementSpot("K1ALPHA", "CW", "retirement alpha")
	second := runtimeRetirementSpot("K1BRAVO", "CW", "retirement bravo")
	late := runtimeRetirementSpot("K1LATE", "FT8", "retirement ft")
	for _, s := range []*spot.Spot{first, second, late} {
		runtimeRetirementSend(t, r, s)
		runtimeRetirementWait(t, "actual classifier admission for "+s.Comment, client.gates[s.Comment].entered)
	}
	client.gates[first.Comment].release()
	runtimeRetirementWait(t, "received CW-A processing barrier", a.entered)
	client.gates[second.Comment].release()
	runtimeRetirementWaitFor(t, "CW-B buffered while CW-A owns processing", func() bool {
		return classifier.ResultBacklog() == 1
	})
	stopped := runtimeRetirementCall(t, release, r.retireOutputPipeline)
	runtimeRetirementWait(t, "runtime stop request", r.outputPipeline.stopCh)
	a.release()
	// Stop was observed before A returned. The mandatory priority check now
	// prevents B from another ordinary receive, so this callback witnesses
	// classifier consumption during grace without a test-only production hook.
	runtimeRetirementWait(t, "CW-B grace processing barrier", b.entered)
	client.gates[late.Comment].release()
	runtimeRetirementWaitFor(t, "FT result buffered during CW-B grace processing", func() bool {
		return classifier.ResultBacklog() == 1
	})
	b.release()
	runtimeRetirementWait(t, "late-FT pipeline retirement", stopped)
	runtimeRetirementAssertDone(t, r)
	for _, s := range []*spot.Spot{first, second, late} {
		if s.ToxicityStatus != spot.ToxicityToxic || s.ToxicityModel != "retirement-fixture" ||
			!reflect.DeepEqual(s.ToxicityCategories, []string{"S10"}) {
			t.Fatalf("result was not normally applied: %+v", s)
		}
	}
	if late.Confidence != "?" {
		t.Fatalf("single-reporter FT hold did not follow its normal release rail: %q", late.Confidence)
	}
	if client.calls.Load() != 3 {
		t.Fatalf("unexpected classifier calls: %d", client.calls.Load())
	}
	runtimeRetirementAssertHandoffs(t, r, first, second, late)
	if got := r.spotBuffer.GetRecent(3); len(got) != 3 {
		t.Fatalf("expected three processed spots, got %d", len(got))
	}
}

func TestRuntimeOutputRetirementPreservesDeliveryGates(t *testing.T) {
	for _, policy := range []string{"normal", "MED suppression", "queue saturation"} {
		t.Run(policy, func(t *testing.T) {
			classifier, client := runtimeRetirementClassifier(t, "retirement policy")
			r := runtimeRetirementRuntime(t, classifier, 1)
			t.Cleanup(client.releaseAll)
			s := runtimeRetirementSpot("K1POL", "CW", "retirement policy")
			sentinel := runtimeRetirementSpot("K1OLD", "CW", "queue sentinel")
			switch policy {
			case "MED suppression":
				r.secondaryMed = dedup.NewSecondaryDeduper(time.Minute, false)
				if !r.secondaryMed.ShouldForward(s) {
					t.Fatal("positive MED seed did not pass")
				}
			case "queue saturation":
				r.archiveWriter.Enqueue(sentinel)
				r.telnetServer.BroadcastSpot(sentinel, true, true, true)
			}
			runtimeRetirementStart(r)
			runtimeRetirementSend(t, r, s)
			runtimeRetirementWait(t, "queued policy classification", client.gates[s.Comment].entered)
			stopped := runtimeRetirementCall(t, client.releaseAll, r.shutdown)
			runtimeRetirementWait(t, "policy stop request", r.outputPipeline.stopCh)
			client.releaseAll()
			runtimeRetirementWait(t, "policy retirement", stopped)
			runtimeRetirementAssertDone(t, r)
			if s.ToxicityStatus != spot.ToxicityToxic || r.secondaryStageCount.Load() != 1 {
				t.Fatalf("policy result bypassed normal processing: toxicity=%s delivery=%d", s.ToxicityStatus, r.secondaryStageCount.Load())
			}
			queueDrops, clientDrops, senderFailures := r.telnetServer.BroadcastMetricSnapshot()
			switch policy {
			case "normal":
				runtimeRetirementAssertHandoffs(t, r, s)
				if r.spotBuffer.GetCount() != 1 || queueDrops != 0 {
					t.Fatal("normal final fanout did not remain accepted")
				}
			case "MED suppression":
				runtimeRetirementAssertHandoffs(t, r)
				processed, duplicates, _ := r.secondaryMed.GetStats()
				if r.spotBuffer.GetCount() != 0 || processed != 2 || duplicates != 1 || queueDrops != 0 {
					t.Fatalf("wrong actual suppression: buffer=%d processed=%d duplicate=%d drops=%d",
						r.spotBuffer.GetCount(), processed, duplicates, queueDrops)
				}
			case "queue saturation":
				// Unstarted bounded consumers make saturation deterministic. This
				// proves attempted handoff/drop behavior, not socket receipt.
				runtimeRetirementAssertHandoffs(t, r, sentinel)
				if r.spotBuffer.GetCount() != 1 || queueDrops != 1 || r.lastOutput.Load() == 0 {
					t.Fatalf("saturation lost its normal attempt/drop semantics: buffer=%d drops=%d last=%d",
						r.spotBuffer.GetCount(), queueDrops, r.lastOutput.Load())
				}
			}
			if clientDrops != 0 || senderFailures != 0 || client.calls.Load() != 1 {
				t.Fatal("fixture fabricated client delivery or repeated classification")
			}
		})
	}
}

func TestRuntimeOutputCloseAfterListenerFailureWaitsForPipeline(t *testing.T) {
	port := runtimeRetirementReservePort(t)
	classifier, client := runtimeRetirementClassifier(t, "retirement startup")
	r := runtimeRetirementRuntime(t, classifier, 4)
	t.Cleanup(client.releaseAll)
	r.telnetServer.Stop()
	r.telnetServer = telnet.NewServer(telnet.ServerOptions{
		Port: port, BroadcastQueue: 4,
	}, nil)
	callback := newRuntimeRetirementBarrier(t)
	r.gridUpdater = func(call, _ string) {
		if call == "K1START" {
			callback.enter()
		}
	}
	var gridStopped atomic.Bool
	r.stopGridWriter = func() { gridStopped.Store(true) }
	archiveStopped, serverStopped := runtimeRetirementResourceSignals(r)
	runtimeRetirementStart(r)
	s := runtimeRetirementSpot("K1START", "CW", "retirement startup")
	runtimeRetirementSend(t, r, s)
	runtimeRetirementWait(t, "startup classifier entry", client.gates[s.Comment].entered)
	client.releaseAll()
	runtimeRetirementWait(t, "startup transferred processing", callback.entered)
	if r.startNetworkServices() || r.startupErr == nil || !strings.Contains(r.startupErr.Error(), "Failed to start telnet server") {
		t.Fatalf("occupied listener did not fail the actual startup boundary: %v", r.startupErr)
	}
	closed := runtimeRetirementCall(t, func() { client.releaseAll(); callback.release() }, r.close)
	runtimeRetirementWait(t, "partial startup stop request", r.outputPipeline.stopCh)
	// Give cleanup an execution opportunity while the callback remains owned;
	// checking one instant immediately after stopCh closes could miss a skipped
	// join whose dependency teardown has not yet been scheduled.
	select {
	case <-closed:
		t.Fatal("runtime close returned while transferred processing was blocked")
	case <-archiveStopped:
		t.Fatal("archive stopped while transferred processing was blocked")
	case <-serverStopped:
		t.Fatal("telnet stopped while transferred processing was blocked")
	case <-r.ctx.Done():
		t.Fatal("parent context canceled while transferred processing was blocked")
	case <-time.After(25 * time.Millisecond):
	}
	if gridStopped.Load() {
		t.Fatal("grid dependency retired while its pipeline callback was blocked")
	}
	callback.release()
	runtimeRetirementWait(t, "failed-startup runtime close", closed)
	runtimeRetirementAssertDone(t, r)
	runtimeRetirementWait(t, "archive dependency stop", archiveStopped)
	runtimeRetirementWait(t, "telnet dependency stop", serverStopped)
	if !gridStopped.Load() || s.ToxicityStatus != spot.ToxicityToxic {
		t.Fatal("partial startup did not finish received processing before dependencies")
	}
	runtimeRetirementAssertHandoffs(t, r, s)
}

func TestRuntimeOutputCloseBeforePipelineRetiresClassifier(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "constructed"
		if started {
			name = "workers started"
		}
		t.Run(name, func(t *testing.T) {
			classifier, client := runtimeRetirementClassifier(t, "retirement partial")
			r := runtimeRetirementRuntime(t, classifier, 4)
			t.Cleanup(client.releaseAll)
			s := runtimeRetirementSpot("K1PART", "CW", "retirement partial")
			if classifier.ClassifyOrEnqueue(s, time.Now().UTC()) || classifier.Pending() != 1 {
				t.Fatal("classifier-before-pipeline fixture did not own accepted work")
			}
			if started {
				classifier.Start()
				runtimeRetirementWait(t, "partial-startup active classifier", client.gates[s.Comment].entered)
			}
			if r.outputPipeline != nil {
				t.Fatal("fixture unexpectedly launched the output owner")
			}
			closed := runtimeRetirementCall(t, client.releaseAll, r.close)
			runtimeRetirementWait(t, "classifier-before-pipeline cleanup", closed)
			if classifier.Pending() != 0 || classifier.ResultBacklog() != 0 ||
				spot.NormalizeToxicityStatus(string(s.ToxicityStatus)) != spot.ToxicityUnknown {
				t.Fatalf("partial cleanup left work or invented a spot outcome: pending=%d backlog=%d spot=%+v",
					classifier.Pending(), classifier.ResultBacklog(), s)
			}
			classifier.Start()
			post := runtimeRetirementSpot("K1POST", "CW", "retirement post stop")
			if !classifier.ClassifyOrEnqueue(post, time.Now().UTC()) || post.ToxicityStatus != spot.ToxicityUnavailable {
				t.Fatal("partial runtime cleanup did not leave the classifier terminal")
			}
			wantCalls := int64(0)
			if started {
				wantCalls = 1
			}
			if client.calls.Load() != wantCalls || classifier.Pending() != 0 {
				t.Fatal("partial cleanup allowed classifier revival")
			}
		})
	}
}

func TestRuntimeOutputCloseJoinsStabilizerReleaseConsumer(t *testing.T) {
	r := runtimeRetirementRuntime(t, nil, 4)
	runtimeRetirementConfigureStabilizer(r, 1, 1, stabilizerTimeoutRelease)
	callback := newRuntimeRetirementBarrier(t)
	r.gridUpdater = func(call, _ string) {
		if call == "K1RELEASE" {
			callback.enter()
		}
	}
	var gridStopped atomic.Bool
	r.stopGridWriter = func() { gridStopped.Store(true) }
	archiveStopped, serverStopped := runtimeRetirementResourceSignals(r)
	runtimeRetirementStart(r)
	s := runtimeRetirementSpot("K1RELEASE", "CW", "actual stabilizer release")
	runtimeRetirementSend(t, r, s)
	// Only the actual configured one-second scheduler can reach this callback:
	// the initial delivery is held before grid updates and final fanout.
	runtimeRetirementWait(t, "real scheduled release callback", callback.entered)
	if r.statsTracker.StabilizerHeld() != 1 || r.statsTracker.StabilizerReleasedDelayed() != 1 || r.spotBuffer.GetCount() != 0 {
		t.Fatal("fixture did not reach the actual held-to-released consumer boundary")
	}
	closed := runtimeRetirementCall(t, callback.release, r.close)
	p := r.outputPipeline
	runtimeRetirementWait(t, "runtime stop for released envelope", p.stopCh)
	runtimeRetirementWait(t, "scheduler completion while release processing is owned", p.telnetStabilizer.done)
	select {
	case <-p.done:
		t.Fatal("pipeline joined only the scheduler while release processing was blocked")
	case <-p.stabilizerReleaseDone:
		t.Fatal("release consumer reported completion while its callback was blocked")
	case <-closed:
		t.Fatal("runtime close returned before release processing completed")
	case <-archiveStopped:
		t.Fatal("archive stopped before release processing completed")
	case <-serverStopped:
		t.Fatal("telnet stopped before release processing completed")
	case <-r.ctx.Done():
		t.Fatal("parent context canceled before release processing completed")
	case <-time.After(25 * time.Millisecond):
	}
	if gridStopped.Load() {
		t.Fatal("grid dependency stopped before release processing completed")
	}
	callback.release()
	runtimeRetirementWait(t, "runtime close after released processing", closed)
	runtimeRetirementAssertDone(t, r)
	runtimeRetirementAssertHandoffs(t, r, s)
	if !gridStopped.Load() || r.spotBuffer.GetCount() != 1 {
		t.Fatal("released envelope did not finish normal fanout before dependencies")
	}
}

func TestRuntimeOutputRetirementDropsHeldWorkAndFencesRetry(t *testing.T) {
	for _, tt := range []struct {
		name      string
		checks    int
		action    string
		forwarded bool
	}{
		{"retry release fails open", 3, stabilizerTimeoutRelease, true},
		{"retry suppress fails open", 3, stabilizerTimeoutSuppress, true},
		{"exhausted suppress remains suppressed", 1, stabilizerTimeoutSuppress, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := runtimeRetirementRuntime(t, nil, 4)
			runtimeRetirementConfigureStabilizer(r, 60, tt.checks, tt.action)
			runtimeRetirementStart(r)
			held := runtimeRetirementSpot("K1HELD", "CW", "unreleased long hold")
			runtimeRetirementSend(t, r, held)
			runtimeRetirementWaitFor(t, "actual long stabilizer hold", func() bool {
				return r.statsTracker.StabilizerHeld() == 1 && r.outputPipeline.telnetStabilizer.Pending() == 1
			})
			stopped := runtimeRetirementCall(t, nil, r.retireOutputPipeline)
			runtimeRetirementWait(t, "runtime retirement of long-held work", stopped)
			runtimeRetirementAssertDone(t, r)
			runtimeRetirementAssertHandoffs(t, r)
			if r.spotBuffer.GetCount() != 0 || r.statsTracker.StabilizerReleasedDelayed() != 0 {
				t.Fatal("unreleased stabilizer work was forced into final delivery")
			}
			p := r.outputPipeline
			retry := runtimeRetirementSpot("K1RETRY", "CW", "terminal risky retry")
			retry.Confidence = "?"
			decision := evaluateTelnetStabilizerDelay(retry, r.recentBandStore, r.cfg.CallCorrection, time.Now().UTC(), spot.ResolverSnapshot{}, false)
			if !decision.ShouldDelay || decision.MaxChecks != tt.checks || shouldRetryTelnetByStabilizer(decision, 1) != tt.forwarded {
				t.Fatal("fixture did not exercise the selected risky retry/exhaustion branch")
			}
			if p.telnetStabilizer.Enqueue(retry) || p.telnetStabilizer.Pending() != 0 {
				t.Fatal("terminal scheduler accepted another held reference")
			}
			// Consumer-branch control only: after the actual runtime/scheduler
			// join, invoke the existing handler with a released risky envelope.
			// Dependencies are still available because retirement alone leaves
			// them open. This does not claim post-completion production ingress.
			p.handleStabilizerRelease(&telnetStabilizerEnvelope{
				spot: retry, delayReason: stabilizerDelayReasonUnknownOrNonRecent.String(),
			})
			if tt.forwarded {
				runtimeRetirementAssertHandoffs(t, r, retry)
				if r.statsTracker.StabilizerOverflowRelease() != 1 || r.statsTracker.StabilizerReleasedDelayed() != 1 ||
					r.statsTracker.StabilizerSuppressedTimeout() != 0 || r.spotBuffer.GetCount() != 1 {
					t.Fatal("terminal retry refusal did not preserve existing overflow fail-open outcome")
				}
			} else {
				runtimeRetirementAssertHandoffs(t, r)
				if r.statsTracker.StabilizerOverflowRelease() != 0 || r.statsTracker.StabilizerReleasedDelayed() != 0 ||
					r.statsTracker.StabilizerSuppressedTimeout() != 1 || r.spotBuffer.GetCount() != 0 {
					t.Fatal("exhausted risky envelope did not preserve normal timeout suppression")
				}
			}
			if p.telnetStabilizer.Pending() != 0 || r.statsTracker.StabilizerHeld() != 1 {
				t.Fatal("release handler revived terminal held work")
			}
		})
	}
}
