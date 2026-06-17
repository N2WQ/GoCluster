// File role: Owns the live spot output integration boundary.
// Crawler notes: Start here when a spot is delayed, mutated, dropped, archived,
// peer-forwarded, broadcast to telnet clients, counted in stats, or converted
// into path/correction/support diagnostics after primary dedupe.
// Related docs: README.md, spot/README.md, pathreliability/README.md.
// Related tests: internal/cluster/*output_pipeline*_test.go, internal/cluster/*stabilizer*_test.go.
package cluster

import (
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/internal/toxicity"
	"dxcluster/pathreliability"
	"dxcluster/peer"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
	"dxcluster/ui"
)

// outputPipeline owns the single consumer of deduplicated spots plus the
// temporal and FT corroboration timer state. This is the runtime boundary where
// a spot becomes archive/peer/telnet output, so comment changes here should
// preserve the support model: explain why a spot was delayed, mutated, dropped,
// or delivered, without splitting ownership across goroutines.
type outputPipeline struct {
	outputChan             <-chan *spot.Spot
	secondaryFast          *dedup.SecondaryDeduper
	secondaryMed           *dedup.SecondaryDeduper
	secondarySlow          *dedup.SecondaryDeduper
	secondaryStage         *atomic.Uint64
	modeAssigner           *spot.ModeAssigner
	buf                    *buffer.RingBuffer
	telnet                 *telnet.Server
	peerManager            *peer.Manager
	tracker                *stats.Tracker
	signalResolver         *spot.SignalResolver
	correctionCfg          config.CallCorrectionConfig
	ctyLookup              func() *cty.CTYDatabase
	metaCache              *callMetaCache
	harmonicDetector       *spot.HarmonicDetector
	harmonicCfg            config.HarmonicConfig
	freqAvg                *spot.FrequencyAverager
	spotPolicy             config.SpotPolicy
	dash                   ui.Surface
	gridUpdate             func(call, grid string)
	gridLookup             func(call string) (string, bool, bool)
	gridLookupSync         func(call string) (string, bool, bool)
	unlicensedReporter     func(source, role, call, deCall, dxCall, mode string, freq float64)
	droppedCallLogger      *droppedCallLogger
	adaptiveMinReports     *spot.AdaptiveMinReports
	refresher              *adaptiveRefresher
	spotterReliability     spot.SpotterReliability
	spotterReliabilityCW   spot.SpotterReliability
	spotterReliabilityRTTY spot.SpotterReliability
	confusionModel         *spot.ConfusionModel
	recentBandStore        spot.RecentSupportStore
	customSCPStore         *spot.CustomSCPStore
	whoSpotsMeStore        *spot.WhoSpotsMeStore
	broadcastKeepSSID      bool
	archiveWriter          *archive.Writer
	lastOutput             *atomic.Int64
	pathPredictor          *pathreliability.Predictor
	pathReport             *pathReportMetrics
	allowedBands           map[string]struct{}
	secondaryActive        bool
	stabilizerEnabled      bool
	telnetStabilizer       *telnetSpotStabilizer
	familySuppressor       *telnetFamilySuppressor
	temporal               *runtimeTemporalController
	temporalTimer          *time.Timer
	temporalTimerCh        <-chan time.Time
	ftConfidence           *ftConfidenceController
	ftRecentBandStore      *spot.RecentBandStore
	ftTimer                *time.Timer
	ftTimerCh              <-chan time.Time
	ftTimerDue             time.Time
	toxicityClassifier     *toxicity.Classifier
}

// outputSpotContext carries mutable spot state and resolver context through the
// staged output path. The intent is to keep support-critical side effects
// visible in one object instead of hiding them in package-level state.
type outputSpotContext struct {
	spot                       *spot.Spot
	ctyDB                      *cty.CTYDatabase
	dirty                      bool
	modeUpper                  string
	stabilizerResolverKey      spot.ResolverSignalKey
	hasStabilizerResolverKey   bool
	stabilizerEvidenceEnqueued bool
}

// newOutputPipeline wires optional runtime stages from config. Disabled stages
// remain nil rather than no-op wrappers so troubleshooting can map missing
// behavior directly to config and startup wiring.
func newOutputPipeline(
	deduplicator *dedup.Deduplicator,
	secondaryFast *dedup.SecondaryDeduper,
	secondaryMed *dedup.SecondaryDeduper,
	secondarySlow *dedup.SecondaryDeduper,
	secondaryStage *atomic.Uint64,
	modeAssigner *spot.ModeAssigner,
	buf *buffer.RingBuffer,
	telnet *telnet.Server,
	peerManager *peer.Manager,
	tracker *stats.Tracker,
	signalResolver *spot.SignalResolver,
	correctionCfg config.CallCorrectionConfig,
	ctyLookup func() *cty.CTYDatabase,
	metaCache *callMetaCache,
	harmonicDetector *spot.HarmonicDetector,
	harmonicCfg config.HarmonicConfig,
	freqAvg *spot.FrequencyAverager,
	spotPolicy config.SpotPolicy,
	dash ui.Surface,
	gridUpdate func(call, grid string),
	gridLookup func(call string) (string, bool, bool),
	gridLookupSync func(call string) (string, bool, bool),
	unlicensedReporter func(source, role, call, deCall, dxCall, mode string, freq float64),
	droppedCallLogger *droppedCallLogger,
	adaptiveMinReports *spot.AdaptiveMinReports,
	refresher *adaptiveRefresher,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
	recentBandStore spot.RecentSupportStore,
	customSCPStore *spot.CustomSCPStore,
	whoSpotsMeStore *spot.WhoSpotsMeStore,
	broadcastKeepSSID bool,
	archiveWriter *archive.Writer,
	lastOutput *atomic.Int64,
	pathPredictor *pathreliability.Predictor,
	pathReport *pathReportMetrics,
	allowedBands map[string]struct{},
	toxicityClassifier *toxicity.Classifier,
) *outputPipeline {
	pipeline := &outputPipeline{
		outputChan:             deduplicator.GetOutputChannel(),
		secondaryFast:          secondaryFast,
		secondaryMed:           secondaryMed,
		secondarySlow:          secondarySlow,
		secondaryStage:         secondaryStage,
		modeAssigner:           modeAssigner,
		buf:                    buf,
		telnet:                 telnet,
		peerManager:            peerManager,
		tracker:                tracker,
		signalResolver:         signalResolver,
		correctionCfg:          correctionCfg,
		ctyLookup:              ctyLookup,
		metaCache:              metaCache,
		harmonicDetector:       harmonicDetector,
		harmonicCfg:            harmonicCfg,
		freqAvg:                freqAvg,
		spotPolicy:             spotPolicy,
		dash:                   dash,
		gridUpdate:             gridUpdate,
		gridLookup:             gridLookup,
		gridLookupSync:         gridLookupSync,
		unlicensedReporter:     unlicensedReporter,
		droppedCallLogger:      droppedCallLogger,
		adaptiveMinReports:     adaptiveMinReports,
		refresher:              refresher,
		spotterReliability:     spotterReliability,
		spotterReliabilityCW:   spotterReliabilityCW,
		spotterReliabilityRTTY: spotterReliabilityRTTY,
		confusionModel:         confusionModel,
		recentBandStore:        recentBandStore,
		customSCPStore:         customSCPStore,
		whoSpotsMeStore:        whoSpotsMeStore,
		broadcastKeepSSID:      broadcastKeepSSID,
		archiveWriter:          archiveWriter,
		lastOutput:             lastOutput,
		pathPredictor:          pathPredictor,
		pathReport:             pathReport,
		allowedBands:           allowedBands,
		toxicityClassifier:     toxicityClassifier,
		secondaryActive:        secondaryFast != nil || secondaryMed != nil || secondarySlow != nil,
		temporal:               newRuntimeTemporalController(correctionCfg),
		ftConfidence:           newFTConfidenceController(correctionCfg),
		ftRecentBandStore:      newFTRecentBandStore(correctionCfg),
	}
	pipeline.stabilizerEnabled = telnet != nil && correctionCfg.Enabled && correctionCfg.StabilizerEnabled && recentBandStore != nil
	if pipeline.stabilizerEnabled {
		pipeline.telnetStabilizer = newTelnetSpotStabilizer(
			time.Duration(correctionCfg.StabilizerDelaySeconds)*time.Second,
			correctionCfg.StabilizerMaxPending,
		)
		pipeline.telnetStabilizer.Start()
	}
	if telnet != nil && correctionCfg.Enabled && correctionCfg.FamilyPolicy.TelnetSuppression.Enabled {
		pipeline.familySuppressor = newTelnetFamilySuppressor(
			time.Duration(correctionCfg.FamilyPolicy.TelnetSuppression.WindowSeconds)*time.Second,
			correctionCfg.FamilyPolicy.TelnetSuppression.MaxEntries,
			spot.CorrectionFamilyPolicy{
				Configured:                 true,
				TruncationEnabled:          correctionCfg.FamilyPolicy.Truncation.Enabled,
				TruncationMaxLengthDelta:   correctionCfg.FamilyPolicy.Truncation.MaxLengthDelta,
				TruncationMinShorterLength: correctionCfg.FamilyPolicy.Truncation.MinShorterLength,
				TruncationAllowPrefix:      correctionCfg.FamilyPolicy.Truncation.AllowPrefixMatch,
				TruncationAllowSuffix:      correctionCfg.FamilyPolicy.Truncation.AllowSuffixMatch,
			},
			correctionCfg.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz,
		)
	}
	return pipeline
}

// run advances timer-backed stages before every select so idle ingest still
// releases bounded temporal and FT holds. On input close it force-drains held
// work before returning to avoid silently losing already accepted spots.
func (p *outputPipeline) run() {
	if p.stabilizerEnabled {
		defer p.telnetStabilizer.Stop()
		p.startStabilizerReleaseLoop()
	}
	defer p.stopTemporalTimer()
	defer p.stopFTTimer()
	if p.toxicityClassifier != nil {
		defer p.toxicityClassifier.Stop()
	}

	for {
		now := time.Now().UTC()
		p.releaseDueTemporal(now, false)
		p.scheduleTemporalTimer(now)
		p.releaseDueFT(now, false)
		p.scheduleFTTimer(now)

		select {
		case s, ok := <-p.outputChan:
			if !ok {
				p.releaseDueTemporal(time.Now().UTC().Add(24*time.Hour), true)
				p.releaseDueFT(time.Now().UTC().Add(24*time.Hour), true)
				p.drainToxicityPending(time.Now().UTC().Add(p.toxicityClassifier.DrainTimeout()))
				return
			}
			p.processSpot(s, nil)
		case result := <-p.toxicityResults():
			toxicity.ApplyResult(result)
			p.processSpot(result.Spot, nil)
		case <-p.temporalTimerCh:
			// Timer-driven wakeup for lag/max-wait release when ingest is idle.
		case <-p.ftTimerCh:
			// Timer-driven wakeup for bounded FT corroboration release when ingest is idle.
		}
	}
}

func (p *outputPipeline) drainToxicityPending(deadline time.Time) {
	if p == nil || p.toxicityClassifier == nil {
		return
	}
	for p.toxicityClassifier.Pending() > 0 || p.toxicityClassifier.ResultBacklog() > 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			log.Printf("toxicity classifier shutdown drain timed out with pending=%d result_backlog=%d",
				p.toxicityClassifier.Pending(),
				p.toxicityClassifier.ResultBacklog(),
			)
			return
		}
		select {
		case result := <-p.toxicityResults():
			toxicity.ApplyResult(result)
			p.processSpot(result.Spot, nil)
		case <-time.After(remaining):
			log.Printf("toxicity classifier shutdown drain timed out with pending=%d result_backlog=%d",
				p.toxicityClassifier.Pending(),
				p.toxicityClassifier.ResultBacklog(),
			)
			return
		}
	}
}

func (p *outputPipeline) toxicityResults() <-chan toxicity.Result {
	if p == nil || p.toxicityClassifier == nil {
		return nil
	}
	return p.toxicityClassifier.Results()
}

// processSpot shields the long-lived output loop from one malformed spot or
// unexpected stage panic. The recovery keeps the cluster alive while preserving
// the stack needed for operator triage.
func (p *outputPipeline) processSpot(s *spot.Spot, temporalRelease *runtimeTemporalRelease) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("processOutputSpots panic: %v\n%s", r, debug.Stack())
		}
	}()
	p.processSpotBody(s, temporalRelease)
}

// processSpotBody is intentionally a linear stage list: each stage answers one
// support question about how a spot changed before fanout.
func (p *outputPipeline) processSpotBody(s *spot.Spot, temporalRelease *runtimeTemporalRelease) {
	if !p.applyToxicityStage(s) {
		return
	}
	ctx, ok := p.prepareSpotContext(s)
	if !ok {
		return
	}
	if !p.applyResolverStage(&ctx, temporalRelease) {
		return
	}
	if !p.applyPostResolverAdjustments(&ctx) {
		return
	}
	if !p.applyFTConfidenceStage(&ctx, time.Now().UTC()) {
		return
	}
	if !p.finalizeSpotForMetrics(&ctx) {
		return
	}
	if !p.prepareFanoutSpot(&ctx) {
		return
	}
	p.deliverSpot(&ctx)
}

// stopTemporalTimer clears both the timer and exposed channel so the main
// select cannot wake on stale timer state after the temporal stage is disabled
// or drained.
func (p *outputPipeline) stopTemporalTimer() {
	if p.temporalTimer == nil {
		return
	}
	if !p.temporalTimer.Stop() {
		select {
		case <-p.temporalTimer.C:
		default:
		}
	}
	p.temporalTimer = nil
	p.temporalTimerCh = nil
}

// scheduleTemporalTimer mirrors the next pending temporal deadline into the
// output loop's select. A zero or overdue delay is deliberate: release promptly
// without spinning in a separate goroutine.
func (p *outputPipeline) scheduleTemporalTimer(now time.Time) {
	if p.temporal == nil || !p.temporal.Enabled() {
		p.stopTemporalTimer()
		return
	}
	nextDue, ok := p.temporal.NextDue()
	if !ok {
		p.stopTemporalTimer()
		return
	}
	delay := nextDue.Sub(now)
	if delay < 0 {
		delay = 0
	}
	if p.temporalTimer == nil {
		p.temporalTimer = time.NewTimer(delay)
	} else {
		if !p.temporalTimer.Stop() {
			select {
			case <-p.temporalTimer.C:
			default:
			}
		}
		p.temporalTimer.Reset(delay)
	}
	p.temporalTimerCh = p.temporalTimer.C
}
