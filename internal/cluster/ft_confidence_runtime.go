// File role: FT confidence burst controller for the output pipeline. It owns
// bounded pending FT groups, due timers, and fail-open release semantics before
// spots cross asynchronous fanout boundaries.
package cluster

import (
	"math"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/strutil"
)

const (
	ftConfidenceMaxPendingGroups      = 12000
	ftConfidenceMaxPendingSpots       = 30000
	ftConfidenceRecentMaxEntries      = 200000
	ftConfidenceRecentMaxSpotters     = 16
	ftConfidenceRecentCleanupInterval = 10 * time.Minute
)

type ftConfidenceTiming struct {
	quietGap time.Duration
	hardCap  time.Duration
}

// ftConfidencePolicy is separated from config so the runtime stage can enforce
// safe defaults even when older configs omit newer FT corroboration knobs.
type ftConfidencePolicy struct {
	pMinUniqueSpotters int
	vMinUniqueSpotters int
	ft8Timing          ftConfidenceTiming
	ft4Timing          ftConfidenceTiming
	ft2Timing          ftConfidenceTiming
}

// ftConfidenceKey groups only reports that can corroborate the same FT display
// claim: same normalized call, timing family, and tight frequency bucket.
type ftConfidenceKey struct {
	call    string
	mode    string
	freqKey int64
}

// ftConfidencePendingGroup is the bounded burst being held until either the
// quiet gap suggests the burst ended or the hard cap forces release.
type ftConfidencePendingGroup struct {
	key          ftConfidenceKey
	firstSeen    time.Time
	lastSeen     time.Time
	hardDeadline time.Time
	due          time.Time
	seq          uint64
	contexts     []ftConfidencePendingContext
	spotters     map[string]struct{}
}

// ftConfidencePendingContext preserves just enough output context to release a
// held FT spot without rerunning upstream resolver stages.
type ftConfidencePendingContext struct {
	spot                       *spot.Spot
	ctyDB                      *cty.CTYDatabase
	modeUpper                  string
	stabilizerResolverKey      spot.ResolverSignalKey
	hasStabilizerResolverKey   bool
	stabilizerEvidenceEnqueued bool
}

func newFTConfidencePendingContext(ctx outputSpotContext) ftConfidencePendingContext {
	return ftConfidencePendingContext{
		spot:                       ctx.spot,
		ctyDB:                      ctx.ctyDB,
		modeUpper:                  ctx.modeUpper,
		stabilizerResolverKey:      ctx.stabilizerResolverKey,
		hasStabilizerResolverKey:   ctx.hasStabilizerResolverKey,
		stabilizerEvidenceEnqueued: ctx.stabilizerEvidenceEnqueued,
	}
}

func (ctx ftConfidencePendingContext) outputContext() outputSpotContext {
	return outputSpotContext{
		spot:                       ctx.spot,
		ctyDB:                      ctx.ctyDB,
		modeUpper:                  ctx.modeUpper,
		stabilizerResolverKey:      ctx.stabilizerResolverKey,
		hasStabilizerResolverKey:   ctx.hasStabilizerResolverKey,
		stabilizerEvidenceEnqueued: ctx.stabilizerEvidenceEnqueued,
	}
}

type ftConfidenceItem struct {
	key ftConfidenceKey
	due time.Time
	seq uint64
}

// ftConfidenceHeap is implemented locally instead of container/heap so the
// single output goroutine can avoid interface conversions in this hot path.
type ftConfidenceHeap []ftConfidenceItem

func (h ftConfidenceHeap) Len() int { return len(h) }

func (h ftConfidenceHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}

func (h ftConfidenceHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ftConfidenceHeap) push(item ftConfidenceItem) {
	if h == nil {
		return
	}
	*h = append(*h, item)
	h.siftUp(len(*h) - 1)
}

func (h *ftConfidenceHeap) pop() (ftConfidenceItem, bool) {
	if h == nil || h.Len() == 0 {
		return ftConfidenceItem{}, false
	}
	items := *h
	item := items[0]
	last := len(items) - 1
	if last == 0 {
		items[0] = ftConfidenceItem{}
		*h = items[:0]
		return item, true
	}
	items[0] = items[last]
	items[last] = ftConfidenceItem{}
	*h = items[:last]
	h.siftDown(0)
	return item, true
}

func (h *ftConfidenceHeap) siftUp(index int) {
	items := *h
	for index > 0 {
		parent := (index - 1) / 2
		if !items.Less(index, parent) {
			return
		}
		items.Swap(index, parent)
		index = parent
	}
}

func (h *ftConfidenceHeap) siftDown(index int) {
	items := *h
	for {
		left := 2*index + 1
		if left >= len(items) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(items) && items.Less(right, left) {
			smallest = right
		}
		if !items.Less(smallest, index) {
			return
		}
		items.Swap(index, smallest)
		index = smallest
	}
}

func popFTConfidenceDue(h *ftConfidenceHeap, now time.Time, force bool, scratch []ftConfidenceItem) []ftConfidenceItem {
	if h == nil || h.Len() == 0 {
		return nil
	}
	out := scratch[:0]
	for h.Len() > 0 {
		head := (*h)[0]
		if !force && head.due.After(now) {
			break
		}
		if item, ok := h.pop(); ok {
			out = append(out, item)
		}
	}
	return out
}

// ftConfidenceController owns bounded FT corroboration state for the single
// output-pipeline goroutine. It groups related FT reports long enough to assign
// ?/P/V without involving resolver mutation, and it fails open when bounds are
// reached by releasing the current spot immediately.
type ftConfidenceController struct {
	enabled          bool
	maxPendingGroups int
	maxPendingSpots  int
	pendingSpots     int
	policy           ftConfidencePolicy
	pending          map[ftConfidenceKey]*ftConfidencePendingGroup
	queue            ftConfidenceHeap
	dueScratch       []ftConfidenceItem
	seq              uint64
}

func newFTConfidencePolicy(cfg config.CallCorrectionConfig) ftConfidencePolicy {
	pMin := cfg.PMinUniqueSpotters
	if pMin < 2 {
		pMin = config.DefaultCallCorrectionFTPMinUniqueSpotters
	}
	vMin := cfg.VMinUniqueSpotters
	if vMin <= pMin {
		vMin = max(config.DefaultCallCorrectionFTVMinUniqueSpotters, pMin+1)
	}
	ft8QuietGap := cfg.FT8QuietGapSeconds
	if ft8QuietGap <= 0 {
		ft8QuietGap = config.DefaultCallCorrectionFT8QuietGapSeconds
	}
	ft8HardCap := cfg.FT8HardCapSeconds
	if ft8HardCap < ft8QuietGap {
		ft8HardCap = max(config.DefaultCallCorrectionFT8HardCapSeconds, ft8QuietGap)
	}
	ft4QuietGap := cfg.FT4QuietGapSeconds
	if ft4QuietGap <= 0 {
		ft4QuietGap = config.DefaultCallCorrectionFT4QuietGapSeconds
	}
	ft4HardCap := cfg.FT4HardCapSeconds
	if ft4HardCap < ft4QuietGap {
		ft4HardCap = max(config.DefaultCallCorrectionFT4HardCapSeconds, ft4QuietGap)
	}
	ft2QuietGap := cfg.FT2QuietGapSeconds
	if ft2QuietGap <= 0 {
		ft2QuietGap = config.DefaultCallCorrectionFT2QuietGapSeconds
	}
	ft2HardCap := cfg.FT2HardCapSeconds
	if ft2HardCap < ft2QuietGap {
		ft2HardCap = max(config.DefaultCallCorrectionFT2HardCapSeconds, ft2QuietGap)
	}
	return ftConfidencePolicy{
		pMinUniqueSpotters: pMin,
		vMinUniqueSpotters: vMin,
		ft8Timing: ftConfidenceTiming{
			quietGap: time.Duration(ft8QuietGap) * time.Second,
			hardCap:  time.Duration(ft8HardCap) * time.Second,
		},
		ft4Timing: ftConfidenceTiming{
			quietGap: time.Duration(ft4QuietGap) * time.Second,
			hardCap:  time.Duration(ft4HardCap) * time.Second,
		},
		ft2Timing: ftConfidenceTiming{
			quietGap: time.Duration(ft2QuietGap) * time.Second,
			hardCap:  time.Duration(ft2HardCap) * time.Second,
		},
	}
}

// newFTConfidenceController creates the FT burst controller even when few FT
// modes are active; mode-specific gating happens in Observe so the output
// pipeline has one consistent stage boundary.
func newFTConfidenceController(cfg config.CallCorrectionConfig) *ftConfidenceController {
	controller := &ftConfidenceController{
		enabled:          true,
		maxPendingGroups: ftConfidenceMaxPendingGroups,
		maxPendingSpots:  ftConfidenceMaxPendingSpots,
		policy:           newFTConfidencePolicy(cfg),
		pending:          make(map[ftConfidenceKey]*ftConfidencePendingGroup),
		queue:            make(ftConfidenceHeap, 0, 4),
		dueScratch:       make([]ftConfidenceItem, 0, 4),
	}
	return controller
}

func (c *ftConfidenceController) Enabled() bool {
	return c != nil && c.enabled
}

// Observe either appends a spot to a bounded corroboration burst or declines to
// hold it. Declines are fail-open: the caller assigns a glyph immediately so
// memory pressure cannot suppress telnet output.
func (c *ftConfidenceController) Observe(now time.Time, ctx outputSpotContext) (bool, int) {
	if !c.Enabled() || ctx.spot == nil {
		return false, 1
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	key, spotter, ok := buildFTConfidenceKey(ctx.spot)
	if !ok {
		return false, 1
	}
	timing, ok := ftConfidenceTimingForMode(key.mode, c.policy)
	if !ok {
		return false, 1
	}
	group := c.pending[key]
	if group != nil {
		uniqueCount := len(group.spotters)
		if spotter != "" {
			if _, exists := group.spotters[spotter]; !exists {
				uniqueCount++
			}
		}
		if c.maxPendingSpots > 0 && c.pendingSpots >= c.maxPendingSpots {
			return false, uniqueCount
		}
		group.contexts = append(group.contexts, newFTConfidencePendingContext(ctx))
		if spotter != "" {
			if group.spotters == nil {
				group.spotters = make(map[string]struct{}, 2)
			}
			group.spotters[spotter] = struct{}{}
		}
		group.lastSeen = now
		c.advanceGroupDue(group, timing)
		c.pendingSpots++
		return true, uniqueCount
	}

	if c.maxPendingGroups > 0 && len(c.pending) >= c.maxPendingGroups {
		return false, 1
	}
	if c.maxPendingSpots > 0 && c.pendingSpots >= c.maxPendingSpots {
		return false, 1
	}

	due := ftConfidenceBurstDue(now, now.Add(timing.hardCap), timing)
	group = &ftConfidencePendingGroup{
		key:          key,
		firstSeen:    now,
		lastSeen:     now,
		hardDeadline: now.Add(timing.hardCap),
		due:          due,
		seq:          c.seq,
		contexts:     make([]ftConfidencePendingContext, 1, 2),
	}
	group.contexts[0] = newFTConfidencePendingContext(ctx)
	if spotter != "" {
		group.spotters = make(map[string]struct{}, 2)
		group.spotters[spotter] = struct{}{}
	}
	c.pending[key] = group
	c.pushDueItem(group)
	c.seq++
	c.pendingSpots++
	return true, 1
}

// Drain releases only the current heap entries for each group; stale heap
// entries are expected after due-time advances and are discarded here.
func (c *ftConfidenceController) Drain(now time.Time, force bool) []*ftConfidencePendingGroup {
	if !c.Enabled() {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	items := popFTConfidenceDue(&c.queue, now, force, c.dueScratch)
	c.dueScratch = items[:0]
	if len(items) == 0 {
		return nil
	}
	out := make([]*ftConfidencePendingGroup, 0, len(items))
	for _, item := range items {
		group := c.pending[item.key]
		if group == nil {
			continue
		}
		if item.seq != group.seq || !item.due.Equal(group.due) {
			continue
		}
		delete(c.pending, item.key)
		c.pendingSpots -= len(group.contexts)
		if c.pendingSpots < 0 {
			c.pendingSpots = 0
		}
		out = append(out, group)
	}
	return out
}

// NextDue exposes the next real group deadline to the output loop, skipping
// stale heap nodes produced by burst extension.
func (c *ftConfidenceController) NextDue() (time.Time, bool) {
	if !c.Enabled() {
		return time.Time{}, false
	}
	for c.queue.Len() > 0 {
		head := c.queue[0]
		group := c.pending[head.key]
		if group == nil || head.seq != group.seq || !head.due.Equal(group.due) {
			c.queue.pop()
			continue
		}
		return head.due, true
	}
	return time.Time{}, false
}

// buildFTConfidenceKey normalizes call/mode/frequency once so support traces can
// explain why two FT spots did or did not land in the same corroboration burst.
func buildFTConfidenceKey(s *spot.Spot) (ftConfidenceKey, string, bool) {
	if s == nil {
		return ftConfidenceKey{}, "", false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	mode = strutil.NormalizeUpper(mode)
	if _, _, ok := spot.FTConfidenceTimingKeys(mode); !ok {
		return ftConfidenceKey{}, "", false
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	call = spot.NormalizeCallsign(call)
	if call == "" {
		return ftConfidenceKey{}, "", false
	}
	spotter := s.DECallNorm
	if spotter == "" {
		spotter = s.DECall
	}
	spotter = spot.NormalizeCallsign(spotter)
	return ftConfidenceKey{
		call:    call,
		mode:    mode,
		freqKey: ftConfidenceFrequencyKey(s.Frequency),
	}, spotter, true
}

// ftConfidenceTimingForMode keeps timing policy tied to taxonomy-known FT
// families instead of treating every digital mode as burst-correlated.
func ftConfidenceTimingForMode(mode string, policy ftConfidencePolicy) (ftConfidenceTiming, bool) {
	quietKey, hardCapKey, ok := spot.FTConfidenceTimingKeys(mode)
	if !ok {
		return ftConfidenceTiming{}, false
	}
	switch {
	case quietKey == "ft8_quiet_gap_seconds" && hardCapKey == "ft8_hard_cap_seconds":
		return policy.ft8Timing, true
	case quietKey == "ft4_quiet_gap_seconds" && hardCapKey == "ft4_hard_cap_seconds":
		return policy.ft4Timing, true
	case quietKey == "ft2_quiet_gap_seconds" && hardCapKey == "ft2_hard_cap_seconds":
		return policy.ft2Timing, true
	default:
		return ftConfidenceTiming{}, false
	}
}

// ftConfidenceBurstDue chooses the earlier of quiet-gap release and hard cap so
// the UI gets fast confirmation when the burst ends but never waits forever.
func ftConfidenceBurstDue(lastSeen, hardDeadline time.Time, timing ftConfidenceTiming) time.Time {
	due := lastSeen.Add(timing.quietGap)
	if due.After(hardDeadline) {
		return hardDeadline
	}
	return due
}

func (c *ftConfidenceController) pushDueItem(group *ftConfidencePendingGroup) {
	if c == nil || group == nil {
		return
	}
	c.queue.push(ftConfidenceItem{
		key: group.key,
		due: group.due,
		seq: group.seq,
	})
}

func (c *ftConfidenceController) advanceGroupDue(group *ftConfidencePendingGroup, timing ftConfidenceTiming) {
	if c == nil || group == nil {
		return
	}
	newDue := ftConfidenceBurstDue(group.lastSeen, group.hardDeadline, timing)
	if group.due.Equal(newDue) {
		return
	}
	group.due = newDue
	group.seq = c.seq
	c.pushDueItem(group)
	c.seq++
}

func ftConfidenceFrequencyKey(freqKHz float64) int64 {
	if freqKHz <= 0 {
		return 0
	}
	return int64(math.Round(freqKHz * 100))
}

// ftConfidenceGlyphForUniqueSpotters keeps FT glyphs tied to independent
// receiver count rather than resolver confidence so FT support remains
// explainable to telnet users.
func ftConfidenceGlyphForUniqueSpotters(uniqueSpotters int, policy ftConfidencePolicy) string {
	switch {
	case uniqueSpotters >= policy.vMinUniqueSpotters:
		return "V"
	case uniqueSpotters >= policy.pMinUniqueSpotters:
		return "P"
	default:
		return "?"
	}
}

// newFTRecentBandStore uses tighter bounds than the general recent-band store
// because FT bursts can be high-volume and are only needed as support-floor
// context for uncertain FT glyphs.
func newFTRecentBandStore(cfg config.CallCorrectionConfig) *spot.RecentBandStore {
	if !cfg.RecentBandBonusEnabled {
		return nil
	}
	window := time.Duration(cfg.RecentBandWindowSeconds) * time.Second
	return spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             window,
		MaxEntries:         ftConfidenceRecentMaxEntries,
		CleanupInterval:    ftConfidenceRecentCleanupInterval,
		MaxSpottersPerCall: ftConfidenceRecentMaxSpotters,
	})
}

// applyFTConfidenceStage diverts only unresolved FT glyphs onto the burst rail.
// Existing confidence, beacons, and non-FT modes keep their earlier pipeline
// decisions.
func (p *outputPipeline) applyFTConfidenceStage(ctx *outputSpotContext, now time.Time) bool {
	if p == nil || ctx == nil || ctx.spot == nil {
		return false
	}
	s := ctx.spot
	if s.IsBeacon || !modeSupportsFTConfidenceGlyph(ctx.modeUpper) || strings.TrimSpace(s.Confidence) != "" {
		return true
	}
	if p.ftConfidence == nil || !p.ftConfidence.Enabled() {
		p.assignFTConfidence(ctx, 1)
		return true
	}
	held, uniqueCount := p.ftConfidence.Observe(now, *ctx)
	if held {
		return false
	}
	p.assignFTConfidence(ctx, uniqueCount)
	return true
}

// assignFTConfidence writes the telnet glyph from unique spotter count and then
// applies support floors only to uncertain bursts, preserving stronger evidence.
func (p *outputPipeline) assignFTConfidence(ctx *outputSpotContext, uniqueSpotters int) {
	if p == nil || ctx == nil || ctx.spot == nil {
		return
	}
	s := ctx.spot
	policy := newFTConfidencePolicy(p.correctionCfg)
	if p.ftConfidence != nil {
		policy = p.ftConfidence.policy
	}
	s.Confidence = ftConfidenceGlyphForUniqueSpotters(uniqueSpotters, policy)
	if s.Confidence == "?" {
		applySupportFloor(s, p.recentBandStore, p.customSCPStore, p.ftRecentBandStore, p.correctionCfg)
	}
	ctx.dirty = true
}

// releaseDueFT sends every spot in a completed FT burst through the remaining
// output stages with the same unique-spotter glyph.
func (p *outputPipeline) releaseDueFT(now time.Time, force bool) {
	if p == nil || p.ftConfidence == nil || !p.ftConfidence.Enabled() {
		return
	}
	releases := p.ftConfidence.Drain(now, force)
	for _, group := range releases {
		if group == nil {
			continue
		}
		uniqueSpotters := len(group.spotters)
		for i := range group.contexts {
			ctx := group.contexts[i].outputContext()
			if ctx.spot == nil {
				continue
			}
			p.assignFTConfidence(&ctx, uniqueSpotters)
			if !p.finalizeSpotForMetrics(&ctx) {
				continue
			}
			if !p.prepareFanoutSpot(&ctx) {
				continue
			}
			p.deliverSpot(&ctx)
		}
	}
}

// stopFTTimer mirrors stopTemporalTimer for the FT rail: clear both the timer
// and select channel to avoid stale wakeups after all groups drain.
func (p *outputPipeline) stopFTTimer() {
	if p == nil || p.ftTimer == nil {
		return
	}
	if !p.ftTimer.Stop() {
		select {
		case <-p.ftTimer.C:
		default:
		}
	}
	p.ftTimer = nil
	p.ftTimerCh = nil
	p.ftTimerDue = time.Time{}
}

// scheduleFTTimer lets the single output goroutine wake for FT hard-cap or
// quiet-gap release while ingest is idle.
func (p *outputPipeline) scheduleFTTimer(now time.Time) {
	if p == nil || p.ftConfidence == nil || !p.ftConfidence.Enabled() {
		p.stopFTTimer()
		return
	}
	nextDue, ok := p.ftConfidence.NextDue()
	if !ok {
		p.stopFTTimer()
		return
	}
	delay := nextDue.Sub(now)
	if delay < 0 {
		delay = 0
	}
	if p.ftTimer == nil {
		p.ftTimer = time.NewTimer(delay)
		p.ftTimerDue = nextDue
	} else {
		if p.ftTimerDue.Equal(nextDue) {
			p.ftTimerCh = p.ftTimer.C
			return
		}
		if !p.ftTimer.Stop() {
			select {
			case <-p.ftTimer.C:
			default:
			}
		}
		p.ftTimer.Reset(delay)
		p.ftTimerDue = nextDue
	}
	p.ftTimerCh = p.ftTimer.C
}

// recordFTRecentBandObservation feeds the FT-only support floor after a spot
// has earned a display glyph, keeping future uncertain FT bursts from starting
// cold on the same band/call.
func (p *outputPipeline) recordFTRecentBandObservation(s *spot.Spot) {
	if p == nil || p.ftRecentBandStore == nil || s == nil || s.IsBeacon {
		return
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	if !modeSupportsFTConfidenceGlyph(mode) {
		return
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	band := s.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	spotter := s.DECallNorm
	if spotter == "" {
		spotter = s.DECall
	}
	seenAt := s.Time.UTC()
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}
	key, baseKey, ok := spot.CorrectionFamilyKeyPair(call)
	if !ok {
		key = call
	}
	if key != "" {
		p.ftRecentBandStore.Record(key, band, mode, spotter, seenAt)
	}
	if baseKey != "" {
		p.ftRecentBandStore.Record(baseKey, band, mode, spotter, seenAt)
	}
}
