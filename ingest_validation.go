package main

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"dxcluster/cty"
	"dxcluster/internal/ratelimit"
	"dxcluster/spot"
	"dxcluster/uls"
)

const (
	defaultIngestCTYLogInterval  = 30 * time.Second
	defaultIngestDropLogInterval = 30 * time.Second
)

// ingestValidator centralizes CTY/ULS validation before deduplication.
// It is intentionally single-consumer to keep CTY cache usage bounded and predictable.
type ingestValidator struct {
	input              chan *spot.Spot
	dedupInput         chan<- *spot.Spot
	ctyLookup          func() *cty.CTYDatabase
	metaCache          *callMetaCache
	ctyUpdater         func(call string, info *cty.PrefixInfo)
	gridUpdate         func(call, grid string)
	unlicensedReporter func(source, role, call, mode string, freq float64)
	dropReporter       func(line string)
	isLicensedUS       func(call string) bool
	requireCTY         bool
	ctyDropDXCounter   ratelimit.Counter
	ctyDropDECounter   ratelimit.Counter
	invalidDropDX      ratelimit.Counter
	invalidDropDE      ratelimit.Counter
	dedupDropCounter   ratelimit.Counter
	ingestTotal        atomic.Uint64 // total spots received (includes those dropped by validation)
}

// newIngestValidator wires a bounded ingest gate for CTY/ULS checks.
func newIngestValidator(
	ctyLookup func() *cty.CTYDatabase,
	metaCache *callMetaCache,
	ctyUpdater func(call string, info *cty.PrefixInfo),
	gridUpdate func(call, grid string),
	dedupInput chan<- *spot.Spot,
	unlicensedReporter func(source, role, call, mode string, freq float64),
	dropReporter func(line string),
	requireCTY bool,
) *ingestValidator {
	inputBuffer := cap(dedupInput)
	if inputBuffer <= 0 {
		inputBuffer = 10000
	}
	return &ingestValidator{
		input:              make(chan *spot.Spot, inputBuffer),
		dedupInput:         dedupInput,
		ctyLookup:          ctyLookup,
		metaCache:          metaCache,
		ctyUpdater:         ctyUpdater,
		gridUpdate:         gridUpdate,
		unlicensedReporter: unlicensedReporter,
		dropReporter:       dropReporter,
		isLicensedUS:       uls.IsLicensedUS,
		requireCTY:         requireCTY,
		ctyDropDXCounter:   ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		ctyDropDECounter:   ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		invalidDropDX:      ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		invalidDropDE:      ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		dedupDropCounter:   ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
	}
}

// Input returns the channel ingest sources should send spots into.
func (v *ingestValidator) Input() chan<- *spot.Spot {
	if v == nil {
		return nil
	}
	return v.input
}

// Start launches the validator loop.
func (v *ingestValidator) Start() {
	if v == nil {
		return
	}
	go v.run()
}

func (v *ingestValidator) run() {
	for s := range v.input {
		if s == nil {
			continue
		}
		v.ingestTotal.Add(1)
		if !v.validateSpot(s) {
			continue
		}
		select {
		case v.dedupInput <- s:
		default:
			if count, ok := v.dedupDropCounter.Inc(); ok {
				log.Printf("Ingest: dedup input full, dropping spot (source=%s total=%d)", ingestSourceLabel(s), count)
			}
		}
	}
}

// IngestCount returns the total number of spots observed at ingest (pre-validation).
func (v *ingestValidator) IngestCount() uint64 {
	if v == nil {
		return 0
	}
	return v.ingestTotal.Load()
}

// validateSpot enforces CTY validity and DE licensing before dedup.
// It refreshes metadata from CTY while preserving any grid fields already attached.
func (v *ingestValidator) validateSpot(s *spot.Spot) bool {
	if s == nil {
		return false
	}
	s.EnsureNormalized()
	if v.ctyLookup == nil {
		return true
	}
	ctyDB := v.ctyLookup()
	if ctyDB == nil {
		if !v.requireCTY {
			return true
		}
		ctyDB = v.waitForCTY()
		if ctyDB == nil {
			return false
		}
	}

	dxCall := s.DXCallNorm
	if dxCall == "" {
		dxCall = s.DXCall
	}
	deCall := s.DECallNorm
	if deCall == "" {
		deCall = s.DECall
	}

	if shouldRejectCTYCall(dxCall) {
		v.logInvalidDrop("DX", dxCall, s)
		return false
	}
	if shouldRejectCTYCall(deCall) {
		v.logInvalidDrop("DE", deCall, s)
		return false
	}

	dxLookupCall := normalizeCallForMetadata(dxCall)
	deLookupCall := normalizeCallForMetadata(deCall)
	if dxLookupCall == "" {
		dxLookupCall = dxCall
	}
	if deLookupCall == "" {
		deLookupCall = deCall
	}
	dxInfo, ok := v.lookupCTY(ctyDB, dxLookupCall)
	if !ok {
		if uls.AllowlistMatchAny(strings.TrimSpace(uls.NormalizeForLicense(dxCall))) {
			goto deLookup
		}
		v.logCTYDrop("DX", dxCall, s)
		return false
	}
deLookup:
	deInfo, ok := v.lookupCTY(ctyDB, deLookupCall)
	if !ok {
		if uls.AllowlistMatchAny(strings.TrimSpace(uls.NormalizeForLicense(deCall))) {
			goto afterLookup
		}
		v.logCTYDrop("DE", deCall, s)
		return false
	}
afterLookup:

	dxGrid := strings.TrimSpace(s.DXMetadata.Grid)
	deGrid := strings.TrimSpace(s.DEMetadata.Grid)
	dxGridDerived := s.DXMetadata.GridDerived
	deGridDerived := s.DEMetadata.GridDerived
	s.DXMetadata = metadataFromPrefix(dxInfo)
	s.DEMetadata = metadataFromPrefix(deInfo)
	if dxGrid != "" {
		s.DXMetadata.Grid = dxGrid
		s.DXMetadata.GridDerived = dxGridDerived
	}
	if deGrid != "" {
		s.DEMetadata.Grid = deGrid
		s.DEMetadata.GridDerived = deGridDerived
	}
	// Metadata refresh can change continent/grid; clear cached norms and rebuild.
	s.InvalidateMetadataCache()
	s.EnsureNormalized()

	// Seed the grid cache early when DE grid is present (e.g., PSKReporter).
	if v.gridUpdate != nil && deGrid != "" {
		v.gridUpdate(deLookupCall, deGrid)
	}

	if s.IsTestSpotter {
		return true
	}
	if v.isLicensedUS != nil {
		deLicenseCall := strings.TrimSpace(uls.NormalizeForLicense(deCall))
		if deLicenseCall != "" {
			// License checks key off the normalized base call; jurisdiction derives from that base.
			if info, ok := v.lookupCTY(ctyDB, deLicenseCall); ok && info.ADIF == 291 {
				callKey := deLicenseCall
				if callKey == "" {
					callKey = deCall
				}
				if uls.AllowlistMatch(info.ADIF, callKey) {
					return true
				}
				if !v.isLicensedUS(callKey) {
					if v.unlicensedReporter != nil {
						v.unlicensedReporter(ingestSourceLabel(s), "DE", callKey, s.ModeNorm, s.Frequency)
					}
					return false
				}
			}
		}
	}

	return true
}

func (v *ingestValidator) waitForCTY() *cty.CTYDatabase {
	if v == nil || v.ctyLookup == nil {
		return nil
	}
	if db := v.ctyLookup(); db != nil {
		return db
	}
	log.Printf("CTY database not loaded; ingest paused until ready")
	timer := time.NewTimer(defaultIngestCTYLogInterval)
	defer timer.Stop()
	for {
		<-timer.C
		if db := v.ctyLookup(); db != nil {
			return db
		}
		log.Printf("CTY database still unavailable; ingest paused")
		timer.Reset(defaultIngestCTYLogInterval)
	}
}

func (v *ingestValidator) lookupCTY(db *cty.CTYDatabase, call string) (*cty.PrefixInfo, bool) {
	if db == nil || call == "" {
		return nil, false
	}
	if shouldRejectCTYCall(call) {
		return nil, false
	}
	if v.metaCache != nil {
		info, ok, cached := v.metaCache.LookupCTY(call, db)
		if ok && !cached && v.ctyUpdater != nil {
			v.ctyUpdater(call, info)
		}
		return info, ok
	}
	return db.LookupCallsignPortable(call)
}

func (v *ingestValidator) logCTYDrop(role, call string, s *spot.Spot) {
	counter := &v.ctyDropDXCounter
	if role == "DE" {
		counter = &v.ctyDropDECounter
	}
	if count, ok := counter.Inc(); ok {
		line := fmt.Sprintf("CTY drop: unknown %s %s at %.1f kHz (source=%s total=%d)", role, call, s.Frequency, ingestSourceLabel(s), count)
		if v.dropReporter != nil {
			v.dropReporter(line)
			return
		}
		log.Print(line)
	}
}

func (v *ingestValidator) logInvalidDrop(role, call string, s *spot.Spot) {
	counter := &v.invalidDropDX
	if role == "DE" {
		counter = &v.invalidDropDE
	}
	if count, ok := counter.Inc(); ok {
		line := fmt.Sprintf("CTY drop: invalid %s %s (>=3 leading letters) at %.1f kHz (source=%s total=%d)", role, call, s.Frequency, ingestSourceLabel(s), count)
		if v.dropReporter != nil {
			v.dropReporter(line)
			return
		}
		log.Print(line)
	}
}

func ingestSourceLabel(s *spot.Spot) string {
	if s == nil {
		return "unknown"
	}
	label := strings.TrimSpace(s.SourceNode)
	if label != "" {
		return label
	}
	if s.SourceType != "" {
		return string(s.SourceType)
	}
	return "unknown"
}
