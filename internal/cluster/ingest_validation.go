// File role: Validates incoming spot identity, source, license, and CTY
// admission before spots can enter runtime cluster state.
package cluster

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
	input                chan *spot.Spot
	dedupInput           chan<- *spot.Spot
	ctyLookup            func() *cty.CTYDatabase
	metaCache            *callMetaCache
	ctyUpdater           func(call string, info *cty.PrefixInfo)
	gridUpdate           func(call, grid string)
	unlicensedReporter   func(source, role, call, deCall, dxCall, mode string, freq float64)
	dropReporter         func(line string)
	badCallReporter      func(source, role, reason, call, deCall, dxCall, mode, detail string)
	lookupUS             func(call string) uls.LookupResult
	licenseChecksEnabled func() bool
	requireCTY           bool
	ctyDropDXCounter     ratelimit.Counter
	ctyDropDECounter     ratelimit.Counter
	invalidDropDX        ratelimit.Counter
	invalidDropDE        ratelimit.Counter
	dedupDropCounter     ratelimit.Counter
	ingestTotal          atomic.Uint64 // total spots received (includes those dropped by validation)
}

// newIngestValidator wires a bounded ingest gate for CTY/ULS checks.
func newIngestValidator(
	ctyLookup func() *cty.CTYDatabase,
	metaCache *callMetaCache,
	ctyUpdater func(call string, info *cty.PrefixInfo),
	gridUpdate func(call, grid string),
	dedupInput chan<- *spot.Spot,
	unlicensedReporter func(source, role, call, deCall, dxCall, mode string, freq float64),
	dropReporter func(line string),
	requireCTY bool,
) *ingestValidator {
	inputBuffer := cap(dedupInput)
	if inputBuffer <= 0 {
		inputBuffer = 10000
	}
	return &ingestValidator{
		input:                make(chan *spot.Spot, inputBuffer),
		dedupInput:           dedupInput,
		ctyLookup:            ctyLookup,
		metaCache:            metaCache,
		ctyUpdater:           ctyUpdater,
		gridUpdate:           gridUpdate,
		unlicensedReporter:   unlicensedReporter,
		dropReporter:         dropReporter,
		lookupUS:             uls.LookupUS,
		licenseChecksEnabled: uls.LicenseChecksEnabled,
		requireCTY:           requireCTY,
		ctyDropDXCounter:     ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		ctyDropDECounter:     ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		invalidDropDX:        ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		invalidDropDE:        ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
		dedupDropCounter:     ratelimit.NewCounterWithRetry(defaultIngestDropLogInterval),
	}
}

func (v *ingestValidator) SetBadCallReporter(reporter func(source, role, reason, call, deCall, dxCall, mode, detail string)) {
	if v == nil {
		return
	}
	v.badCallReporter = reporter
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

// run is the ingress pressure boundary. Validation drops are intentional data
// quality decisions, while dedup-channel drops mean downstream backpressure and
// are logged separately for operator troubleshooting.
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
	// Incoming metadata cannot establish an FCC address association.
	s.DEMetadata.State, s.DXMetadata.State = "", ""
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

	return v.checkSpotterLicense(s, ctyDB, deCall, dxCall)
}

// checkSpotterLicense attaches address metadata independently of enforcement.
// Base-call jurisdiction selects the lookup; an admission exception never
// converts an absent license into a found record or a fabricated state.
func (v *ingestValidator) checkSpotterLicense(s *spot.Spot, ctyDB *cty.CTYDatabase, deCall, dxCall string) bool {
	call := uls.NormalizeForLicense(deCall)
	if call == "" {
		return true
	}
	info, ok := v.lookupCTY(ctyDB, call)
	if !ok {
		return true
	}
	var result uls.LookupResult
	var enabled bool
	switch {
	case spot.IsFCCJurisdiction(info.ADIF):
		if v.lookupUS == nil {
			return true
		}
		result = v.lookupUS(call)
		enabled = v.licenseChecksEnabled != nil && v.licenseChecksEnabled()
	case spot.IsCanadianJurisdiction(info.ADIF):
		result = uls.LookupCanadian(call)
		enabled = uls.CanadianLicenseChecksEnabled()
	default:
		return true
	}
	if result.Available && result.Found {
		s.DEMetadata.State = result.State
	}
	if s.IsTestSpotter || uls.AllowlistMatch(info.ADIF, call) ||
		!enabled ||
		!result.Available || result.Found {
		return true
	}
	if v.unlicensedReporter != nil {
		v.unlicensedReporter(ingestSourceLabel(s), "DE", call, deCall, dxCall, s.ModeNorm, s.Frequency)
	}
	return false
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

// lookupCTY keeps all CTY calls behind one helper so support diagnostics can
// distinguish an unknown prefix from a nil database or malformed calls.
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

// logCTYDrop records rejected DX/DE calls with rate limiting because CTY misses
// can arrive in bursts after upstream parser or data-feed problems.
func (v *ingestValidator) logCTYDrop(role, call string, s *spot.Spot) {
	counter := &v.ctyDropDXCounter
	if role == "DE" {
		counter = &v.ctyDropDECounter
	}
	if count, ok := counter.Inc(); ok {
		line := fmt.Sprintf("CTY drop: unknown %s %s at %.1f kHz (source=%s total=%d)", role, call, s.Frequency, ingestSourceLabel(s), count)
		v.reportBadCall(role, "cty_unknown", call, s, "cty_validation")
		if v.dropReporter != nil {
			v.dropReporter(line)
			return
		}
		log.Print(line)
	}
}

// logInvalidDrop records syntactically invalid calls separately from CTY misses
// so a support agent can route malformed input differently from missing country
// metadata.
func (v *ingestValidator) logInvalidDrop(role, call string, s *spot.Spot) {
	counter := &v.invalidDropDX
	if role == "DE" {
		counter = &v.invalidDropDE
	}
	if count, ok := counter.Inc(); ok {
		line := fmt.Sprintf("CTY drop: invalid %s %s (malformed callsign) at %.1f kHz (source=%s total=%d)", role, call, s.Frequency, ingestSourceLabel(s), count)
		v.reportBadCall(role, "invalid_callsign", call, s, "cty_prefilter")
		if v.dropReporter != nil {
			v.dropReporter(line)
			return
		}
		log.Print(line)
	}
}

// reportBadCall feeds the separate bad-call log used by support tooling. It is
// intentionally best-effort so diagnostics never become another ingest failure
// path.
func (v *ingestValidator) reportBadCall(role, reason, call string, s *spot.Spot, detail string) {
	if v == nil || v.badCallReporter == nil || s == nil {
		return
	}
	v.badCallReporter(ingestSourceLabel(s), role, reason, call, s.DECall, s.DXCall, s.ModeNorm, detail)
}

// ingestSourceLabel prefers the upstream node name when present because source
// routing is usually the first question in support triage.
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
