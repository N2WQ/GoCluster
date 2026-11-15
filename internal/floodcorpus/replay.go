package floodcorpus

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/floodcontrol"
	"dxcluster/spot"
)

// Options are declared hypothetical experiment parameters, never server defaults.
type Options struct {
	Threshold     int  `json:"threshold"`
	WindowSeconds int  `json:"window_seconds"`
	ActorCap      int  `json:"actor_cap_per_source"`
	AllowPartial  bool `json:"allow_partial_sensitivity"`
}

// Report contains allowlisted aggregates only. No source payload, identifiers,
// endpoint, captured reason or independently supplied label is printed.
type Report struct {
	EventsSHA256           string            `json:"events_sha256"`
	Version                int               `json:"version"`
	Implementation         Build             `json:"implementation"`
	Parameters             Options           `json:"hypothetical_parameters"`
	Partial                bool              `json:"partial_sensitivity_only"`
	Arrivals               int               `json:"raw_corpus_arrivals"`
	RollingAllowed         int               `json:"rolling_allowed"`
	ReplenishingAllowed    int               `json:"replenishing_allowed"`
	RollingActors          int               `json:"rolling_actors"`
	ReplenishingActors     int               `json:"replenishing_actors"`
	RollingOverflow        uint64            `json:"rolling_overflow"`
	ReplenishingOverflow   uint64            `json:"replenishing_overflow"`
	LabeledArrivals        int               `json:"independently_labeled_arrivals"`
	KnownCopyArrivals      int               `json:"known_copy_arrivals"`
	LogicalPosts           *int              `json:"known_logical_posts"`
	RecoveredLogicalPosts  *int              `json:"oracle_recovered_logical_posts"`
	IndistinguishablePosts *int              `json:"distinct_truth_posts_with_equal_observable_fields"`
	Charging               string            `json:"charging_difference"`
	Limits                 string            `json:"claim_limits"`
	Scenarios              []ScenarioSummary `json:"generated_scenarios,omitempty"`
}

// ScenarioSummary exposes recovery/burst timing for declared generated phases,
// not automatically classified real traffic. At most eight fixed names exist.
type ScenarioSummary struct {
	Name                                string `json:"name"`
	Arrivals                            int    `json:"arrivals"`
	RollingAllowed                      int    `json:"rolling_allowed"`
	ReplenishingAllowed                 int    `json:"replenishing_allowed"`
	RollingFirst                        *int64 `json:"rolling_first_allowed_ns"`
	ReplenishingFirst                   *int64 `json:"replenishing_first_allowed_ns"`
	RollingResumed                      *int64 `json:"rolling_first_allowed_after_refusal_ns"`
	ReplenishingResumed                 *int64 `json:"replenishing_first_allowed_after_refusal_ns"`
	rollingRefused, replenishingRefused bool
}

var scenarioNames = [...]string{"burst", "sustained", "cutoff", "idle_recovery", "slower_recovery", "known_copies", "genuine_repeats", "native"}

func scenarioIndex(name string) int {
	for i, s := range scenarioNames {
		if s == name {
			return i
		}
	}
	return -1
}
func (s *ScenarioSummary) observe(ns int64, rollingAllowed, replenishingAllowed bool) {
	s.Arrivals++
	if rollingAllowed {
		s.RollingAllowed++
		if s.RollingFirst == nil {
			s.RollingFirst = pointerNS(ns)
		}
		if s.rollingRefused && s.RollingResumed == nil {
			s.RollingResumed = pointerNS(ns)
		}
	} else {
		s.rollingRefused = true
	}
	if replenishingAllowed {
		s.ReplenishingAllowed++
		if s.ReplenishingFirst == nil {
			s.ReplenishingFirst = pointerNS(ns)
		}
		if s.replenishingRefused && s.ReplenishingResumed == nil {
			s.ReplenishingResumed = pointerNS(ns)
		}
	} else {
		s.replenishingRefused = true
	}
}
func pointerNS(ns int64) *int64 { return &ns }

func (o Options) validate() error {
	if o.Threshold < 1 || o.Threshold > 100000 || o.WindowSeconds < 1 || o.WindowSeconds > 86400 || o.ActorCap < 1 || o.ActorCap > MaxEvents {
		return errors.New("experiment parameters outside bounded ranges")
	}
	w := int64(o.WindowSeconds) * int64(time.Second)
	if int64(o.Threshold) > math.MaxInt64/w {
		return errors.New("experiment credit arithmetic overflow")
	}
	return nil
}

func rolling(o Options) *floodcontrol.Controller {
	rail := config.FloodRailConfig{Enabled: true, Action: config.FloodActionSuppress, WindowSeconds: o.WindowSeconds, MaxEntriesPerPartition: o.ActorCap, ThresholdsPerSourceType: map[string]int{"MANUAL": o.Threshold, "PEER": o.Threshold, "UPSTREAM": o.Threshold, "RBN": o.Threshold, "FT4": o.Threshold, "FT8": o.Threshold, "PSKREPORTER": o.Threshold}}
	c := floodcontrol.New(config.FloodControlConfig{Enabled: true, LogIntervalSeconds: 86400, PartitionMode: config.FloodControlPartitionExactSourceType, Rails: config.FloodControlRailsConfig{DECall: rail}}, make(chan *spot.Spot, 1), nil, func(string) {})
	// A synchronous always-successful sink removes queue pressure from this
	// quota-only comparison; the controller does not mutate or retain the Spot.
	c.SetAdmissionSink(func(*spot.Spot) bool { return true })
	return c
}

type bucket struct{ at, credit int64 }
type replenishing struct {
	options        Options
	capacity, cost int64
	partitions     map[string]map[string]*bucket
	overflow       uint64
	actors         int
}

func newReplenishing(o Options) *replenishing {
	w := int64(o.WindowSeconds) * int64(time.Second)
	return &replenishing{options: o, capacity: int64(o.Threshold) * w, cost: w, partitions: make(map[string]map[string]*bucket)}
}

func reporter(s *spot.Spot) (string, string, bool) {
	source := strings.ToUpper(strings.TrimSpace(string(s.SourceType)))
	if spot.IsSkimmerSource(spot.SourceType(source)) {
		return source, "", false
	}
	key := s.DECallNorm
	if key == "" {
		key = s.DECall
	}
	key = strings.ToUpper(strings.TrimSpace(key))
	return source, key, key != ""
}

func (r *replenishing) admit(ns int64, s *spot.Spot) bool {
	source, key, charge := reporter(s)
	if !charge {
		return true
	}
	actors := r.partitions[source]
	if actors == nil {
		actors = make(map[string]*bucket)
		r.partitions[source] = actors
	}
	b := actors[key]
	if b == nil {
		if len(actors) >= r.options.ActorCap {
			r.overflow++
			return true
		}
		b = &bucket{at: ns, credit: r.capacity}
		actors[key] = b
		r.actors++
	}
	if ns > b.at {
		missing := r.capacity - b.credit
		rate := int64(r.options.Threshold)
		// ceil(missing/rate) avoids overflow in elapsed*rate: multiply only
		// when the product is strictly smaller than bounded missing credit.
		untilFull := missing / rate
		if missing%rate != 0 {
			untilFull++
		}
		elapsed := ns - b.at
		if elapsed >= untilFull {
			b.credit = r.capacity
		} else {
			b.credit += elapsed * rate
		}
		b.at = ns
	}
	if b.credit < r.cost {
		return false
	}
	b.credit -= r.cost
	return true
}

type logicalOutcome struct{ baseline, oracle bool }

// Compare replays one bounded corpus into independent actual rolling and
// experimental replenishing state. Neither runs periodic cleanup: capacity
// comparisons concern this finite workload, not live cardinality under a sweeper.
func Compare(directory string, o Options) (Report, error) {
	r := Report{Version: Schema, Implementation: buildIdentity(), Parameters: o, Charging: "rolling charges every eligible arrival including refused attempts; replenishing debits successful allowances only; initially full T, nominal rate T/W, burst T", Limits: "hypothetically eligible DECALL-only suppress experiment; no periodic actor sweep, CTY/license/age/handshake simulation, production winner, calibrated limits or inferred duplicate truth"}
	if err := o.validate(); err != nil {
		return r, err
	}
	m, err := readManifest(directory)
	if err != nil {
		return r, err
	}
	r.EventsSHA256 = m.EventsSHA256
	if !m.Complete && !o.AllowPartial {
		return r, errors.New("partial corpus requires explicit sensitivity option")
	}
	r.Partial = !m.Complete
	base, oracle := rolling(o), rolling(o)
	candidate := newReplenishing(o)
	decoder, err := newNativeDecoder()
	if err != nil {
		return r, err
	}
	defer decoder.manager.Stop()
	logical := make(map[string]logicalOutcome)
	observable := make(map[[32]byte]string)
	allTruth := true
	ambiguities := 0
	var scenarios [len(scenarioNames)]ScenarioSummary
	err = readEvents(directory, m, func(e Event) error {
		s, err := eventSpot(e, decoder)
		if err != nil {
			return err
		}
		at := time.Unix(0, 0).Add(time.Duration(e.NS))
		allowed := base.Admit(at, s)
		if allowed {
			r.RollingAllowed++
		}
		candidateAllowed := candidate.admit(e.NS, s)
		if candidateAllowed {
			r.ReplenishingAllowed++
		}
		r.Arrivals++
		if i := scenarioIndex(e.Scenario); i >= 0 {
			scenarios[i].Name = e.Scenario
			scenarios[i].observe(e.NS, allowed, candidateAllowed)
		}
		if e.Truth == nil {
			allTruth = false
			return nil
		}
		r.LabeledArrivals++
		if e.Truth.Kind == "known_copy" {
			r.KnownCopyArrivals++
		}
		logicalKey := e.Source + ":" + e.Truth.LogicalID
		outcome, seen := logical[logicalKey]
		outcome.baseline = outcome.baseline || allowed
		if !seen {
			outcome.oracle = oracle.Admit(at, s)
			// Ignore arrival time, transport and truth when demonstrating that
			// independently distinct posts can have equal observable source data.
			key := e.Source + ":" + e.SentenceBase64
			if e.Spot != nil {
				raw, marshalErr := json.Marshal(e.Spot)
				if marshalErr != nil {
					return errors.New("invalid synthetic observable fields")
				}
				key = e.Source + ":" + string(raw)
			}
			fingerprint := sha256.Sum256([]byte(key))
			if prior, ok := observable[fingerprint]; ok && prior != logicalKey {
				ambiguities++
			} else {
				observable[fingerprint] = logicalKey
			}
		}
		logical[logicalKey] = outcome
		return nil
	})
	if err != nil {
		return r, err
	}
	_, _, _, _, r.RollingOverflow, r.RollingActors = base.GetStats()
	r.ReplenishingActors = candidate.actors
	r.ReplenishingOverflow = candidate.overflow
	for _, scenario := range scenarios {
		if scenario.Arrivals > 0 {
			r.Scenarios = append(r.Scenarios, scenario)
		}
	}
	if allTruth && r.Arrivals > 0 {
		n, recovered := len(logical), 0
		for _, outcome := range logical {
			if outcome.oracle && !outcome.baseline {
				recovered++
			}
		}
		r.LogicalPosts = &n
		r.RecoveredLogicalPosts = &recovered
		r.IndistinguishablePosts = &ambiguities
	}
	return r, nil
}

func readManifest(directory string) (Manifest, error) {
	var m Manifest
	if !localPath(directory) {
		return m, errors.New("local corpus required")
	}
	f, err := os.Open(filepath.Join(directory, "manifest.json")) // #nosec G703 -- Operator-selected local corpus; fixed filename is not captured input.
	if err != nil {
		return m, errors.New("cannot open manifest")
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 16385))
	if err != nil || len(raw) > 16384 || strictJSON(raw, &m) != nil || !validManifest(m) {
		return m, errors.New("invalid corpus manifest")
	}
	return m, nil
}

func validManifest(m Manifest) bool {
	if m.Version != Schema || m.Stage != corpusStage || m.Events < 0 || m.Events > MaxEvents || m.Rejected < 0 || m.Rejected > int(MaxInputBytes) || m.Other < 0 || m.Other > int(MaxInputBytes) || !identifier(m.Transport, 64) || !validHash(m.EventsSHA256) || m.Timing == "" || len(m.Timing) > 1024 || m.Eligibility == "" || len(m.Eligibility) > 1024 || m.Implementation.Revision == "" || m.Implementation.GoVersion == "" || m.LastNS < 0 {
		return false
	}
	if m.Synthetic {
		return m.CaptureSHA256 == "" && m.RecorderSHA256 == "" && m.CaptureStartUTC == "" && m.Direction == "" && m.Complete && m.CaptureComplete && !m.PendingTail && m.Received == 0 && m.Recorded == 0 && m.Sent == 0 && m.Unsent == 0
	}
	return validHash(m.CaptureSHA256) && validUTC(m.CaptureStartUTC) && m.RecorderSHA256 == RecorderSHA256 && directionIndex(m.Direction) >= 0 && m.Sent >= 0 && m.Sent <= m.Recorded && m.Recorded <= m.Received && m.Received <= MaxInputBytes+16384 && m.Received-m.Recorded <= 16384 && m.Recorded-m.Sent <= 16384 && m.Unsent == m.Recorded-m.Sent && m.Complete == (m.CaptureComplete && !m.PendingTail) && (!m.CaptureComplete || m.Received == m.Sent)
}

func validHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func readEvents(directory string, m Manifest, visit func(Event) error) error {
	f, err := os.Open(filepath.Join(directory, "events.jsonl")) // #nosec G703 -- Operator-selected local corpus; fixed filename is not captured input.
	if err != nil {
		return errors.New("cannot open events")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > MaxInputBytes {
		return errors.New("event input limit")
	}
	h := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(io.LimitReader(f, MaxInputBytes+1), h), MaxRecordBytes+1)
	count, total, previous := 0, int64(0), int64(-1)
	for {
		raw, readErr := reader.ReadSlice('\n')
		if len(raw) == 0 && readErr == io.EOF {
			break
		}
		total += int64(len(raw))
		count++
		if readErr != nil || len(raw) > MaxRecordBytes || total > MaxInputBytes || count > MaxEvents {
			return errors.New("event record or count limit")
		}
		var e Event
		if strictJSON(raw, &e) != nil || !integerFields(raw, "version", "arrival", "receipt_ns") || e.Version != Schema || e.Arrival != count || e.NS < 0 || e.NS < previous || !identifier(e.Transport, 64) {
			return errors.New("invalid event sequence, time or fields")
		}
		previous = e.NS
		if e.Scenario != "" && (!m.Synthetic || scenarioIndex(e.Scenario) < 0) {
			return errors.New("invalid generated scenario")
		}
		if !m.Synthetic && (e.Source != "PEER" || e.Spot != nil || e.Transport != m.Transport || e.NS > m.LastNS) {
			return errors.New("event contradicts captured peer provenance")
		}
		if e.Truth != nil && ((e.Truth.Kind != "known_copy" && e.Truth.Kind != "known_post") || !identifier(e.Truth.LogicalID, 64) || !identifier(e.Truth.Provenance, 128)) {
			return errors.New("invalid independent truth label")
		}
		if err := visit(e); err != nil {
			return err
		}
	}
	if count != m.Events || hex.EncodeToString(h.Sum(nil)) != m.EventsSHA256 {
		return errors.New("event hash or count mismatch")
	}
	return nil
}

func eventSpot(e Event, d *nativeDecoder) (*spot.Spot, error) {
	source := spot.SourceType(e.Source)
	switch source {
	case spot.SourcePeer:
		if e.Spot != nil {
			return nil, errors.New("peer event requires native sentence")
		}
		data, err := base64.StdEncoding.Strict().DecodeString(e.SentenceBase64)
		if err != nil || len(data) > peerFrameLimit {
			return nil, errors.New("invalid decoded sentence")
		}
		s, other := d.decode(string(data))
		if s == nil || other {
			return nil, errors.New("invalid native spot event")
		}
		return s, nil
	case spot.SourceManual, spot.SourceUpstream, spot.SourceRBN, spot.SourceFT4, spot.SourceFT8, spot.SourcePSKReporter:
	default:
		return nil, errors.New("invalid trusted source")
	}
	p := e.Spot
	if p == nil || e.SentenceBase64 != "" || !spot.IsValidNormalizedCallsign(p.DX) || !spot.IsValidNormalizedCallsign(p.DE) || len(p.Node) > 128 || len(p.IP) > 128 || len(p.Mode) > 16 || math.IsNaN(p.Frequency) || math.IsInf(p.Frequency, 0) || p.Frequency < 0 || p.Frequency >= 1<<32 {
		return nil, errors.New("invalid synthetic spot")
	}
	at, err := time.Parse(time.RFC3339Nano, p.Time)
	if err != nil {
		return nil, errors.New("invalid original time")
	}
	s := spot.NewSpotNormalized(p.DX, p.DE, p.Frequency, p.Mode)
	s.Time = at
	s.SourceType = source
	s.SourceNode = p.Node
	s.SpotterIP = p.IP
	return s, nil
}

const peerFrameLimit = 64 << 10
