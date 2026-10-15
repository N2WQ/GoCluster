package telnet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"testing"
	"time"

	"dxcluster/pathreliability"
)

const firstWaveComparisonProtocol = "path-first-wave-comparison-v1"

// This file must compile unchanged on the original 5d40e7f and final code. New
// diagnostics are decoded from complete Result JSON into a private projection;
// absence means unavailable, never numeric zero. No later test helper is used.
type firstWaveSupport struct {
	RawUsable, Admitted         float64
	AdmittedAvailable, Enforced bool
}

type firstWaveResult struct {
	Glyph, Class, P50Glyph                                         string
	P50DB, Weight, RawWeight, CappedWeight                         float64
	HasP50, HasObservedSamplingTime, ObservedDistributionAvailable bool
	ObservedSamplingUnix, AgeSec                                   int64
	Count, ObservationCount, RawCount, CappedCount                 uint32
	ReceiverCount, ReceiverRequired                                uint32
	CapLimited, CapWouldBlock, BeaconRX                            bool
	Source                                                         pathreliability.PredictionSource
	InsufficientReason                                             pathreliability.InsufficientReason
	VOACAPFT8SNRDB, VOACAPSSN, VOACAPHourUTC                       int
	VOACAPAgeSec                                                   int64
	ObservedRecentSupport                                          *firstWaveSupport
}

type firstWaveReport struct {
	Cells                     [4]uint16
	Receiver                  uint64
	DB, Weight                float64
	ObservedUnix, ArrivedUnix int64
	Beacon                    bool
}

type firstWaveInput struct {
	Case, CapMode         string
	Cells                 [4]uint16
	QueryUnix             int64
	Beacon, ForecastReady bool
	Reports               []firstWaveReport
}

// Every material input is explicit and shared by both variants. The receipt
// hashes this descriptor and every ordered report, excluding variant labels.
type firstWaveConfig struct {
	Band, Mode, UserGrid, DXGrid                                       string
	NoiseDB                                                            float64
	BeaconWeightCap, ForecastMHz                                       float64
	HalfLife, StaleSeconds                                             int
	StaleMultiplier, PredictionAgeMultiplier, MinWeight                float64
	OrdinaryFloor, BeaconFloor, FineSlots, CoarseSlots                 int
	ReceiverCapCount                                                   uint32
	ReceiverCapWeight, MinFine, FineOnly, RXMix, TXMix, SingleDiscount float64
	High, Medium, Low, Unlikely, Closed                                float64
	Glyphs                                                             [6]string
	GatedOpen, SparseUpgrade, Native160                                bool
	ForecastDB, ForecastSSN                                            int
	ForecastAge                                                        int64
}

func firstWaveFixedConfig() firstWaveConfig {
	return firstWaveConfig{
		Band: "20m", Mode: "FT8", UserGrid: "FN31", DXGrid: "IO91", NoiseDB: 0, BeaconWeightCap: 1, ForecastMHz: 14.1,
		HalfLife: 360, StaleSeconds: 1800, StaleMultiplier: 5, PredictionAgeMultiplier: 1.25, MinWeight: 0.5,
		OrdinaryFloor: 21, BeaconFloor: 11, FineSlots: 6, CoarseSlots: 12, ReceiverCapCount: 8, ReceiverCapWeight: 8,
		MinFine: 5, FineOnly: 20, RXMix: 0.6, TXMix: 0.4, SingleDiscount: 0.5,
		High: -13, Medium: -17, Low: -21, Unlikely: -24, Closed: -29, Glyphs: [6]string{">", "=", "<", "-", " ", "#"},
		GatedOpen: false, SparseUpgrade: false, Native160: false, ForecastDB: -34, ForecastSSN: 112, ForecastAge: 42,
	}
}

func firstWavePredictor(input firstWaveConfig, mode string) *pathreliability.Predictor {
	cfg := pathreliability.DefaultConfig()
	cfg.Enabled, cfg.DisplayEnabled = true, true
	cfg.AllowedBands = []string{input.Band}
	cfg.DefaultHalfLifeSec, cfg.BandHalfLifeSec = input.HalfLife, map[string]int{input.Band: input.HalfLife}
	cfg.StaleAfterSeconds, cfg.StaleAfterHalfLifeMultiplier = input.StaleSeconds, input.StaleMultiplier
	cfg.MaxPredictionAgeHalfLifeMultiplier, cfg.MinEffectiveWeight = input.PredictionAgeMultiplier, input.MinWeight
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = input.OrdinaryFloor, input.BeaconFloor
	cfg.ReceiverContributionMode, cfg.ReceiverFineSlots, cfg.ReceiverCoarseSlots = mode, input.FineSlots, input.CoarseSlots
	cfg.ReceiverMaxEffectiveCount, cfg.ReceiverMaxEffectiveWeight = input.ReceiverCapCount, input.ReceiverCapWeight
	cfg.MinFineWeight, cfg.FineOnlyWeight = input.MinFine, input.FineOnly
	cfg.MergeReceiveWeight, cfg.MergeTransmitWeight, cfg.ReverseHintDiscount = input.RXMix, input.TXMix, input.SingleDiscount
	cfg.BeaconWeightCap = input.BeaconWeightCap
	thresholds := cfg.ModeThresholds[input.Mode]
	thresholds.High, thresholds.Medium, thresholds.Low, thresholds.Unlikely, thresholds.Closed = input.High, input.Medium, input.Low, input.Unlikely, input.Closed
	cfg.GlyphThresholds, cfg.ModeThresholds = thresholds, map[string]pathreliability.GlyphThresholds{input.Mode: thresholds}
	cfg.GlyphSymbols = pathreliability.GlyphSymbols{High: input.Glyphs[0], Medium: input.Glyphs[1], Low: input.Glyphs[2], Unlikely: input.Glyphs[3], Insufficient: input.Glyphs[4], Closed: input.Glyphs[5]}
	cfg.VOACAPFallback.Enabled = false
	cfg.VOACAPFallback.ReliabilityGatedOpenEnabled, cfg.VOACAPFallback.ReliabilitySparseUpgradeEnabled = input.GatedOpen, input.SparseUpgrade
	cfg.Native160Fallback.Enabled = input.Native160
	return pathreliability.NewPredictor(cfg, []string{input.Band})
}

func firstWaveReports(cells [4]uint16, count, receivers int, firstHash uint64, db float64, beacon bool) []firstWaveReport {
	reports := make([]firstWaveReport, count)
	for i := range reports {
		reports[i] = firstWaveReport{cells, firstHash + uint64(i%receivers), db, 1, 1_700_000_000, 1_700_000_000, beacon}
	}
	return reports
}

func firstWaveInputs() []firstWaveInput {
	var cases []firstWaveInput
	add := func(name string, query [4]uint16, aged, beacon bool, reports []firstWaveReport) {
		at := int64(1_700_000_000)
		if aged {
			at += 360
		}
		cases = append(cases, firstWaveInput{Case: name, Cells: query, QueryUnix: at, Beacon: beacon, Reports: reports})
	}
	forward, reverse := [4]uint16{1, 2, 3, 4}, [4]uint16{2, 1, 4, 3}
	fresh := append(firstWaveReports(forward, 21, 6, 1, -10, false), firstWaveReports(reverse, 21, 6, 101, -10, false)...)
	add("fresh_distinct42", forward, false, false, fresh)
	add("single21_aged_half_life", forward, true, false, firstWaveReports(forward, 21, 6, 1, -10, false))
	shared := [4]uint16{1, 1, 3, 3}
	add("shared_regional11", shared, false, false, firstWaveReports(shared, 11, 6, 1, -10, false))
	local := append(firstWaveReports(forward, 16, 6, 1, -10, false), firstWaveReports([4]uint16{5, 6, 3, 4}, 21, 6, 1, -20, false)...)
	add("local16_regional21", forward, false, false, local)
	add("beacon11_aged_half_life", forward, true, true, firstWaveReports(forward, 11, 6, 1, -10, true))
	add("receivers60_aged_half_life", forward, true, false, firstWaveReports(forward, 60, 60, 1, -10, false))
	inputs := make([]firstWaveInput, 0, 36)
	for _, item := range cases {
		for _, mode := range []string{"off", "shadow", "enforce"} {
			for _, ready := range []bool{false, true} {
				input := item
				input.CapMode, input.ForecastReady = mode, ready
				inputs = append(inputs, input)
			}
		}
	}
	return inputs
}

type firstWaveFallback struct {
	forecast    pathreliability.VOACAPCachedForecast
	ready       bool
	calls       int
	lastRequest pathreliability.VOACAPClosedRequest
	lastAt      time.Time
}

func (f *firstWaveFallback) CheckForecast(req pathreliability.VOACAPClosedRequest, at time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	f.calls++
	f.lastRequest, f.lastAt = req, at
	return f.forecast, f.ready
}

type firstWaveExpected struct {
	db, weight, recent                     float64
	lifetime, capped, receivers, required  uint32
	age                                    int64
	class, glyph, observedClass, pathClass string
	source                                 pathreliability.PredictionSource
	reason                                 pathreliability.InsufficientReason
	limited, wouldBlock                    bool
}

// Expected values are independent bin-majority/count arithmetic. In particular,
// legacy shared reports count22, local bins have32 strong versus21 weak, and
// the60-report fine population retains six slots. Candidate populations differ.
func firstWaveOracle(input firstWaveInput, variant string) firstWaveExpected {
	want := firstWaveExpected{db: -9.5, class: "HIGH", glyph: ">", observedClass: "HIGH", pathClass: "HIGH", source: pathreliability.SourceCombined}
	switch input.Case {
	case "fresh_distinct42":
		want.weight, want.recent, want.lifetime, want.receivers = 21, 42, 42, 12
	case "single21_aged_half_life":
		want.weight, want.recent, want.lifetime, want.receivers, want.age = 5.25, 10.5, 21, 6, 360
	case "shared_regional11":
		want.weight, want.recent, want.lifetime, want.receivers = 11, 11, 22, 12
		if variant == "candidate" {
			want.weight, want.lifetime, want.receivers = 5.5, 11, 6
		}
	case "local16_regional21":
		want.weight, want.recent, want.lifetime, want.receivers = 18.5, 37, 37, 6
		if variant == "candidate" {
			want.db, want.class, want.glyph, want.observedClass, want.pathClass = -19.5, "LOW", "<", "LOW", "LOW"
		}
	case "beacon11_aged_half_life":
		want.weight, want.recent, want.lifetime, want.receivers, want.age = 5.5, 5.5, 11, 6, 360
		if variant == "candidate" {
			want.receivers = 5
		}
	case "receivers60_aged_half_life":
		want.weight, want.recent, want.lifetime, want.receivers, want.age = 15, 30, 60, 6, 360
		if variant == "candidate" {
			want.receivers = 0
		}
	}
	want.capped = want.lifetime
	if want.age != 0 {
		switch input.Case {
		case "single21_aged_half_life":
			want.capped = 10
		case "beacon11_aged_half_life":
			want.capped = 5
		case "receivers60_aged_half_life":
			want.capped = 30
		}
	}
	if input.CapMode == "off" {
		want.receivers = 0
		want.capped = want.lifetime
	} else {
		want.required = 3
		if input.Beacon {
			want.required = 2
		}
		want.limited = want.age != 0
	}
	if variant == "candidate" {
		switch input.Case {
		case "single21_aged_half_life", "shared_regional11", "beacon11_aged_half_life":
			want.source, want.reason = pathreliability.SourceInsufficient, pathreliability.InsufficientLowCount
		case "receivers60_aged_half_life":
			if input.CapMode == "enforce" {
				want.source, want.reason = pathreliability.SourceInsufficient, pathreliability.InsufficientLowReceiver
			}
		}
		want.wouldBlock = input.CapMode == "shadow" && (input.Case == "single21_aged_half_life" || input.Case == "shared_regional11" || input.Case == "beacon11_aged_half_life" || input.Case == "receivers60_aged_half_life")
	}
	if want.source == pathreliability.SourceInsufficient {
		want.class, want.glyph, want.pathClass = "", " ", "INSUFFICIENT"
	}
	return want
}

type firstWaveRecord struct {
	ID                                               string         `json:"id"`
	Input                                            firstWaveInput `json:"input"`
	InputSHA256                                      string         `json:"input_sha256"`
	BaseRaw, FinalRaw                                json.RawMessage
	NormalizedConfigRaw                              json.RawMessage
	Base, Final                                      firstWaveResult
	BaseObservedClass, BasePathClass, FinalPathClass string
	RecentSupportTracked                             bool
	ForecastCalls                                    int
	Failures                                         []string
}

func firstWaveID(input firstWaveInput) string {
	forecast := "unavailable"
	if input.ForecastReady {
		forecast = "ready_closed"
	}
	return input.Case + "/" + input.CapMode + "/" + forecast
}

func firstWaveJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func firstWaveDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func firstWaveProjection(t *testing.T, result pathreliability.Result) (json.RawMessage, firstWaveResult) {
	t.Helper()
	raw := firstWaveJSON(t, result)
	var projection firstWaveResult
	if err := json.Unmarshal(raw, &projection); err != nil {
		t.Fatal(err)
	}
	return raw, projection
}

func firstWaveSupportFailure(got *firstWaveSupport, input firstWaveInput, variant string, units float64) string {
	if variant == "baseline" {
		if got != nil {
			return "original baseline unexpectedly reports recent support"
		}
		return ""
	}
	want := firstWaveSupport{RawUsable: units, Admitted: units, AdmittedAvailable: input.CapMode != "off", Enforced: input.CapMode == "enforce"}
	if input.CapMode == "off" {
		want.Admitted = 0
	}
	if got == nil || *got != want {
		return fmt.Sprintf("recent support=%+v, want %+v", got, want)
	}
	return ""
}

func firstWaveCompareBase(got firstWaveResult, want firstWaveExpected, input firstWaveInput, variant string) []string {
	var failures []string
	if got.Source != want.source || got.InsufficientReason != want.reason || got.Class != want.class || got.Glyph != want.glyph {
		failures = append(failures, fmt.Sprintf("base source/reason/class/glyph=%d/%s/%q/%q, want%d/%s/%q/%q", got.Source, got.InsufficientReason, got.Class, got.Glyph, want.source, want.reason, want.class, want.glyph))
	}
	if !got.HasP50 || !got.ObservedDistributionAvailable || got.P50DB != want.db || got.P50Glyph != map[string]string{"HIGH": ">", "LOW": "<"}[want.observedClass] {
		failures = append(failures, "base observed median/glyph/distribution differs from literal bin majority")
	}
	count := want.lifetime
	if input.CapMode == "enforce" {
		count = want.capped
	}
	if got.Count != count || got.ObservationCount != want.lifetime || got.RawCount != want.lifetime || got.CappedCount != want.capped {
		failures = append(failures, fmt.Sprintf("base legacy counts=%d/%d/%d/%d want%d/%d/%d/%d", got.Count, got.ObservationCount, got.RawCount, got.CappedCount, count, want.lifetime, want.lifetime, want.capped))
	}
	if got.ReceiverCount != want.receivers || got.ReceiverRequired != want.required || got.CapLimited != want.limited || got.CapWouldBlock != want.wouldBlock {
		failures = append(failures, "base receiver/cap diagnostics differ from literal physical populations")
	}
	for _, weight := range []float64{got.Weight, got.RawWeight, got.CappedWeight} {
		if !(math.Abs(weight-want.weight) <= 1e-12) {
			failures = append(failures, fmt.Sprintf("base signal weight=%g, want%g", weight, want.weight))
			break
		}
	}
	if got.AgeSec != want.age || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != input.QueryUnix || got.BeaconRX != input.Beacon {
		failures = append(failures, "base age/sampling reference/direction changed")
	}
	if failure := firstWaveSupportFailure(got.ObservedRecentSupport, input, variant, want.recent); failure != "" {
		failures = append(failures, "base: "+failure)
	}
	return failures
}

func firstWaveRun(t *testing.T, config firstWaveConfig, input firstWaveInput, variant string) firstWaveRecord {
	t.Helper()
	p := firstWavePredictor(config, input.CapMode)
	for position, report := range input.Reports {
		admission := p.UpdateWithReceiverHash(pathreliability.BucketCombined, pathreliability.CellID(report.Cells[0]), pathreliability.CellID(report.Cells[1]), pathreliability.CellID(report.Cells[2]), pathreliability.CellID(report.Cells[3]), config.Band, report.DB, report.Weight,
			pathreliability.ObservationTimes{ObservedAt: time.Unix(report.ObservedUnix, 0), ArrivedAt: time.Unix(report.ArrivedUnix, 0)}, report.Beacon, report.Receiver)
		if !admission.Accepted() || admission.EffectiveObservation.Unix() != report.ObservedUnix {
			t.Fatalf("fixed report position%d rejected/changed: %+v", position, admission)
		}
	}
	query := input.Cells
	at := time.Unix(input.QueryUnix, 0).UTC()
	var base pathreliability.Result
	if input.Beacon {
		base = p.PredictBeaconReceiveOnlyWithMinObservationCount(pathreliability.CellID(query[0]), pathreliability.CellID(query[1]), pathreliability.CellID(query[2]), pathreliability.CellID(query[3]), config.Band, config.Mode, config.NoiseDB, config.BeaconFloor, at)
	} else {
		base = p.PredictWithMinObservationCount(pathreliability.CellID(query[0]), pathreliability.CellID(query[1]), pathreliability.CellID(query[2]), pathreliability.CellID(query[3]), config.Band, config.Mode, config.NoiseDB, config.OrdinaryFloor, at)
	}
	provider := &firstWaveFallback{ready: input.ForecastReady, forecast: pathreliability.VOACAPCachedForecast{Record: pathreliability.VOACAPHourlyForecast{FT8SNRDB: config.ForecastDB, HourUTC: at.Hour(), FrequencyMHz: config.ForecastMHz}, SSN: config.ForecastSSN, AgeSec: config.ForecastAge}}
	s := &Server{pathPredictor: p, pathClosedFallback: provider}
	req := pathreliability.VOACAPClosedRequest{UserCell: pathreliability.CellID(query[0]), DXCell: pathreliability.CellID(query[1]), UserGrid: config.UserGrid, DXGrid: config.DXGrid, Band: config.Band, Mode: config.Mode, ReceiveNoisePenaltyDB: config.NoiseDB}
	final := s.pathResultWithClosedFallback(base, req, at)
	record := firstWaveRecord{ID: firstWaveID(input), Input: input, NormalizedConfigRaw: firstWaveJSON(t, p.Config()), InputSHA256: firstWaveDigest(firstWaveJSON(t, struct {
		Config firstWaveConfig
		Input  firstWaveInput
	}{config, input})), ForecastCalls: provider.calls}
	record.BaseRaw, record.Base = firstWaveProjection(t, base)
	record.FinalRaw, record.Final = firstWaveProjection(t, final)
	record.BaseObservedClass = pathreliability.ClassForDB(base.P50DB, config.Mode, p.Config())
	record.BasePathClass, record.FinalPathClass = pathClassFromPrediction(pathPrediction{result: base}), pathClassFromPrediction(pathPrediction{result: final})
	record.RecentSupportTracked = record.Base.ObservedRecentSupport != nil
	want := firstWaveOracle(input, variant)
	record.Failures = firstWaveCompareBase(record.Base, want, input, variant)
	if record.BaseObservedClass != want.observedClass || record.BasePathClass != want.pathClass {
		record.Failures = append(record.Failures, "base observed/operator class differs from literal oracle")
	}
	expectedCalls := 0
	if want.source == pathreliability.SourceInsufficient {
		expectedCalls = 1
	}
	if provider.calls != expectedCalls {
		record.Failures = append(record.Failures, fmt.Sprintf("forecast calls=%d, want%d", provider.calls, expectedCalls))
	}
	if expectedCalls == 1 && (provider.lastRequest.UserCell != req.UserCell || provider.lastRequest.DXCell != req.DXCell || provider.lastRequest.UserGrid != req.UserGrid || provider.lastRequest.DXGrid != req.DXGrid || provider.lastRequest.Band != req.Band || provider.lastRequest.Mode != req.Mode || provider.lastRequest.ReceiveNoisePenaltyDB != 0 || !provider.lastAt.Equal(at)) {
		record.Failures = append(record.Failures, "forecast received a different fixed request/clock")
	}
	if want.source == pathreliability.SourceInsufficient && input.ForecastReady {
		got := record.Final
		if got.Source != pathreliability.SourceVOACAPClosed || got.Glyph != "#" || got.Class != "UNLIKELY" || got.P50DB != -34 || !got.HasP50 || got.BeaconRX != input.Beacon || record.FinalPathClass != "CLOSED" {
			record.Failures = append(record.Failures, "ready CLOSED forecast did not replace positive insufficient observed evidence")
		}
		if !got.HasObservedSamplingTime || got.ObservedSamplingUnix != input.QueryUnix || !got.ObservedDistributionAvailable || got.AgeSec != 42 || got.VOACAPAgeSec != 42 || got.VOACAPFT8SNRDB != -34 || got.VOACAPSSN != 112 || got.VOACAPHourUTC != at.Hour() {
			record.Failures = append(record.Failures, "CLOSED replacement lost observed/forecast provenance")
		}
		if got.Count != 0 || got.ObservationCount != 0 || got.RawCount != 0 || got.CappedCount != 0 || got.ReceiverCount != 0 || got.ReceiverRequired != 0 || got.CapLimited || got.CapWouldBlock {
			record.Failures = append(record.Failures, "CLOSED replacement changed legacy forecast-only count/cap diagnostics")
		}
	} else if !bytes.Equal(record.BaseRaw, record.FinalRaw) {
		record.Failures = append(record.Failures, "sufficient authority or unavailable forecast changed complete base Result")
	}
	if failure := firstWaveSupportFailure(record.Final.ObservedRecentSupport, input, variant, want.recent); failure != "" {
		record.Failures = append(record.Failures, "final: "+failure)
	}
	return record
}

func TestPathFirstWaveComparison(t *testing.T) {
	variant := os.Getenv("GOCLUSTER_PATH_WAVE_VARIANT")
	if variant == "" {
		t.Skip("original/final fixed-input comparison is opt-in")
	}
	if variant != "baseline" && variant != "candidate" {
		t.Fatal("GOCLUSTER_PATH_WAVE_VARIANT must be baseline or candidate")
	}
	output := os.Getenv("GOCLUSTER_PATH_WAVE_OUTPUT")
	if output == "" {
		t.Fatal("GOCLUSTER_PATH_WAVE_OUTPUT is required for an opted-in comparison")
	}
	config, inputs := firstWaveFixedConfig(), firstWaveInputs()
	inputDocument := struct {
		Protocol string
		Config   firstWaveConfig
		Inputs   []firstWaveInput
	}{firstWaveComparisonProtocol, config, inputs}
	inputHash := firstWaveDigest(firstWaveJSON(t, inputDocument))
	records := make([]firstWaveRecord, 0, 36)
	seen := make(map[string]bool, 36)
	for _, input := range inputs {
		id := firstWaveID(input)
		if seen[id] {
			t.Errorf("duplicate fixed input %s", id)
		}
		seen[id] = true
		t.Run(id, func(t *testing.T) {
			record := firstWaveRun(t, config, input, variant)
			records = append(records, record)
			for _, failure := range record.Failures {
				t.Error(failure)
			}
		})
	}
	if len(inputs) != 36 || len(records) != 36 || firstWaveDigest(firstWaveJSON(t, inputDocument)) != inputHash {
		t.Error("fixed input/record cardinality or immutable input hash changed")
	}
	receipt := struct {
		Protocol, Variant, GoVersion, InputSHA256 string
		ExpectedRecords                           int
		Complete, Passed                          bool
		Config                                    firstWaveConfig
		Records                                   []firstWaveRecord
	}{firstWaveComparisonProtocol, variant, runtime.Version(), inputHash, 36, len(records) == 36, !t.Failed(), config, records}
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("first-wave comparison: variant=%s records=%d input_sha256=%s output=%s", variant, len(records), inputHash, output)
}
