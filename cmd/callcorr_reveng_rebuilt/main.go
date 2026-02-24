package main

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

const (
	defaultEvalNegatives = 10000
	neighborCap          = 250000
)

type inferredMethod struct {
	WindowSec            int    `json:"window_sec"`
	ToleranceHz          int64  `json:"tolerance_hz"`
	FreqModel            string `json:"freq_model"`
	TopK                 int    `json:"top_k"`
	MinReports           int    `json:"min_reports"`
	MinAdvantage         int    `json:"min_advantage"`
	MinConfidencePercent int    `json:"min_confidence_percent"`
	MaxEditDistance      int    `json:"max_edit_distance"`
}

type mismatch struct {
	TsUnix        int64  `json:"ts_unix"`
	FreqHz        int64  `json:"freq_hz"`
	Spotter       string `json:"spotter"`
	Subject       string `json:"subject"`
	Expected      string `json:"expected"`
	Predicted     string `json:"predicted"`
	TotalReporter int    `json:"total_reporters"`
}

type evalReport struct {
	RBNRows          int64      `json:"rbn_rows"`
	PositivesTotal   int64      `json:"positives_total"`
	NegativesSampled int64      `json:"negatives_sampled"`
	CorrectPositive  int64      `json:"correct_positive"`
	MissedPositive   int64      `json:"missed_positive"`
	FalsePositive    int64      `json:"false_positive"`
	TrueNegative     int64      `json:"true_negative"`
	Mismatches       []mismatch `json:"mismatches,omitempty"`
}

type trainingReport struct {
	ExamplesTotal   int           `json:"examples_total"`
	Method          inferredMethod `json:"method"`
	Mode            string        `json:"mode"`
	Negatives       int           `json:"negatives"`
	PipelineSource  string        `json:"pipeline_source"`
	Positives       int           `json:"positives"`
}

type otherLoadStats struct {
	Lines          int64 `json:"lines"`
	Parsed         int64 `json:"parsed"`
	SkippedHarmonic int64 `json:"skipped_harmonic"`
	SkippedInvalid int64 `json:"skipped_invalid"`
	SkippedNoDB    int64 `json:"skipped_no_db"`
	Conflicts      int64 `json:"conflicts"`
}

type pipelineMethodConfig struct {
	CallCorrection struct {
		MinConsensusReports int     `yaml:"min_consensus_reports"`
		CandidateEvalTopK   int     `yaml:"candidate_eval_top_k"`
		MinAdvantage        int     `yaml:"min_advantage"`
		MinConfidencePct    int     `yaml:"min_confidence_percent"`
		MaxEditDistance     int     `yaml:"max_edit_distance"`
		RecencySeconds      int     `yaml:"recency_seconds"`
		RecencySecondsCW    int     `yaml:"recency_seconds_cw"`
		FreqToleranceHz     float64 `yaml:"frequency_tolerance_hz"`
	} `yaml:"call_correction"`
}

type interner struct {
	ids map[string]uint32
	arr []string
}

func newInterner() *interner {
	return &interner{ids: map[string]uint32{}, arr: []string{""}}
}

func (i *interner) intern(raw string) uint32 {
	call := normalizeCall(raw)
	if call == "" {
		return 0
	}
	if id, ok := i.ids[call]; ok {
		return id
	}
	id := uint32(len(i.arr))
	i.ids[call] = id
	i.arr = append(i.arr, call)
	return id
}

func (i *interner) str(id uint32) string {
	if int(id) < len(i.arr) {
		return i.arr[id]
	}
	return ""
}

type key struct {
	MinuteUnix int64
	FreqKey    int64
	SpotterID  uint32
	SubjectID  uint32
}

type correctionEvent struct {
	WinnerID uint32
	DB       int16
}

type row struct {
	TsUnix    int64
	Minute    int64
	FreqHz    int64
	SpotterID uint32
	SubjectID uint32
}

type neighborStats struct {
	Total int
	Calls map[uint32]int
}

type targetKind int

const (
	targetPositive targetKind = iota
	targetNegative
)

type target struct {
	Kind       targetKind
	Row        row
	ExpectedID uint32
	Stats      neighborStats
}

type coarseKey struct {
	Minute int64
	Bin    int64
}

type xorshift64 struct {
	state uint64
}

func (x *xorshift64) next() uint64 {
	if x.state == 0 {
		x.state = 1
	}
	s := x.state
	s ^= s << 13
	s ^= s >> 7
	s ^= s << 17
	x.state = s
	return s
}

func (x *xorshift64) chance(num, den int64) bool {
	if den <= 0 || num <= 0 {
		return false
	}
	return int64(x.next()%uint64(den)) < num
}

func main() {
	var (
		rbnDir     = flag.String("rbn-dir", "archive data", "RBN .zip/.csv directory")
		otherDir   = flag.String("other-dir", "archive data", "Other cluster .txt/.log directory")
		outDir     = flag.String("out", filepath.Join("data", "analysis", "callcorr_reveng_rebuilt"), "Output directory")
		pipelineY  = flag.String("fixed-from-pipeline-yaml", "data/config/pipeline.yaml", "Pipeline YAML for fixed method")
		evalNeg    = flag.Int("eval-negatives", defaultEvalNegatives, "Max sampled negatives")
		seed       = flag.Uint64("seed", 1, "Sampling seed")
		windowSec  = flag.Int("fixed-window-sec", -1, "Override window")
		tolerance  = flag.Int64("fixed-tolerance-hz", -1, "Override tolerance")
		topK       = flag.Int("fixed-top-k", -1, "Override top-k")
		minReports = flag.Int("fixed-min-reports", -1, "Override min-reports")
		minAdv     = flag.Int("fixed-min-advantage", -1, "Override min-advantage")
		minConf    = flag.Int("fixed-min-confidence-percent", -1, "Override min-confidence")
		maxEdit    = flag.Int("fixed-max-edit-distance", -1, "Override max-edit-distance")
	)
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.LUTC)
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}

	method, err := loadMethod(*pipelineY)
	if err != nil {
		log.Fatal(err)
	}
	applyOverrides(&method, *windowSec, *tolerance, *topK, *minReports, *minAdv, *minConf, *maxEdit)
	if err := validateMethod(method); err != nil {
		log.Fatal(err)
	}

	rbnFiles, err := discover(*rbnDir, []string{".zip", ".csv"})
	if err != nil {
		log.Fatal(err)
	}
	otherFiles, err := discover(*otherDir, []string{".txt", ".log"})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	in := newInterner()
	positives, otherStats, err := loadOther(otherFiles, in)
	if err != nil {
		log.Fatal(err)
	}
	targets, rowsTotal, err := collectTargets(ctx, rbnFiles, positives, in, *evalNeg, *seed)
	if err != nil {
		log.Fatal(err)
	}
	if err := hydrateNeighbors(ctx, rbnFiles, targets, in, method); err != nil {
		log.Fatal(err)
	}
	report := scoreTargets(targets, in, method)
	report.RBNRows = rowsTotal
	report.PositivesTotal = int64(countByKind(targets, targetPositive))
	report.NegativesSampled = int64(countByKind(targets, targetNegative))

	train := trainingReport{
		ExamplesTotal:  len(targets),
		Method:         method,
		Mode:           "fixed_method",
		Negatives:      countByKind(targets, targetNegative),
		PipelineSource: *pipelineY,
		Positives:      countByKind(targets, targetPositive),
	}

	mustWrite(filepath.Join(*outDir, "inferred_method.json"), method)
	mustWrite(filepath.Join(*outDir, "training_report.json"), train)
	mustWrite(filepath.Join(*outDir, "eval_report.json"), report)
	writeSummary(filepath.Join(*outDir, "summary.txt"), method, report, otherStats, rowsTotal, len(targets))
	log.Printf("Wrote outputs to %s", *outDir)
}

func loadMethod(path string) (inferredMethod, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return inferredMethod{}, err
	}
	var cfg pipelineMethodConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return inferredMethod{}, err
	}
	window := cfg.CallCorrection.RecencySeconds
	if cfg.CallCorrection.RecencySecondsCW > 0 {
		window = cfg.CallCorrection.RecencySecondsCW
	}
	return inferredMethod{
		WindowSec:            window,
		ToleranceHz:          int64(math.Round(cfg.CallCorrection.FreqToleranceHz)),
		FreqModel:            "bucket",
		TopK:                 cfg.CallCorrection.CandidateEvalTopK,
		MinReports:           cfg.CallCorrection.MinConsensusReports,
		MinAdvantage:         cfg.CallCorrection.MinAdvantage,
		MinConfidencePercent: cfg.CallCorrection.MinConfidencePct,
		MaxEditDistance:      cfg.CallCorrection.MaxEditDistance,
	}, nil
}

func applyOverrides(m *inferredMethod, w int, t int64, top, reps, adv, conf, edit int) {
	if w >= 0 {
		m.WindowSec = w
	}
	if t >= 0 {
		m.ToleranceHz = t
	}
	if top >= 0 {
		m.TopK = top
	}
	if reps >= 0 {
		m.MinReports = reps
	}
	if adv >= 0 {
		m.MinAdvantage = adv
	}
	if conf >= 0 {
		m.MinConfidencePercent = conf
	}
	if edit >= 0 {
		m.MaxEditDistance = edit
	}
}

func validateMethod(m inferredMethod) error {
	if m.WindowSec <= 0 || m.ToleranceHz <= 0 || m.TopK <= 0 || m.MinReports <= 0 || m.MinConfidencePercent <= 0 || m.MinConfidencePercent > 100 || m.MaxEditDistance < 0 {
		return errors.New("invalid fixed method")
	}
	if m.FreqModel == "" {
		return errors.New("freq_model required")
	}
	return nil
}

func discover(root string, exts []string) ([]string, error) {
	set := map[string]struct{}{}
	for _, ext := range exts {
		set[strings.ToLower(ext)] = struct{}{}
	}
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if _, ok := set[strings.ToLower(filepath.Ext(d.Name()))]; ok {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func loadOther(files []string, in *interner) (map[key][]correctionEvent, otherLoadStats, error) {
	out := map[key][]correctionEvent{}
	stats := otherLoadStats{}
	for _, path := range files {
		year, ok := inferYear(filepath.Base(path))
		if !ok {
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, stats, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
		for sc.Scan() {
			stats.Lines++
			parts := strings.Fields(sc.Text())
			if len(parts) < 8 || strings.ToUpper(parts[len(parts)-1]) != "CW" {
				continue
			}
			dbIdx := -1
			for i := len(parts) - 3; i >= 4; i-- {
				if _, err := strconv.Atoi(parts[i]); err == nil {
					dbIdx = i
					break
				}
			}
			if dbIdx < 0 {
				stats.SkippedNoDB++
				continue
			}
			corr := strings.Join(parts[4:dbIdx], " ")
			if corr == "?" || corr == "" || strings.Contains(corr, "Harmonic") || strings.HasPrefix(corr, "*") {
				stats.SkippedHarmonic++
				continue
			}
			corr = strings.TrimPrefix(corr, "?")
			ts, err := time.Parse("02-Jan-2006 1504Z", fmt.Sprintf("%s-%04d %s", parts[0], year, parts[1]))
			if err != nil {
				stats.SkippedInvalid++
				continue
			}
			dbVal, _ := strconv.ParseInt(parts[dbIdx], 10, 16)
			spotterID := in.intern(parts[len(parts)-2])
			subjectID := in.intern(parts[2])
			winnerID := in.intern(corr)
			if spotterID == 0 || subjectID == 0 || winnerID == 0 || subjectID == winnerID || !spot.IsValidNormalizedCallsign(in.str(subjectID)) || !spot.IsValidNormalizedCallsign(in.str(winnerID)) {
				stats.SkippedInvalid++
				continue
			}
			k := key{
				MinuteUnix: ts.UTC().Unix(),
				FreqKey:    keyHzFromKHz(parts[3]),
				SpotterID:  spotterID,
				SubjectID:  subjectID,
			}
			existing := out[k]
			if len(existing) > 0 && existing[0].WinnerID != winnerID {
				stats.Conflicts++
			}
			out[k] = append(out[k], correctionEvent{WinnerID: winnerID, DB: int16(dbVal)})
			stats.Parsed++
		}
		if err := sc.Err(); err != nil {
			_ = f.Close()
			return nil, stats, err
		}
		if err := f.Close(); err != nil {
			return nil, stats, err
		}
	}
	return out, stats, nil
}

func collectTargets(ctx context.Context, rbnFiles []string, positives map[key][]correctionEvent, in *interner, evalNeg int, seed uint64) ([]target, int64, error) {
	var targets []target
	var rows int64
	var negSeen int64
	prng := &xorshift64{state: seed}
	for _, path := range rbnFiles {
		if err := ctx.Err(); err != nil {
			return nil, rows, err
		}
		r, closeFn, err := openRBN(path)
		if err != nil {
			return nil, rows, err
		}
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
		if !sc.Scan() {
			_ = closeFn()
			return nil, rows, sc.Err()
		}
		hdr, err := parseRBNHeader(sc.Text())
		if err != nil {
			_ = closeFn()
			if strings.Contains(strings.ToLower(err.Error()), "missing required rbn csv column") {
				log.Printf("Skipping RBN input %q: %v", path, err)
				continue
			}
			return nil, rows, err
		}
		for sc.Scan() {
			if err := ctx.Err(); err != nil {
				_ = closeFn()
				return nil, rows, err
			}
			fields := strings.Split(sc.Text(), ",")
			if len(fields) <= hdr.txMode || strings.ToUpper(strings.TrimSpace(fields[hdr.txMode])) != "CW" {
				continue
			}
			rw, ok := parseRBNRow(fields, hdr, in)
			if !ok {
				continue
			}
			rows++
			k := key{
				MinuteUnix: rw.Minute,
				FreqKey:    keyHzFromHz(rw.FreqHz),
				SpotterID:  rw.SpotterID,
				SubjectID:  rw.SubjectID,
			}
			if ev := positives[k]; len(ev) > 0 {
				targets = append(targets, target{
					Kind:       targetPositive,
					Row:        rw,
					ExpectedID: majorityWinner(ev),
					Stats:      neighborStats{Calls: map[uint32]int{}},
				})
				continue
			}
			negSeen++
			if evalNeg <= 0 {
				continue
			}
			if len(targets) < evalNeg {
				targets = append(targets, target{
					Kind:  targetNegative,
					Row:   rw,
					Stats: neighborStats{Calls: map[uint32]int{}},
				})
				continue
			}
			if prng.chance(int64(evalNeg), negSeen) {
				idx := int(prng.next() % uint64(evalNeg))
				targets[idx] = target{
					Kind:  targetNegative,
					Row:   rw,
					Stats: neighborStats{Calls: map[uint32]int{}},
				}
			}
		}
		if err := sc.Err(); err != nil {
			_ = closeFn()
			return nil, rows, err
		}
		if err := closeFn(); err != nil {
			return nil, rows, err
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Row.TsUnix < targets[j].Row.TsUnix })
	return targets, rows, nil
}

func hydrateNeighbors(ctx context.Context, rbnFiles []string, targets []target, in *interner, method inferredMethod) error {
	if len(targets) == 0 {
		return nil
	}
	index := map[coarseKey][]int{}
	binHz := method.ToleranceHz
	if binHz <= 0 {
		binHz = 500
	}
	for i := range targets {
		t := targets[i].Row
		minLo := (t.TsUnix - int64(method.WindowSec)) / 60
		minHi := (t.TsUnix + int64(method.WindowSec)) / 60
		fLo := (t.FreqHz - method.ToleranceHz) / binHz
		fHi := (t.FreqHz + method.ToleranceHz) / binHz
		for minute := minLo; minute <= minHi; minute++ {
			for bin := fLo; bin <= fHi; bin++ {
				k := coarseKey{Minute: minute, Bin: bin}
				index[k] = append(index[k], i)
			}
		}
	}

	for _, path := range rbnFiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, closeFn, err := openRBN(path)
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
		if !sc.Scan() {
			_ = closeFn()
			return sc.Err()
		}
		hdr, err := parseRBNHeader(sc.Text())
		if err != nil {
			_ = closeFn()
			if strings.Contains(strings.ToLower(err.Error()), "missing required rbn csv column") {
				log.Printf("Skipping RBN input %q: %v", path, err)
				continue
			}
			return err
		}
		for sc.Scan() {
			if err := ctx.Err(); err != nil {
				_ = closeFn()
				return err
			}
			fields := strings.Split(sc.Text(), ",")
			if len(fields) <= hdr.txMode || strings.ToUpper(strings.TrimSpace(fields[hdr.txMode])) != "CW" {
				continue
			}
			rw, ok := parseRBNRow(fields, hdr, in)
			if !ok {
				continue
			}
			minute := rw.TsUnix / 60
			bin := rw.FreqHz / binHz
			idxs := index[coarseKey{Minute: minute, Bin: bin}]
			for _, ti := range idxs {
				tg := &targets[ti]
				if abs64(rw.TsUnix-tg.Row.TsUnix) > int64(method.WindowSec) || abs64(rw.FreqHz-tg.Row.FreqHz) > method.ToleranceHz {
					continue
				}
				if tg.Stats.Total < neighborCap {
					tg.Stats.Total++
				}
				tg.Stats.Calls[rw.SubjectID]++
			}
		}
		if err := sc.Err(); err != nil {
			_ = closeFn()
			return err
		}
		if err := closeFn(); err != nil {
			return err
		}
	}
	return nil
}

func scoreTargets(targets []target, in *interner, m inferredMethod) evalReport {
	report := evalReport{}
	report.Mismatches = make([]mismatch, 0, 25)
	for _, tg := range targets {
		pred := predict(tg, in, m)
		if tg.Kind == targetPositive {
			if pred == tg.ExpectedID {
				report.CorrectPositive++
			} else {
				report.MissedPositive++
				if len(report.Mismatches) < 25 {
					report.Mismatches = append(report.Mismatches, mismatch{
						TsUnix:        tg.Row.TsUnix,
						FreqHz:        tg.Row.FreqHz,
						Spotter:       in.str(tg.Row.SpotterID),
						Subject:       in.str(tg.Row.SubjectID),
						Expected:      in.str(tg.ExpectedID),
						Predicted:     in.str(pred),
						TotalReporter: tg.Stats.Total,
					})
				}
			}
			continue
		}
		if pred != 0 {
			report.FalsePositive++
		} else {
			report.TrueNegative++
		}
	}
	return report
}

func predict(tg target, in *interner, m inferredMethod) uint32 {
	if tg.Stats.Total < m.MinReports {
		return 0
	}
	type candidate struct {
		ID    uint32
		Count int
	}
	all := make([]candidate, 0, len(tg.Stats.Calls))
	for id, count := range tg.Stats.Calls {
		all = append(all, candidate{ID: id, Count: count})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Count == all[j].Count {
			return in.str(all[i].ID) < in.str(all[j].ID)
		}
		return all[i].Count > all[j].Count
	})
	for i := 0; i < len(all) && i < m.TopK; i++ {
		c := all[i]
		if c.ID == tg.Row.SubjectID || c.Count < m.MinReports {
			continue
		}
		runner := 0
		if len(all) > i+1 {
			runner = all[i+1].Count
		}
		if c.Count-runner < m.MinAdvantage {
			continue
		}
		conf := c.Count * 100 / tg.Stats.Total
		if conf < m.MinConfidencePercent {
			continue
		}
		if editDistance(in.str(tg.Row.SubjectID), in.str(c.ID)) > m.MaxEditDistance {
			continue
		}
		return c.ID
	}
	return 0
}

func parseRBNRow(fields []string, hdr csvHdr, in *interner) (row, bool) {
	if len(fields) <= hdr.callsign || len(fields) <= hdr.freq || len(fields) <= hdr.dx || len(fields) <= hdr.date {
		return row{}, false
	}
	ts, ok := parseRBNTime(fields[hdr.date])
	if !ok {
		return row{}, false
	}
	freqHz, ok := parseHz(fields[hdr.freq])
	if !ok {
		return row{}, false
	}
	spotterID := in.intern(fields[hdr.callsign])
	subjectID := in.intern(fields[hdr.dx])
	if spotterID == 0 || subjectID == 0 {
		return row{}, false
	}
	return row{
		TsUnix:    ts,
		Minute:    (ts / 60) * 60,
		FreqHz:    freqHz,
		SpotterID: spotterID,
		SubjectID: subjectID,
	}, true
}

type csvHdr struct{ callsign, freq, dx, date, txMode int }

func parseRBNHeader(line string) (csvHdr, error) {
	fields := strings.Split(strings.ToLower(strings.TrimSpace(line)), ",")
	idx := map[string]int{}
	for i, f := range fields {
		idx[strings.TrimSpace(f)] = i
	}
	required := []string{"callsign", "freq", "dx", "date", "tx_mode"}
	for _, key := range required {
		if _, ok := idx[key]; !ok {
			return csvHdr{}, fmt.Errorf("missing required RBN CSV column %q", key)
		}
	}
	return csvHdr{
		callsign: idx["callsign"],
		freq:     idx["freq"],
		dx:       idx["dx"],
		date:     idx["date"],
		txMode:   idx["tx_mode"],
	}, nil
}

func openRBN(path string) (io.Reader, func() error, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		return f, f.Close, nil
	case ".zip":
		z, err := zip.OpenReader(path)
		if err != nil {
			return nil, nil, err
		}
		for _, member := range z.File {
			if strings.HasSuffix(strings.ToLower(member.Name), ".csv") {
				rc, err := member.Open()
				if err != nil {
					_ = z.Close()
					return nil, nil, err
				}
				return rc, func() error {
					_ = rc.Close()
					return z.Close()
				}, nil
			}
		}
		_ = z.Close()
		return nil, nil, fmt.Errorf("zip %q: no .csv member found", path)
	default:
		return nil, nil, fmt.Errorf("unsupported RBN file extension: %s", filepath.Ext(path))
	}
}

func writeSummary(path string, m inferredMethod, report evalReport, other otherLoadStats, rows int64, examples int) {
	text := fmt.Sprintf(
		"callcorr_reveng summary\n======================\n\nOther logs: lines=%d parsed=%d skipped_harmonic=%d skipped_invalid=%d skipped_no_db=%d conflicts=%d\nTraining pass: rbn_rows=%d matched_pos=%d sampled_pos=%d sampled_neg=%d unmatched_pos=%d\n\nInferred method:\n  window_sec=%d\n  tolerance_hz=%d\n  freq_model=%s\n  top_k=%d\n  min_reports=%d\n  min_advantage=%d\n  min_confidence_percent=%d\n  max_edit_distance=%d\n\nEvaluation:\n  rbn_rows=%d\n  positives_total=%d\n  negatives_sampled=%d\n  correct_positive=%d\n  missed_positive=%d\n  false_positive=%d\n  true_negative=%d\n",
		other.Lines, other.Parsed, other.SkippedHarmonic, other.SkippedInvalid, other.SkippedNoDB, other.Conflicts,
		rows, report.PositivesTotal, countByKindReportExamples(examples, report.PositivesTotal), report.NegativesSampled, report.PositivesTotal-int64(countByKindReportExamples(examples, report.PositivesTotal)),
		m.WindowSec, m.ToleranceHz, m.FreqModel, m.TopK, m.MinReports, m.MinAdvantage, m.MinConfidencePercent, m.MaxEditDistance,
		report.RBNRows, report.PositivesTotal, report.NegativesSampled, report.CorrectPositive, report.MissedPositive, report.FalsePositive, report.TrueNegative,
	)
	_ = os.WriteFile(path, []byte(text), 0o644)
}

func countByKindReportExamples(examples int, positives int64) int {
	if int64(examples) < positives {
		return int(positives)
	}
	return int(positives)
}

func mustWrite(path string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Printf("marshal %s: %v", path, err)
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("write %s: %v", path, err)
	}
}

func normalizeCall(raw string) string {
	call := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(raw, ".", "/")))
	call = strings.TrimSuffix(call, "/")
	return call
}

func inferYear(base string) (int, bool) {
	parts := strings.Split(base, "-")
	for i := len(parts) - 1; i >= 0; i-- {
		p := strings.Split(parts[i], ".")[0]
		if len(p) == 4 {
			y, err := strconv.Atoi(p)
			if err == nil && y >= 1980 && y <= 2100 {
				return y, true
			}
		}
	}
	return 0, false
}

func keyHzFromKHz(s string) int64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return keyHzFromHz(int64(math.Round(f * 1000)))
}

func keyHzFromHz(hz int64) int64 {
	return (hz + 50) / 100
}

func parseRBNTime(raw string) (int64, bool) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, false
	}
	t, err := time.Parse("2006-01-02 15:04:05", v)
	if err != nil {
		return 0, false
	}
	return t.UTC().Unix(), true
}

func parseHz(raw string) (int64, bool) {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, false
	}
	return int64(math.Round(v * 1000)), true
}

func majorityWinner(events []correctionEvent) uint32 {
	if len(events) == 0 {
		return 0
	}
	counts := map[uint32]int{}
	bestID := uint32(0)
	bestN := 0
	for _, event := range events {
		counts[event.WinnerID]++
		n := counts[event.WinnerID]
		if n > bestN {
			bestN = n
			bestID = event.WinnerID
		}
	}
	return bestID
}

func countByKind(targets []target, kind targetKind) int {
	total := 0
	for _, tg := range targets {
		if tg.Kind == kind {
			total++
		}
	}
	return total
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func editDistance(a, b string) int {
	if a == b {
		return 0
	}
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		ai := a[i-1]
		for j := 1; j <= len(b); j++ {
			cost := 0
			if ai != b[j-1] {
				cost = 1
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = min3(del, ins, sub)
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func min3(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= a && b <= c {
		return b
	}
	return c
}
