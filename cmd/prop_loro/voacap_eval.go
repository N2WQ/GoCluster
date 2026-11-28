// File role: Strictly sequential 9-band x 24-hour directed VOACAP evaluator
// backed by a resumable JSONL disk cache, reusing production voacap.BuildPathDeck,
// voacap.Runner, and voacap.ParsePredictions without parallelizing shared
// C:\itshfbc\run state.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/internal/voacap"
	"dxcluster/pathreliability"
)

const hfBandCount = 9

var hfBandOrder = [hfBandCount]string{"80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"}

func hfBandIndex(band string) (int, bool) {
	switch strings.ToLower(strings.TrimSpace(band)) {
	case "80m":
		return 0, true
	case "60m":
		return 1, true
	case "40m":
		return 2, true
	case "30m":
		return 3, true
	case "20m":
		return 4, true
	case "17m":
		return 5, true
	case "15m":
		return 6, true
	case "12m":
		return 7, true
	case "10m":
		return 8, true
	default:
		return -1, false
	}
}

func bandForCenterMHz(freqMHz float64) (int, bool) {
	switch {
	case freqMHz >= 3.5 && freqMHz <= 4.0:
		return 0, true
	case freqMHz >= 5.0 && freqMHz <= 5.5:
		return 1, true
	case freqMHz >= 7.0 && freqMHz <= 7.3:
		return 2, true
	case freqMHz >= 10.1 && freqMHz <= 10.15:
		return 3, true
	case freqMHz >= 14.0 && freqMHz <= 14.35:
		return 4, true
	case freqMHz >= 18.068 && freqMHz <= 18.168:
		return 5, true
	case freqMHz >= 21.0 && freqMHz <= 21.45:
		return 6, true
	case freqMHz >= 24.89 && freqMHz <= 24.99:
		return 7, true
	case freqMHz >= 28.0 && freqMHz <= 29.7:
		return 8, true
	default:
		return -1, false
	}
}

type voacapDirectedKey struct {
	TXCell pathreliability.CellID
	RXCell pathreliability.CellID
	Year   int
	Month  int
	SSN    int
}

type voacapUndirectedPairKey struct {
	LoCell pathreliability.CellID
	HiCell pathreliability.CellID
	Year   int
	Month  int
	SSN    int
}

func makeUndirectedPairKey(a, b pathreliability.CellID, year, month, ssn int) voacapUndirectedPairKey {
	if a > b {
		a, b = b, a
	}
	return voacapUndirectedPairKey{LoCell: a, HiCell: b, Year: year, Month: month, SSN: ssn}
}

type voacapHourCell struct {
	FT8SNRDB int     `json:"snr"`
	Rel      float64 `json:"rel"`
	HasRel   bool    `json:"has_rel"`
	Valid    bool    `json:"v"`
}

// voacapDirectedTable holds all 9 HF bands x 24 UTC hours from one Voacapw.exe run.
type voacapDirectedTable struct {
	Cells [hfBandCount][24]voacapHourCell `json:"cells"`
}

type voacapDiskCacheEntry struct {
	TXCell uint32              `json:"tx_cell"`
	RXCell uint32              `json:"rx_cell"`
	Year   int                 `json:"year"`
	Month  int                 `json:"month"`
	SSN    int                 `json:"ssn"`
	Table  voacapDirectedTable `json:"table"`
}

type voacapEvalStats struct {
	CacheHits            int64 `json:"cache_hits"`
	DiskCacheLoaded      int64 `json:"disk_cache_loaded"`
	RunnerCalls          int64 `json:"runner_calls"`
	RunnerErrors         int64 `json:"runner_errors"`
	UniquePairsEvaluated int64 `json:"unique_pairs_evaluated"`
	UniquePairsSkipped   int64 `json:"unique_pairs_skipped_cap"`
	SpotsSkippedCap      int64 `json:"spots_skipped_cap"`
	MaxPairsCap          int   `json:"max_pairs_cap"`
	TruncatedByCap       bool  `json:"truncated_by_cap"`
}

type voacapEvaluator interface {
	Forecast(
		ctx context.Context,
		userCell, dxCell pathreliability.CellID,
		userLat, userLon, dxLat, dxLon float64,
		band string,
		at time.Time,
		ssn int,
		cfg pathreliability.Config,
	) (pathreliability.VOACAPCachedForecast, bool, error)
	Stats() voacapEvalStats
	Close() error
}

// runnerVOACAPEvaluator executes Voacapw.exe strictly sequentially (one shared
// C:\itshfbc\run workspace serialized by internal/voacap.Runner) and caches
// every directed 9-band x 24-hour forecast to disk.
type runnerVOACAPEvaluator struct {
	runner         voacap.Runner
	freqsMHz       []float64
	timeout        time.Duration
	outputPrefix   string
	maxPairs       int
	cache          map[voacapDirectedKey]*voacapDirectedTable
	failedKeys     map[voacapDirectedKey]bool
	admittedPairs  map[voacapUndirectedPairKey]bool
	skippedPairs   map[voacapUndirectedPairKey]bool
	diskFile       *os.File
	diskBuf        *bufio.Writer
	stats          voacapEvalStats
}

func newRunnerVOACAPEvaluator(
	fbCfg pathreliability.VOACAPFallbackConfig,
	cachePath string,
	maxPairs int,
) (*runnerVOACAPEvaluator, error) {
	runner := voacap.NewRunner(fbCfg.VOACAPHome)
	if err := runner.Validate(); err != nil {
		return nil, fmt.Errorf("validate VOACAP runner (%s): %w", fbCfg.VOACAPHome, err)
	}
	timeout := time.Duration(fbCfg.VOACAPTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	prefix := strings.TrimSpace(fbCfg.OutputNamePrefix)
	if prefix == "" {
		prefix = "prop_loro_voacap"
	} else {
		prefix = prefix + "_loro"
	}
	ev := &runnerVOACAPEvaluator{
		runner:        runner,
		freqsMHz:      append([]float64(nil), fbCfg.CenterFrequenciesMHz...),
		timeout:       timeout,
		outputPrefix:  prefix,
		maxPairs:      maxPairs,
		cache:         make(map[voacapDirectedKey]*voacapDirectedTable),
		failedKeys:    make(map[voacapDirectedKey]bool),
		admittedPairs: make(map[voacapUndirectedPairKey]bool),
		skippedPairs:  make(map[voacapUndirectedPairKey]bool),
		stats: voacapEvalStats{
			MaxPairsCap: maxPairs,
		},
	}
	if strings.TrimSpace(cachePath) != "" {
		if err := ev.openDiskCache(cachePath); err != nil {
			return nil, err
		}
	}
	return ev, nil
}

func (e *runnerVOACAPEvaluator) openDiskCache(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create voacap cache dir: %w", err)
	}
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var ent voacapDiskCacheEntry
			if err := json.Unmarshal([]byte(line), &ent); err != nil {
				continue
			}
			k := voacapDirectedKey{
				TXCell: pathreliability.CellID(ent.TXCell),
				RXCell: pathreliability.CellID(ent.RXCell),
				Year:   ent.Year,
				Month:  ent.Month,
				SSN:    ent.SSN,
			}
			tbl := ent.Table
			e.cache[k] = &tbl
			e.stats.DiskCacheLoaded++
		}
		_ = f.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read voacap cache %s: %w", path, err)
	}

	out, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open voacap cache %s for append: %w", path, err)
	}
	e.diskFile = out
	e.diskBuf = bufio.NewWriterSize(out, 64*1024)
	return nil
}

func (e *runnerVOACAPEvaluator) Close() error {
	if e == nil {
		return nil
	}
	var err error
	if e.diskBuf != nil {
		err = errors.Join(err, e.diskBuf.Flush())
		e.diskBuf = nil
	}
	if e.diskFile != nil {
		err = errors.Join(err, e.diskFile.Close())
		e.diskFile = nil
	}
	return err
}

func (e *runnerVOACAPEvaluator) Stats() voacapEvalStats {
	s := e.stats
	s.UniquePairsEvaluated = int64(len(e.admittedPairs))
	s.UniquePairsSkipped = int64(len(e.skippedPairs))
	s.TruncatedByCap = s.UniquePairsSkipped > 0
	return s
}

func (e *runnerVOACAPEvaluator) Forecast(
	ctx context.Context,
	userCell, dxCell pathreliability.CellID,
	userLat, userLon, dxLat, dxLon float64,
	band string,
	at time.Time,
	ssn int,
	cfg pathreliability.Config,
) (pathreliability.VOACAPCachedForecast, bool, error) {
	bandIdx, ok := hfBandIndex(band)
	if !ok {
		return pathreliability.VOACAPCachedForecast{}, false, nil
	}
	if ssn <= 0 {
		return pathreliability.VOACAPCachedForecast{}, false, nil
	}
	utc := at.UTC()
	year, month, hour := utc.Year(), int(utc.Month()), utc.Hour()

	pairKey := makeUndirectedPairKey(userCell, dxCell, year, month, ssn)
	rxKey := voacapDirectedKey{TXCell: dxCell, RXCell: userCell, Year: year, Month: month, SSN: ssn}
	txKey := voacapDirectedKey{TXCell: userCell, RXCell: dxCell, Year: year, Month: month, SSN: ssn}

	if !e.admittedPairs[pairKey] {
		_, rxCached := e.cache[rxKey]
		_, txCached := e.cache[txKey]
		if e.maxPairs > 0 && (!rxCached || !txCached) && len(e.admittedPairs) >= e.maxPairs {
			e.skippedPairs[pairKey] = true
			e.stats.SpotsSkippedCap++
			return pathreliability.VOACAPCachedForecast{}, false, nil
		}
		e.admittedPairs[pairKey] = true
	}

	rxTable, err := e.getOrRunDirected(ctx, rxKey, dxLat, dxLon, userLat, userLon)
	if err != nil || rxTable == nil {
		return pathreliability.VOACAPCachedForecast{}, false, err
	}
	txTable, err := e.getOrRunDirected(ctx, txKey, userLat, userLon, dxLat, dxLon)
	if err != nil || txTable == nil {
		return pathreliability.VOACAPCachedForecast{}, false, err
	}

	rxCell := rxTable.Cells[bandIdx][hour]
	txCell := txTable.Cells[bandIdx][hour]
	if !rxCell.Valid || !txCell.Valid {
		return pathreliability.VOACAPCachedForecast{}, false, nil
	}

	return buildVOACAPCachedForecast(rxCell, txCell, hour, e.freqForBand(bandIdx), ssn, cfg), true, nil
}

var errVOACAPCachedFailure = errors.New("voacap directed key previously failed")

func (e *runnerVOACAPEvaluator) getOrRunDirected(
	ctx context.Context,
	key voacapDirectedKey,
	txLat, txLon, rxLat, rxLon float64,
) (*voacapDirectedTable, error) {
	if tbl, ok := e.cache[key]; ok {
		e.stats.CacheHits++
		return tbl, nil
	}
	if e.failedKeys[key] {
		return nil, errVOACAPCachedFailure
	}

	txEP := voacap.DeckEndpoint{Label: "TRANSMITTER", Latitude: txLat, Longitude: txLon}
	rxEP := voacap.DeckEndpoint{Label: "RECEIVER", Latitude: rxLat, Longitude: rxLon}
	method, _, err := voacap.RecommendedPathMethod(txEP, rxEP)
	if err != nil {
		e.failedKeys[key] = true
		e.stats.RunnerErrors++
		return nil, err
	}
	monthStart := time.Date(key.Year, time.Month(key.Month), 1, 0, 0, 0, 0, time.UTC)
	deck, err := voacap.BuildPathDeck(voacap.PathDeckRequest{
		Comment:              fmt.Sprintf("GoCluster prop_loro %d->%d SSN=%d", key.TXCell, key.RXCell, key.SSN),
		Transmit:             txEP,
		Receive:              rxEP,
		SSN:                  key.SSN,
		Now:                  monthStart,
		ForecastHours:        24,
		StartVOACAPHour:      1,
		CenterFrequenciesMHz: e.freqsMHz,
		Method:               method,
	})
	if err != nil {
		e.failedKeys[key] = true
		e.stats.RunnerErrors++
		return nil, err
	}
	outputName := fmt.Sprintf("%s_%04d%02d_%d_%d_%d.out",
		e.outputPrefix, key.Year, key.Month, key.TXCell, key.RXCell, key.SSN)
	res, err := e.runner.Run(ctx, voacap.RunRequest{
		Deck:                  deck,
		OutputName:            outputName,
		Timeout:               e.timeout,
		RemoveOutputAfterRead: true,
	})
	e.stats.RunnerCalls++
	if err != nil {
		e.failedKeys[key] = true
		e.stats.RunnerErrors++
		return nil, err
	}
	records, err := voacap.ParsePredictions(res.Output)
	if err != nil {
		e.failedKeys[key] = true
		e.stats.RunnerErrors++
		return nil, err
	}
	tbl := buildDirectedTable(records)
	e.cache[key] = tbl
	e.appendDiskCache(key, tbl)
	return tbl, nil
}

func (e *runnerVOACAPEvaluator) appendDiskCache(key voacapDirectedKey, tbl *voacapDirectedTable) {
	if e.diskBuf == nil || tbl == nil {
		return
	}
	line, err := json.Marshal(voacapDiskCacheEntry{
		TXCell: uint32(key.TXCell),
		RXCell: uint32(key.RXCell),
		Year:   key.Year,
		Month:  key.Month,
		SSN:    key.SSN,
		Table:  *tbl,
	})
	if err != nil {
		return
	}
	line = append(line, '\n')
	if _, err := e.diskBuf.Write(line); err != nil {
		return
	}
	if e.stats.RunnerCalls%50 == 0 {
		_ = e.diskBuf.Flush()
	}
}

func (e *runnerVOACAPEvaluator) freqForBand(bandIdx int) float64 {
	for _, f := range e.freqsMHz {
		if idx, ok := bandForCenterMHz(f); ok && idx == bandIdx {
			return f
		}
	}
	return 0
}

func buildDirectedTable(records []voacap.PredictionRecord) *voacapDirectedTable {
	tbl := &voacapDirectedTable{}
	for _, rec := range records {
		bIdx, ok := bandForCenterMHz(rec.FrequencyMHz)
		if !ok || rec.HourUTC < 0 || rec.HourUTC >= 24 {
			continue
		}
		cur := tbl.Cells[bIdx][rec.HourUTC]
		if !cur.Valid || rec.FT8SNRDB > cur.FT8SNRDB {
			tbl.Cells[bIdx][rec.HourUTC] = voacapHourCell{
				FT8SNRDB: rec.FT8SNRDB,
				Rel:      rec.Reliability,
				HasRel:   rec.HasReliability,
				Valid:    true,
			}
		}
	}
	return tbl
}

// buildVOACAPCachedForecast reproduces pathreliability's exact bidirectional
// receive/transmit merge contract (voacap_forecaster.go + voacap_fallback.go)
// at ReceiveNoisePenaltyDB=0.
func buildVOACAPCachedForecast(
	rxCell, txCell voacapHourCell,
	hourUTC int,
	freqMHz float64,
	ssn int,
	cfg pathreliability.Config,
) pathreliability.VOACAPCachedForecast {
	rxDB := float64(rxCell.FT8SNRDB)
	txDB := float64(txCell.FT8SNRDB)
	effDB := cfg.MergeReceiveWeight*rxDB + cfg.MergeTransmitWeight*txDB
	hasRel := rxCell.HasRel && txCell.HasRel
	minRel := math.Min(rxCell.Rel, txCell.Rel)
	return pathreliability.VOACAPCachedForecast{
		Record: pathreliability.VOACAPHourlyForecast{
			FT8SNRDB:                        int(math.Round(effDB)),
			HourUTC:                         hourUTC,
			FrequencyMHz:                    freqMHz,
			ReqSNRReliability:               minRel,
			HasReqSNRReliability:            hasRel,
			ReceiveFT8SNRDB:                 rxCell.FT8SNRDB,
			TransmitFT8SNRDB:                txCell.FT8SNRDB,
			ReceiveReqSNRReliability:        rxCell.Rel,
			TransmitReqSNRReliability:       txCell.Rel,
			HasDirectionalReqSNRReliability: hasRel,
			HasDirectionalSNR:               true,
		},
		EffectiveFT8SNRDB:     effDB,
		HasEffectiveFT8SNRDB:  true,
		ExactReceiveFT8SNRDB:  rxDB,
		ExactTransmitFT8SNRDB: txDB,
		HasExactDirectionalDB: true,
		ReceiveNoisePenaltyDB: 0,
		SSN:                   ssn,
		PropPrior:             false,
	}
}
