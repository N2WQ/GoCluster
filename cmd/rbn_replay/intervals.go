package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"dxcluster/spot"
)

type resolverSample struct {
	TS      time.Time
	Metrics spot.SignalResolverMetrics
}

type intervalRow struct {
	T0 string
	T1 string

	Minutes float64

	DStateConfident int64
	DStateProbable  int64
	DStateUncertain int64
	DStateSplit     int64
	DQ              int64
	DK              int64
	DCcap           int64
	DR              int64
	DPC             int64
	DPR             int64
	DEC             int64
	DER             int64

	DEPTH           int
	QueueDepthRatio float64
	SplitSharePct   float64
}

type gatesSummary struct {
	Overall struct {
		MaxQueueDepth     int                    `json:"max_queue_depth"`
		TotalQueueDrops   int64                  `json:"total_queue_drops"`
		TotalCapPressureC int64                  `json:"total_cap_pressure_c"`
		TotalCapPressureR int64                  `json:"total_cap_pressure_r"`
		TotalEvictionsC   int64                  `json:"total_evictions_c"`
		TotalEvictionsR   int64                  `json:"total_evictions_r"`
		StateConfident    uint64                 `json:"state_confident"`
		StateProbable     uint64                 `json:"state_probable"`
		StateUncertain    uint64                 `json:"state_uncertain"`
		StateSplit        uint64                 `json:"state_split"`
		Stability         replayStabilitySummary `json:"stability"`
		ABMetrics         replayABMetrics        `json:"ab_metrics"`
	} `json:"overall"`

	ThresholdHits struct {
		Count int `json:"count"`
	} `json:"threshold_hits"`
}

func computeIntervalsAndGates(samples []resolverSample, queueSize int) (intervals []intervalRow, hits []intervalRow, gates gatesSummary) {
	if len(samples) < 2 {
		return nil, nil, gates
	}
	intervals = make([]intervalRow, 0, len(samples)-1)
	maxDepth := 0
	var (
		totalQ  int64
		totalPC int64
		totalPR int64
		totalEC int64
		totalER int64
	)

	for i := 1; i < len(samples); i++ {
		a := samples[i-1]
		b := samples[i]

		dStateConfident := int64(b.Metrics.StateConfident) - int64(a.Metrics.StateConfident)
		dStateProbable := int64(b.Metrics.StateProbable) - int64(a.Metrics.StateProbable)
		dStateUncertain := int64(b.Metrics.StateUncertain) - int64(a.Metrics.StateUncertain)
		dStateSplit := int64(b.Metrics.StateSplit) - int64(a.Metrics.StateSplit)
		dQ := int64(b.Metrics.DropQueueFull) - int64(a.Metrics.DropQueueFull)
		dK := int64(b.Metrics.DropMaxKeys) - int64(a.Metrics.DropMaxKeys)
		dCcap := int64(b.Metrics.DropMaxCandidates) - int64(a.Metrics.DropMaxCandidates)
		dR := int64(b.Metrics.DropMaxReporters) - int64(a.Metrics.DropMaxReporters)
		dPC := int64(b.Metrics.CapPressureCandidates) - int64(a.Metrics.CapPressureCandidates)
		dPR := int64(b.Metrics.CapPressureReporters) - int64(a.Metrics.CapPressureReporters)
		dEC := int64(b.Metrics.EvictedCandidates) - int64(a.Metrics.EvictedCandidates)
		dER := int64(b.Metrics.EvictedReporters) - int64(a.Metrics.EvictedReporters)

		queueRatio := 0.0
		if queueSize > 0 {
			queueRatio = float64(b.Metrics.QueueDepth) / float64(queueSize)
		}

		totalStates := int64(b.Metrics.StateConfident + b.Metrics.StateProbable + b.Metrics.StateUncertain + b.Metrics.StateSplit)
		splitSharePct := 0.0
		if totalStates > 0 {
			splitSharePct = (100.0 * float64(b.Metrics.StateSplit)) / float64(totalStates)
		}

		row := intervalRow{
			T0:              a.TS.UTC().Format("2006-01-02 15:04:05"),
			T1:              b.TS.UTC().Format("2006-01-02 15:04:05"),
			Minutes:         b.TS.Sub(a.TS).Minutes(),
			DStateConfident: dStateConfident,
			DStateProbable:  dStateProbable,
			DStateUncertain: dStateUncertain,
			DStateSplit:     dStateSplit,
			DQ:              dQ,
			DK:              dK,
			DCcap:           dCcap,
			DR:              dR,
			DPC:             dPC,
			DPR:             dPR,
			DEC:             dEC,
			DER:             dER,
			DEPTH:           b.Metrics.QueueDepth,
			QueueDepthRatio: queueRatio,
			SplitSharePct:   splitSharePct,
		}
		intervals = append(intervals, row)

		if b.Metrics.QueueDepth > maxDepth {
			maxDepth = b.Metrics.QueueDepth
		}

		totalQ += dQ
		totalPC += dPC
		totalPR += dPR
		totalEC += dEC
		totalER += dER
	}

	gates.Overall.MaxQueueDepth = maxDepth
	gates.Overall.TotalQueueDrops = totalQ
	gates.Overall.TotalCapPressureC = totalPC
	gates.Overall.TotalCapPressureR = totalPR
	gates.Overall.TotalEvictionsC = totalEC
	gates.Overall.TotalEvictionsR = totalER
	last := samples[len(samples)-1].Metrics
	gates.Overall.StateConfident = last.StateConfident
	gates.Overall.StateProbable = last.StateProbable
	gates.Overall.StateUncertain = last.StateUncertain
	gates.Overall.StateSplit = last.StateSplit

	hits = make([]intervalRow, 0)
	for _, r := range intervals {
		if r.QueueDepthRatio >= 0.25 ||
			r.DQ >= 1 || r.DK >= 1 || r.DCcap >= 1 || r.DR >= 1 ||
			r.DPC >= 1 || r.DPR >= 1 {
			hits = append(hits, r)
		}
	}
	gates.ThresholdHits.Count = len(hits)
	return intervals, hits, gates
}

func roundFloat(v float64, places int) float64 {
	if places <= 0 {
		return math.Round(v)
	}
	factor := math.Pow(10, float64(places))
	return math.Round(v*factor) / factor
}

func writeIntervalsCSV(path string, rows []intervalRow) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write([]string{
		"T0", "T1", "Minutes",
		"dStateConfident", "dStateProbable", "dStateUncertain", "dStateSplit",
		"dQ", "dK", "dCcap", "dR",
		"dPC", "dPR", "dEC", "dER",
		"DEPTH", "QueueDepthRatio", "SplitSharePct",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.T0,
			r.T1,
			fmt.Sprintf("%.3f", r.Minutes),
			strconv.FormatInt(r.DStateConfident, 10),
			strconv.FormatInt(r.DStateProbable, 10),
			strconv.FormatInt(r.DStateUncertain, 10),
			strconv.FormatInt(r.DStateSplit, 10),
			strconv.FormatInt(r.DQ, 10),
			strconv.FormatInt(r.DK, 10),
			strconv.FormatInt(r.DCcap, 10),
			strconv.FormatInt(r.DR, 10),
			strconv.FormatInt(r.DPC, 10),
			strconv.FormatInt(r.DPR, 10),
			strconv.FormatInt(r.DEC, 10),
			strconv.FormatInt(r.DER, 10),
			strconv.Itoa(r.DEPTH),
			fmt.Sprintf("%.6f", r.QueueDepthRatio),
			fmt.Sprintf("%.3f", r.SplitSharePct),
		}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return f.Sync()
}
