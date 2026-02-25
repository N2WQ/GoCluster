package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
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

	DA    int64
	DCMP  int64
	DSP   int64
	DDW   int64
	DUC   int64
	DQ    int64
	DK    int64
	DCcap int64
	DR    int64
	DPC   int64
	DPR   int64
	DEC   int64
	DER   int64

	AgreementPct string
	DWPct        string
	SPPct        string
	UCPct        string

	DEPTH           int
	QueueDepthRatio float64
	SplitSharePct   float64
	SampleGuardPass bool
}

type gatesSummary struct {
	Overall struct {
		ComparableDecisions uint64                   `json:"comparable_decisions"`
		AgreementPct        float64                  `json:"agreement_pct"`
		DWPct               float64                  `json:"dw_pct"`
		SPPct               float64                  `json:"sp_pct"`
		UCPct               float64                  `json:"uc_pct"`
		MaxQueueDepth       int                      `json:"max_queue_depth"`
		TotalQueueDrops     int64                    `json:"total_queue_drops"`
		TotalCapPressureC   int64                    `json:"total_cap_pressure_c"`
		TotalCapPressureR   int64                    `json:"total_cap_pressure_r"`
		TotalEvictionsC     int64                    `json:"total_evictions_c"`
		TotalEvictionsR     int64                    `json:"total_evictions_r"`
		Stability           replayStabilitySummary   `json:"stability"`
		MethodStability     replayMethodStabilitySet `json:"method_stability"`
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
		totalCMP int64
		totalA   int64
		totalDW  int64
		totalSP  int64
		totalUC  int64
		totalQ   int64
		totalPC  int64
		totalPR  int64
		totalEC  int64
		totalER  int64
	)

	for i := 1; i < len(samples); i++ {
		a := samples[i-1]
		b := samples[i]

		dCMP := int64(b.Metrics.DecisionsComparable) - int64(a.Metrics.DecisionsComparable)
		dA := int64(b.Metrics.DecisionAgreement) - int64(a.Metrics.DecisionAgreement)
		dSP := int64(b.Metrics.DisagreeSplitCorrected) - int64(a.Metrics.DisagreeSplitCorrected)
		dDW := int64(b.Metrics.DisagreeConfidentDifferentWinner) - int64(a.Metrics.DisagreeConfidentDifferentWinner)
		dUC := int64(b.Metrics.DisagreeUncertainCorrected) - int64(a.Metrics.DisagreeUncertainCorrected)
		dQ := int64(b.Metrics.DropQueueFull) - int64(a.Metrics.DropQueueFull)
		dK := int64(b.Metrics.DropMaxKeys) - int64(a.Metrics.DropMaxKeys)
		dCcap := int64(b.Metrics.DropMaxCandidates) - int64(a.Metrics.DropMaxCandidates)
		dR := int64(b.Metrics.DropMaxReporters) - int64(a.Metrics.DropMaxReporters)
		dPC := int64(b.Metrics.CapPressureCandidates) - int64(a.Metrics.CapPressureCandidates)
		dPR := int64(b.Metrics.CapPressureReporters) - int64(a.Metrics.CapPressureReporters)
		dEC := int64(b.Metrics.EvictedCandidates) - int64(a.Metrics.EvictedCandidates)
		dER := int64(b.Metrics.EvictedReporters) - int64(a.Metrics.EvictedReporters)

		agreementPct := ""
		dwPct := ""
		spPct := ""
		ucPct := ""
		if dCMP > 0 {
			agreementPct = fmt.Sprintf("%.3f", (100.0*float64(dA))/float64(dCMP))
			dwPct = fmt.Sprintf("%.3f", (100.0*float64(dDW))/float64(dCMP))
			spPct = fmt.Sprintf("%.3f", (100.0*float64(dSP))/float64(dCMP))
			ucPct = fmt.Sprintf("%.3f", (100.0*float64(dUC))/float64(dCMP))
		}

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
			DA:              dA,
			DCMP:            dCMP,
			DSP:             dSP,
			DDW:             dDW,
			DUC:             dUC,
			DQ:              dQ,
			DK:              dK,
			DCcap:           dCcap,
			DR:              dR,
			DPC:             dPC,
			DPR:             dPR,
			DEC:             dEC,
			DER:             dER,
			AgreementPct:    agreementPct,
			DWPct:           dwPct,
			SPPct:           spPct,
			UCPct:           ucPct,
			DEPTH:           b.Metrics.QueueDepth,
			QueueDepthRatio: queueRatio,
			SplitSharePct:   splitSharePct,
			SampleGuardPass: dCMP >= 200,
		}
		intervals = append(intervals, row)

		if b.Metrics.QueueDepth > maxDepth {
			maxDepth = b.Metrics.QueueDepth
		}

		totalCMP += dCMP
		totalA += dA
		totalDW += dDW
		totalSP += dSP
		totalUC += dUC
		totalQ += dQ
		totalPC += dPC
		totalPR += dPR
		totalEC += dEC
		totalER += dER
	}

	overallAgreement := 0.0
	overallDW := 0.0
	overallSP := 0.0
	overallUC := 0.0
	if totalCMP > 0 {
		overallAgreement = (100.0 * float64(totalA)) / float64(totalCMP)
		overallDW = (100.0 * float64(totalDW)) / float64(totalCMP)
		overallSP = (100.0 * float64(totalSP)) / float64(totalCMP)
		overallUC = (100.0 * float64(totalUC)) / float64(totalCMP)
	}
	gates.Overall.ComparableDecisions = uint64(totalCMP)
	gates.Overall.AgreementPct = roundFloat(overallAgreement, 3)
	gates.Overall.DWPct = roundFloat(overallDW, 3)
	gates.Overall.SPPct = roundFloat(overallSP, 3)
	gates.Overall.UCPct = roundFloat(overallUC, 3)
	gates.Overall.MaxQueueDepth = maxDepth
	gates.Overall.TotalQueueDrops = totalQ
	gates.Overall.TotalCapPressureC = totalPC
	gates.Overall.TotalCapPressureR = totalPR
	gates.Overall.TotalEvictionsC = totalEC
	gates.Overall.TotalEvictionsR = totalER

	hits = make([]intervalRow, 0)
	for _, r := range intervals {
		if !r.SampleGuardPass {
			continue
		}
		agreementPct := parseFloatOrZero(r.AgreementPct)
		dwPct := parseFloatOrZero(r.DWPct)
		spPct := parseFloatOrZero(r.SPPct)
		ucPct := parseFloatOrZero(r.UCPct)

		if agreementPct < 98.5 ||
			dwPct > 0.5 ||
			spPct > 1.0 ||
			ucPct > 2.0 ||
			r.QueueDepthRatio >= 0.25 ||
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

func parseFloatOrZero(s string) float64 {
	if strings.TrimSpace(s) == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
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
		"dA", "dCMP", "dSP", "dDW", "dUC",
		"dQ", "dK", "dCcap", "dR",
		"dPC", "dPR", "dEC", "dER",
		"AgreementPct", "DWPct", "SPPct", "UCPct",
		"DEPTH", "QueueDepthRatio", "SplitSharePct",
		"SampleGuardPass",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.T0,
			r.T1,
			fmt.Sprintf("%.3f", r.Minutes),
			strconv.FormatInt(r.DA, 10),
			strconv.FormatInt(r.DCMP, 10),
			strconv.FormatInt(r.DSP, 10),
			strconv.FormatInt(r.DDW, 10),
			strconv.FormatInt(r.DUC, 10),
			strconv.FormatInt(r.DQ, 10),
			strconv.FormatInt(r.DK, 10),
			strconv.FormatInt(r.DCcap, 10),
			strconv.FormatInt(r.DR, 10),
			strconv.FormatInt(r.DPC, 10),
			strconv.FormatInt(r.DPR, 10),
			strconv.FormatInt(r.DEC, 10),
			strconv.FormatInt(r.DER, 10),
			r.AgreementPct,
			r.DWPct,
			r.SPPct,
			r.UCPct,
			strconv.Itoa(r.DEPTH),
			fmt.Sprintf("%.6f", r.QueueDepthRatio),
			fmt.Sprintf("%.3f", r.SplitSharePct),
			strconv.FormatBool(r.SampleGuardPass),
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
