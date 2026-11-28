// File role: Writes structured scorecard.json and human-readable scorecard.md
// with prominent pair-cap truncation banners, head-to-head deltas, and explicit
// scientific interpretation caveats.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func writeScorecardArtifacts(outDir string, sc loroScorecard) (string, string, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", fmt.Errorf("create output dir %q: %w", outDir, err)
	}
	jsonPath := filepath.Join(outDir, "scorecard.json")
	mdPath := filepath.Join(outDir, "scorecard.md")

	rawJSON, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("marshal scorecard.json: %w", err)
	}
	rawJSON = append(rawJSON, '\n')
	if err := os.WriteFile(jsonPath, rawJSON, 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", jsonPath, err)
	}

	md := renderScorecardMarkdown(sc)
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", mdPath, err)
	}
	return jsonPath, mdPath, nil
}

func renderScorecardMarkdown(sc loroScorecard) string {
	var b bytes.Buffer
	m := sc.Manifest
	vs := m.VOACAPStats

	b.WriteString("# Head-to-Head LORO Scorecard: `v4.3 propprior` vs. `VOACAP`\n\n")

	if vs.MaxPairsCap > 0 && vs.TruncatedByCap {
		fmt.Fprintf(&b, "> **WARNING — PAIR CAP TRUNCATION ACTIVE (`-max-voacap-pairs=%d`):** "+
			"VOACAP evaluation hit the pair cap after **%d unique cell pairs**, skipping **%d additional unique pairs (%d held-out spots)**.\n"+
			"> All comparisons below are strictly matched (`both_ready == true`) on the non-truncated subset, "+
			"**not** the full capture. Re-run with `-max-voacap-pairs=0` for the full uncapped weekend scorecard.\n\n",
			vs.MaxPairsCap, vs.UniquePairsEvaluated, vs.UniquePairsSkipped, vs.SpotsSkippedCap)
	} else if vs.MaxPairsCap > 0 {
		fmt.Fprintf(&b, "> **Note:** Run executed with `-max-voacap-pairs=%d` (cap was not reached; %d unique pairs evaluated, 0 skipped).\n\n",
			vs.MaxPairsCap, vs.UniquePairsEvaluated)
	}

	b.WriteString("## Run Manifest\n\n")
	fmt.Fprintf(&b, "- **Capture window (UTC):** `%s` -> `%s` (%d spot files, %d space-weather files)\n",
		m.FirstObservedUTC, m.LastObservedUTC, len(m.SpotFiles), len(m.SpaceWeatherFiles))
	fmt.Fprintf(&b, "- **LORO split:** `holdout_unit=%s`, `k=%d`, `fold=%d`, `warmup=%ds`, `exclude_cold_sw=%t`\n",
		m.HoldoutUnit, m.Folds, m.Fold, m.WarmupSeconds, m.ExcludeColdSW)
	fmt.Fprintf(&b, "- **Spot counts:** read=`%d`, train_updates=`%d`, warmup_skipped=`%d`, cold_sw_skipped=`%d`, held_out_hf_matched=`%d`, unmatched=`%d`\n",
		m.TotalRowsRead, m.TrainUpdates, m.WarmupSkipped, m.ColdSWSkipped, m.HeldOutHFMatched, m.HeldOutUnmatched)
	fmt.Fprintf(&b, "- **Models:** `internal/propprior` CRC32 `%s` vs. `VOACAP` (runner_calls=`%d`, cache_hits=`%d`, disk_loaded=`%d`, errors=`%d`, unique_pairs=`%d`, elapsed=`%.1fs`)\n\n",
		m.PropPriorCRC32, vs.RunnerCalls, vs.CacheHits, vs.DiskCacheLoaded, vs.RunnerErrors, vs.UniquePairsEvaluated, m.ElapsedSeconds)

	b.WriteString("## 1. Overall & Prior-Only / Fallback-Hit Breakdown\n\n")
	writeH2HTable(&b, []namedH2H{
		{"Regime A: Pure Prior (All HF 80m-10m)", sc.RegimeAOverall},
		{"Regime A: Exact Grids Only", sc.RegimeAExactGrid},
		{"Regime A: CTY-Derived Grids", sc.RegimeADerivedGrid},
		{"Regime A: Path-Hour Empirical p50 (n>=3)", sc.RegimeAPathHourP50},
		{"Regime B: Fallback-Hit (All Insufficient)", sc.RegimeBFallbackAll},
		{"Regime B: Fallback-Hit (No p50 / Prior-Only)", sc.RegimeBFallbackNoP50},
		{"Regime B: Fallback-Hit (Sparse p50 1..20)", sc.RegimeBFallbackSpP50},
		{"Regime B: Live-Dominated (SourceCombined)", sc.RegimeBCombined},
	})

	fmt.Fprintf(&b, "**Regime B Fallback Stage Outcomes (on `%d` insufficient held-out HF spots):**\n", sc.RegimeBPPOutcomes.Total)
	fmt.Fprintf(&b, "- **`v4.3 propprior`:** open=`%d`, closed=`%d`, aligned=`%d`, sparse_upgrade=`%d`, still_insufficient=`%d`\n",
		sc.RegimeBPPOutcomes.Open, sc.RegimeBPPOutcomes.Closed, sc.RegimeBPPOutcomes.Aligned, sc.RegimeBPPOutcomes.SparseUpgrade, sc.RegimeBPPOutcomes.Insufficient)
	fmt.Fprintf(&b, "- **`VOACAP`:** open=`%d`, closed=`%d`, aligned=`%d`, sparse_upgrade=`%d`, still_insufficient=`%d`\n\n",
		sc.RegimeBVOOutcomes.Open, sc.RegimeBVOOutcomes.Closed, sc.RegimeBVOOutcomes.Aligned, sc.RegimeBVOOutcomes.SparseUpgrade, sc.RegimeBVOOutcomes.Insufficient)

	b.WriteString("## 2. Per-Mode Residual Bias (`T_mode` Offset Check)\n\n")
	b.WriteString("*`obs_ft8 - pred_rx` is residual bias under current `mode_offsets`; `raw_snr - pred_rx` is the implied empirical offset relative to each model's 100W FT8-equivalent scale.*\n\n")
	writeModeTable(&b, "Regime A (Pure Prior — All Paths)", sc.ByMode)
	if len(sc.ByModeNonPolar) > 0 {
		writeModeTable(&b, "Regime A (Non-Polar Only, max |geomag| < 60 deg)", sc.ByModeNonPolar)
	}
	if len(sc.ByModeNonPolarNAEU) > 0 {
		writeModeTable(&b, "Regime A (Non-Polar NA <-> EU — Clean Station-Class Offset)", sc.ByModeNonPolarNAEU)
	}
	if len(sc.ByModeFallbackOnly) > 0 {
		writeModeTable(&b, "Regime B (Fallback-Hit Only)", sc.ByModeFallbackOnly)
	}

	b.WriteString("## 3. Per-Band (`80m`-`10m` + `160m`) & Day/Night\n\n")
	var bandRows []namedH2H
	for _, band := range hfBandOrder {
		if s, ok := sc.ByBand[band]; ok && s.MatchedCount > 0 {
			bandRows = append(bandRows, namedH2H{band + " (all)", s})
		}
		for _, sol := range []string{"day", "twilight", "night"} {
			key := band + "_" + sol
			if s, ok := sc.ByBandDayNight[key]; ok && s.MatchedCount > 0 {
				bandRows = append(bandRows, namedH2H{band + " " + sol, s})
			}
		}
	}
	writeH2HTable(&b, bandRows)

	fmt.Fprintf(&b, "**160m (`native_160m_fallback`, excluded from HF skywave priors):** held_out=`%d`, live_combined=`%d`, native160_low=`%d`, native160_unlikely=`%d`, native160_closed(false_closed)=`%d`, insufficient=`%d`\n\n",
		sc.Native160m.TotalHeldOut, sc.Native160m.LiveCombined, sc.Native160m.EmittedLow,
		sc.Native160m.EmittedUnlk, sc.Native160m.EmittedClose, sc.Native160m.Insufficient)

	b.WriteString("## 4. Long-Haul & Polar DX (`d >= 6,000 km`, `20m` Polar, `20m` NA<->JA)\n\n")
	dxKeys := []struct {
		key   string
		label string
	}{
		{"dist_ge_6000km", "All HF, d >= 6,000 km"},
		{"20m_dist_ge_6000km", "20m, d >= 6,000 km"},
		{"polar_all", "All HF, Polar (max |geomag| >= 60 deg)"},
		{"20m_polar", "20m, Polar (max |geomag| >= 60 deg)"},
		{"20m_na_ja", "20m, NA <-> JA Corridor"},
	}
	var dxRows []namedH2H
	for _, item := range dxKeys {
		if s, ok := sc.LongHaulPolar[item.key]; ok && s.MatchedCount > 0 {
			dxRows = append(dxRows, namedH2H{item.label + " (Pure Prior)", s})
		}
		if s, ok := sc.LongHaulPolarFBOnly[item.key]; ok && s.MatchedCount > 0 {
			dxRows = append(dxRows, namedH2H{item.label + " (Fallback-Hit)", s})
		}
	}
	writeH2HTable(&b, dxRows)

	b.WriteString("### Geomagnetic Latitude Cliff & North-NA (VE3/NE) vs. Mid-NA (W3) <-> EEU\n\n")
	cliffItems := []struct {
		key   string
		label string
	}{
		{"geomag_lt_50", "max |geomag| < 50 deg"},
		{"geomag_50_55", "max |geomag| 50-55 deg"},
		{"geomag_55_60", "max |geomag| 55-60 deg"},
		{"geomag_60_65", "max |geomag| 60-65 deg"},
		{"geomag_65_70", "max |geomag| 65-70 deg"},
		{"geomag_ge_70", "max |geomag| >= 70 deg"},
		{"north_na_eeu", "North-NA (VE3/NE, lat>=42N) <-> EEU"},
		{"mid_na_eeu", "Mid-NA (W3, 34-42N) <-> EEU"},
	}
	var cliffRows []namedH2H
	for _, item := range cliffItems {
		if s, ok := sc.GeomagCliff[item.key]; ok && s.MatchedCount > 0 {
			cliffRows = append(cliffRows, namedH2H{item.label + " (All HF)", s})
		}
		if s, ok := sc.GeomagCliff20m[item.key]; ok && s.MatchedCount > 0 {
			cliffRows = append(cliffRows, namedH2H{item.label + " (20m)", s})
		}
	}
	writeH2HTable(&b, cliffRows)

	b.WriteString("## 5. `Rel` Discrimination & Active-Pair False-Open Calibration\n\n")
	b.WriteString("| Model | Positives | Negatives (Active Non-Heard) | ROC-AUC | Brier | Mean Rel (Pos) | Mean Rel (Neg) | False-Open (`Rel>=0.50`) | False-Open (`Rel>=0.70`) |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	fmt.Fprintf(&b, "| `v4.3 propprior` | %d | %d | %.4f | %.4f | %.4f | %.4f | %.2f%% | %.2f%% |\n",
		sc.RelCalibrationPP.Positives, sc.RelCalibrationPP.Negatives, sc.RelCalibrationPP.ROCAUC, sc.RelCalibrationPP.BrierScore,
		sc.RelCalibrationPP.MeanRelPos, sc.RelCalibrationPP.MeanRelNeg, 100*sc.RelCalibrationPP.FalseOpenGE50, 100*sc.RelCalibrationPP.FalseOpenGE70)
	fmt.Fprintf(&b, "| `VOACAP` | %d | %d | %.4f | %.4f | %.4f | %.4f | %.2f%% | %.2f%% |\n\n",
		sc.RelCalibrationVO.Positives, sc.RelCalibrationVO.Negatives, sc.RelCalibrationVO.ROCAUC, sc.RelCalibrationVO.BrierScore,
		sc.RelCalibrationVO.MeanRelPos, sc.RelCalibrationVO.MeanRelNeg, 100*sc.RelCalibrationVO.FalseOpenGE50, 100*sc.RelCalibrationVO.FalseOpenGE70)

	writeRelBySliceTable(&b, "Per-Band Active-Pair `Rel` Calibration (`80m`-`10m`)", hfBandOrder[:], sc.RelCalibrationByBandPP, sc.RelCalibrationByBandVO)
	writeRelBySliceTable(&b, "High-Band N-S (`NA <-> SA/CA`: `15m`/`12m`/`10m`) Active-Pair `Rel` Calibration", highBandOrder, sc.RelCalibrationNASACAPP, sc.RelCalibrationNASACAVO)

	var naSACARows []namedH2H
	for _, band := range highBandOrder {
		if s, ok := sc.NASACAHighBand[band]; ok && s.MatchedCount > 0 {
			naSACARows = append(naSACARows, namedH2H{band + " NA <-> SA/CA (Pure Prior)", s})
		}
	}
	if len(naSACARows) > 0 {
		b.WriteString("### High-Band N-S (`NA <-> SA/CA`) Matched-Spot Accuracy\n\n")
		b.WriteString("*Note: `12m` is a WARC band (no CQ WW CW traffic) and reflects casual FT8/non-contest volume rather than contest CW volume.*\n\n")
		writeH2HTable(&b, naSACARows)
	}

	b.WriteString("## Scientific Caveats\n\n")
	b.WriteString("- Decoded spots are left-censored at each mode's decode floor; positive-only residuals reflect decoded paths and station TX/antenna power relative to both models' 100W isotropic reference scale.\n")
	b.WriteString("- Active-TX x active-RX non-heard pairs provide a lower bound on ionospheric opening (antenna directionality and QRM can cause non-decodes on open paths); use ROC-AUC, Brier, and false-open rates for head-to-head ranking rather than absolute isotropic REL equality.\n")

	return b.String()
}

type namedH2H struct {
	label string
	s     headToHeadSummary
}

func writeH2HTable(b *bytes.Buffer, rows []namedH2H) {
	if len(rows) == 0 {
		b.WriteString("*(No matched samples in this slice.)*\n\n")
		return
	}
	b.WriteString("| Slice | Matched N | v4.3 MAE | VOACAP MAE | dMAE (v4.3-VO) | v4.3 Bias | VOACAP Bias | v4.3 FalseClosed | VOACAP FalseClosed | v4.3 +-1 Tier | VOACAP +-1 Tier | v4.3 |z|<=1 / <=2 |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for i := range rows {
		r := &rows[i]
		if r.s.MatchedCount == 0 {
			continue
		}
		p, v := r.s.PropPrior, r.s.VOACAP
		fmt.Fprintf(b, "| %s | %d | %.2f | %.2f | %+.2f | %+.2f | %+.2f | %.2f%% | %.2f%% | %.1f%% | %.1f%% | %.1f%% / %.1f%% |\n",
			r.label, r.s.MatchedCount, p.MAE, v.MAE, r.s.DeltaMAE,
			p.MeanBias, v.MeanBias,
			100*p.FalseClosedRate, 100*v.FalseClosedRate,
			100*p.Within1TierAcc, 100*v.Within1TierAcc,
			100*p.ZAbsLE1Rate, 100*p.ZAbsLE2Rate)
	}
	b.WriteString("\n")
}

func writeModeTable(b *bytes.Buffer, title string, m map[string]headToHeadSummary) {
	if len(m) == 0 {
		return
	}
	fmt.Fprintf(b, "### %s\n\n", title)
	b.WriteString("| Mode | Matched N | v4.3 MAE | VOACAP MAE | v4.3 `ft8-pred` Mean/Med | VOACAP `ft8-pred` Mean/Med | v4.3 `raw-pred` Mean/Med | VOACAP `raw-pred` Mean/Med | v4.3 FC | VOACAP FC |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := m[k]
		if s.MatchedCount == 0 {
			continue
		}
		p, v := s.PropPrior, s.VOACAP
		fmt.Fprintf(b, "| %s | %d | %.2f | %.2f | %+.2f / %+.1f | %+.2f / %+.1f | %+.2f / %+.1f | %+.2f / %+.1f | %.2f%% | %.2f%% |\n",
			k, s.MatchedCount, p.MAE, v.MAE,
			p.MeanBias, p.MedianBias, v.MeanBias, v.MedianBias,
			p.RawMeanResidual, p.RawMedResidual, v.RawMeanResidual, v.RawMedResidual,
			100*p.FalseClosedRate, 100*v.FalseClosedRate)
	}
	b.WriteString("\n")
}

func writeRelBySliceTable(b *bytes.Buffer, title string, order []string, ppMap, voMap map[string]relCalibSummary) {
	var rows []string
	for _, k := range order {
		p, okP := ppMap[k]
		v, okV := voMap[k]
		if (!okP || p.Positives+p.Negatives == 0) && (!okV || v.Positives+v.Negatives == 0) {
			continue
		}
		rows = append(rows, k)
	}
	if len(rows) == 0 {
		return
	}
	fmt.Fprintf(b, "### %s\n\n", title)
	b.WriteString("| Slice | Pos | Neg | v4.3 AUC | VOACAP AUC | v4.3 Rel Pos/Neg | VOACAP Rel Pos/Neg | v4.3 False-Open (>=0.50 / >=0.70) | VOACAP False-Open (>=0.50 / >=0.70) |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, k := range rows {
		p := ppMap[k]
		v := voMap[k]
		fmt.Fprintf(b, "| %s | %d | %d | %.4f | %.4f | %.3f / %.3f | %.3f / %.3f | %.2f%% / %.2f%% | %.2f%% / %.2f%% |\n",
			k, p.Positives, p.Negatives, p.ROCAUC, v.ROCAUC,
			p.MeanRelPos, p.MeanRelNeg, v.MeanRelPos, v.MeanRelNeg,
			100*p.FalseOpenGE50, 100*p.FalseOpenGE70,
			100*v.FalseOpenGE50, 100*v.FalseOpenGE70)
	}
	b.WriteString("\n")
}
