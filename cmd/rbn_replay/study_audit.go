// File role: Re-score frozen terminal receipts without running a predictor.
// A day owns the archive rows, accepted A-only sightings, three terminal streams
// and three legacy flag bits per row. All are released before the next day.
// The 8M-row/512 MiB frozen input limits bound these owners, not process RSS.
package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	stabilitypkg "dxcluster/internal/stability"
	"dxcluster/spot"
)

type studyAuditFraction struct {
	Stable  int64 `json:"stable"`
	Applied int64 `json:"applied"`
	Defined bool  `json:"defined"`
}

type studyAuditCounts struct {
	RawUnion                int64                 `json:"terminal_raw_id_union"`
	Arms                    [3]studyAuditFraction `json:"arms_shipped_off_on"`
	Common                  int64                 `json:"common_applied"`
	CommonSame              int64                 `json:"common_same_winner_and_clock"`
	CommonRetargeted        int64                 `json:"common_retargeted"`
	CommonSemanticChanged   int64                 `json:"common_semantic_target_changed"`
	CommonLiteralChanged    int64                 `json:"common_literal_winner_changed"`
	CommonScorerKeyChanged  int64                 `json:"common_scorer_winner_key_changed"`
	CommonRetimed           int64                 `json:"common_retimed"`
	CommonRetargetedRetimed int64                 `json:"common_retargeted_and_retimed"`
	ONOnly                  int64                 `json:"on_only_applied"`
	OFFOnly                 int64                 `json:"off_only_applied"`
	ONOnlyFraction          studyAuditFraction    `json:"on_only_stability"`
	OFFOnlyFraction         studyAuditFraction    `json:"off_only_stability"`
	Neither                 int64                 `json:"neither_applied"`
	Stable00                int64                 `json:"common_stability_00"`
	Stable01                int64                 `json:"common_stability_01"`
	Stable10                int64                 `json:"common_stability_10"`
	Stable11                int64                 `json:"common_stability_11"`
	Statuses                map[string]int64      `json:"off_on_terminal_statuses"`
	ExactRateComparison     string                `json:"exact_on_vs_off_rate"`
}

type studyAuditDaySummary struct {
	Group            int                          `json:"group"`
	DateUTC          string                       `json:"date_utc"`
	Phase            string                       `json:"phase"`
	RawRows          int                          `json:"archive_rows"`
	AcceptedARows    int                          `json:"accepted_a_raw_rows_before_beacon_filter"`
	FocalRows        int                          `json:"focal_a_rows"`
	Counts           studyAuditCounts             `json:"counts"`
	Modes            map[string]*studyAuditCounts `json:"modes"`
	Legacy           [3]studyAuditFraction        `json:"legacy_60_2_1000_exact_manifest_match"`
	Reports          [3]studyAuditReportInput     `json:"frozen_reports_and_manifests"`
	Decisions        [3]studyFile                 `json:"frozen_decisions"`
	Archive          studyFile                    `json:"frozen_archive"`
	ReceiverMetadata studyFile                    `json:"frozen_receiver_metadata"`
}

type studyAuditGroupProvenance struct {
	Plan            studyFile          `json:"plan"`
	Phase           string             `json:"phase"`
	Candidate       studyCandidate     `json:"candidate"`
	CandidateSHA256 string             `json:"candidate_sha256"`
	DeclaredOldArms []studyArm         `json:"declared_old_arms_not_reloaded"`
	InputScenario   studyInputScenario `json:"input_scenario"`
}

type studyAuditResult struct {
	SchemaVersion    int                          `json:"schema_version"`
	Complete         bool                         `json:"complete"`
	Exposed          bool                         `json:"exposed"`
	AuditPlan        studyFile                    `json:"audit_plan"`
	CurrentStability replayStabilityConfig        `json:"current_stability"`
	ArmOrder         [3]string                    `json:"arm_order"`
	Groups           []studyAuditGroupProvenance  `json:"groups"`
	Days             []studyAuditDaySummary       `json:"days"`
	Counts           studyAuditCounts             `json:"descriptive_pooled_counts"`
	Modes            map[string]*studyAuditCounts `json:"descriptive_pooled_modes"`
	Receipts         studyFile                    `json:"applied_receipts"`
	Paired           studyFile                    `json:"full_paired_union"`
	ClaimLimits      []string                     `json:"claim_limits"`
}

type studyAuditReceipt struct {
	Status        string    `json:"status"`
	Applied       bool      `json:"applied"`
	ActualApplied bool      `json:"actual_applied_output"`
	FinalIdentity string    `json:"final_identity"`
	FinalObserved string    `json:"final_observed"`
	DecisionAtUTC time.Time `json:"decision_at_utc"`
	WinnerKey     string    `json:"scorer_winner_key"`
	LegacyStable  bool      `json:"legacy_stable"`
	CurrentStable bool      `json:"current_stable"`
}

type studyAuditAppliedRecord struct {
	Group           int               `json:"group"`
	DateUTC         string            `json:"date_utc"`
	Phase           string            `json:"phase"`
	Arm             string            `json:"arm"`
	RawID           string            `json:"raw_id"`
	RawSHA256       string            `json:"raw_sha256"`
	Mode            string            `json:"mode"`
	SubjectIdentity string            `json:"subject_identity"`
	Receipt         studyAuditReceipt `json:"receipt"`
}

type studyAuditPairRecord struct {
	Group           int                  `json:"group"`
	DateUTC         string               `json:"date_utc"`
	Phase           string               `json:"phase"`
	RawID           string               `json:"raw_id"`
	RawSHA256       string               `json:"raw_sha256"`
	Mode            string               `json:"mode"`
	SubjectIdentity string               `json:"subject_identity"`
	Class           string               `json:"off_on_applied_pair_class"`
	Arms            [3]studyAuditReceipt `json:"arms_shipped_off_on"`
}

func runCCV2Audit(planPath, frozenSHA, output string) error {
	return studyAuditRun(planPath, frozenSHA, output, studyAuditDefaultFileOps())
}

// The sole injection seam is run-owned file output. It is used to prove that
// late writer/finalization errors cannot publish COMPLETE, without globals.
func studyAuditRun(planPath, frozenSHA, output string, ops studyAuditFileOps) (err error) {
	p, digest, err := loadStudyAuditPlan(planPath, frozenSHA)
	if err != nil {
		return err
	}
	if err := studyAuditNewDirectory(output); err != nil {
		return err
	}
	quota := &studyAuditQuota{limit: studyAuditOutputBytes, reserve: studyAuditReserveBytes}
	defer func() {
		if err != nil {
			err = errors.Join(err, studyAuditFailure(output, err, quota, ops))
		}
	}()
	loadedGroups, err := studyAuditPrepareGroups(p)
	if err != nil {
		return err
	}
	receipts, err := studyAuditOpenWriter(filepath.Join(output, "applied_receipts.jsonl"), quota, ops)
	if err != nil {
		return err
	}
	paired, err := studyAuditOpenWriter(filepath.Join(output, "paired_union.jsonl"), quota, ops)
	if err != nil {
		return errors.Join(err, receipts.close())
	}
	defer func() { err = errors.Join(err, receipts.close(), paired.close()) }()
	result := studyAuditResult{SchemaVersion: studyAuditSchema, Exposed: true, AuditPlan: studyFile{Path: planPath, SHA256: digest},
		CurrentStability: p.Stability, ArmOrder: [3]string{"shipped", "repaired_off", "repaired_on"}, Counts: studyAuditNewCounts(), Modes: make(map[string]*studyAuditCounts),
		ClaimLimits: []string{
			"Exposed retrospective stability audit of existing frozen replay receipts; no new prediction, candidate selection, holdout qualification or RF-truth claim",
			"Actual means frozen status=output and applied=true at the replay output boundary, not a network delivery receipt; no hypothetical corrections are counted",
			"A-only accepted raw rows include accepted beacon classes before the replay beacon filter; B rows are excluded; repeated rows count, not unique receivers",
			"Winner key is NormalizeCallsign(final_observed); raw key is NormalizeSpotDXCallsign(raw DX); decision_at_utc starts the inclusive window",
			"No extra mode/band filter and no next-day archive; day-end windows have only the frozen day's observed support",
			"Source covariates, R1/R2 labels and original model/config/asset references are retained provenance and never scorer inputs",
			"Compact rows join by immutable raw_id to the hash-bound frozen decisions, intentionally retaining all per-ID R1/R2 labels and unknown reasons in those original inputs",
			"Exact fractions describe dependent archive receipts; zero denominators are undefined; pooled counts are descriptive, not independent or calibrated accuracy evidence",
			"Common same requires equal semantic target, literal final winner, scorer winner key and decision clock; retargeted counts any target/winner/key change, with separate overlapping change counters",
			"Archive rows, accepted sightings, terminals and legacy flags are day-owned and released between dates; frozen 8M-row/512 MiB CSV limits do not establish process RSS",
		}}
	for gi := range loadedGroups {
		loaded := &loadedGroups[gi]
		group := loaded.Input
		result.Groups = append(result.Groups, studyAuditGroupProvenance{Plan: group.Plan, Phase: group.Phase, Candidate: loaded.Candidate,
			CandidateSHA256: hashAnyStable(loaded.Candidate), DeclaredOldArms: loaded.Plan.Arms, InputScenario: loaded.Plan.InputScenario})
		aliases, err := loadStudyReceiverAliases(loaded.Plan.ReceiverMetadata)
		if err != nil {
			return err
		}
		for _, input := range loaded.Plan.Inputs {
			if input.Phase != group.Phase {
				continue
			}
			day, err := studyAuditScoreDay(gi, *loaded, input, aliases, p.Stability, receipts, paired)
			if err != nil {
				return fmt.Errorf("audit %s/%s: %w", group.Phase, input.DateUTC, err)
			}
			result.Days = append(result.Days, day)
			studyAuditMergeCounts(&result.Counts, day.Counts)
			for mode, counts := range day.Modes {
				if result.Modes[mode] == nil {
					c := studyAuditNewCounts()
					result.Modes[mode] = &c
				}
				studyAuditMergeCounts(result.Modes[mode], *counts)
			}
		}
	}
	if err := errors.Join(receipts.close(), paired.close()); err != nil {
		return err
	}
	result.Receipts, err = receipts.binding()
	if err != nil {
		return err
	}
	result.Paired, err = paired.binding()
	if err != nil {
		return err
	}
	result.Complete = true
	if err := studyAuditAtomicJSON(output, "aggregates.json", result, quota, ops); err != nil {
		return err
	}
	sum, err := studyHashFile(filepath.Join(output, "aggregates.json"))
	if err != nil {
		return err
	}
	return studyAuditAtomicJSON(output, "COMPLETE.json", struct {
		SchemaVersion int       `json:"schema_version"`
		Complete      bool      `json:"complete"`
		AuditPlan     studyFile `json:"audit_plan"`
		Aggregates    studyFile `json:"aggregates"`
		Receipts      studyFile `json:"applied_receipts"`
		Paired        studyFile `json:"full_paired_union"`
	}{studyAuditSchema, true, result.AuditPlan, studyFile{Path: filepath.Join(output, "aggregates.json"), SHA256: sum}, result.Receipts, result.Paired}, quota, ops)
}

// Recheck all small hash-bound metadata after loading, before archive scoring.
func studyAuditLoadAndFreezeGroup(group studyAuditGroup) (studyAuditLoadedGroup, error) {
	loaded, err := studyAuditLoadGroup(group)
	if err != nil {
		return loaded, err
	}
	if err := studyCheckFile(group.Plan); err != nil {
		return loaded, err
	}
	for _, binding := range group.Reports {
		if err := errors.Join(studyCheckFile(binding.Report), studyCheckFile(binding.Manifest)); err != nil {
			return loaded, err
		}
	}
	return loaded, nil
}

func studyAuditScoreDay(group int, loaded studyAuditLoadedGroup, input studyInput, aliases map[string]string,
	current replayStabilityConfig, receipts, paired *studyAuditWriter) (studyAuditDaySummary, error) {
	summary := studyAuditDaySummary{Group: group, DateUTC: input.DateUTC, Phase: input.Phase, Counts: studyAuditNewCounts(), Modes: make(map[string]*studyAuditCounts),
		Reports: loaded.Bindings[input.DateUTC], Archive: input.Archive, ReceiverMetadata: loaded.Plan.ReceiverMetadata}
	day, err := loadStudyRawDay(input, aliases, loaded.Plan.WarmupSeconds)
	if err != nil {
		return summary, err
	}
	// Reference indices are not needed by this audit's scorer or provenance
	// check. Drop their backing storage while retaining frozen episode IDs.
	day.Index = nil
	spots, accepted, bad, skipped, focal := studyAuditRawPopulation(day)
	summary.RawRows, summary.AcceptedARows, summary.FocalRows = len(day.Rows), accepted, focal
	reports := loaded.Reports[input.DateUTC]
	for arm, report := range reports {
		manifest := loaded.Manifests[input.DateUTC][arm]
		if report.RawRows != len(day.Rows) || report.FocalRows != focal || report.TerminalRows != focal ||
			manifest.CSV.RecordsTotal != int64(len(day.Rows)) || manifest.CSV.RecordsProcessed != int64(accepted) ||
			manifest.CSV.RecordsSkippedBad != int64(bad) || manifest.CSV.RecordsSkippedMode != int64(skipped) {
			return summary, errors.New("archive population does not reconcile with report/manifest integer counts")
		}
		summary.Decisions[arm] = report.Decisions
	}
	pairs, err := studyReadDayPairs(input, reports)
	if err != nil {
		return summary, err
	}
	if err := studyAuditArchiveBindings(day, pairs); err != nil {
		return summary, err
	}
	legacyFlags := make([]uint8, len(day.Rows))
	legacy := replayStabilityConfig{WindowMinutes: 60, MinFollowOn: 2, FreqToleranceHz: 1000}
	for ordinal, pair := range pairs {
		for arm, d := range pair.Arms {
			if !studyAuditApplied(d) {
				continue
			}
			summary.Legacy[arm].Applied++
			if studyAuditStable(d, spots, legacy) {
				summary.Legacy[arm].Stable++
				legacyFlags[ordinal-1] |= 1 << arm
			}
		}
	}
	for arm := range reports {
		summary.Legacy[arm].Defined = summary.Legacy[arm].Applied > 0
		expected := loaded.Manifests[input.DateUTC][arm].Results.Stability
		if summary.Legacy[arm].Applied != int64(expected.TotalApplied) || summary.Legacy[arm].Stable != int64(expected.StableApplied) {
			return summary, fmt.Errorf("legacy scorer reconciliation failed for %s: got %d/%d expected %d/%d", reports[arm].Arm,
				summary.Legacy[arm].Stable, summary.Legacy[arm].Applied, expected.StableApplied, expected.TotalApplied)
		}
	}
	// Ordinal iteration provides deterministic output even when delayed terminal
	// streams have different orders. It never zips their stream positions.
	for index := range day.Rows {
		raw := &day.Rows[index]
		pair := pairs[int64(index+1)]
		if pair == nil {
			continue
		}
		record := studyAuditPairRecord{Group: group, DateUTC: input.DateUTC, Phase: input.Phase, RawID: pair.Arms[0].RawID,
			RawSHA256: pair.Arms[0].RawSHA256, Mode: raw.Row.Mode, SubjectIdentity: pair.Arms[0].SubjectIdentity}
		for arm, d := range pair.Arms {
			record.Arms[arm] = studyAuditMakeReceipt(d, legacyFlags[index]&(1<<arm) != 0)
			if record.Arms[arm].ActualApplied {
				record.Arms[arm].CurrentStable = studyAuditStable(d, spots, current)
				if err := receipts.record(studyAuditAppliedRecord{Group: group, DateUTC: input.DateUTC, Phase: input.Phase, Arm: reports[arm].Arm,
					RawID: d.RawID, RawSHA256: d.RawSHA256, Mode: d.Raw.Mode, SubjectIdentity: d.SubjectIdentity, Receipt: record.Arms[arm]}); err != nil {
					return summary, err
				}
			}
		}
		record.Class = studyAuditPairClass(record.Arms[1], record.Arms[2])
		if err := paired.record(record); err != nil {
			return summary, err
		}
		studyAuditAccount(&summary.Counts, record.Arms)
		if summary.Modes[raw.Row.Mode] == nil {
			c := studyAuditNewCounts()
			summary.Modes[raw.Row.Mode] = &c
		}
		studyAuditAccount(summary.Modes[raw.Row.Mode], record.Arms)
	}
	// Rehash immutable evidence after reads as well; the audit never updates it.
	if err := errors.Join(studyCheckFile(input.Archive), studyCheckFile(loaded.Plan.ReceiverMetadata)); err != nil {
		return summary, err
	}
	for _, report := range reports {
		if err := studyCheckFile(report.Decisions); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

func studyAuditRawPopulation(day *studyRawDay) (map[string][]stabilitypkg.Spot, int, int, int, int) {
	spots := make(map[string][]stabilitypkg.Spot)
	accepted, bad, skipped, focal := 0, 0, 0, 0
	for index := range day.Rows {
		raw := &day.Rows[index]
		if !raw.Valid {
			bad++
			continue
		}
		if !raw.A {
			continue
		}
		if !raw.Row.SpotClass.Accepted() || !spot.IsCallCorrectionCandidate(raw.Row.Mode) {
			skipped++
			continue
		}
		accepted++
		if studyFocalRow(*raw) {
			focal++
		}
		key := spot.NormalizeSpotDXCallsign(raw.Row.DXCall)
		if key != "" && raw.Row.FreqKHz > 0 && !raw.Row.Time.IsZero() {
			spots[key] = append(spots[key], stabilitypkg.Spot{Ts: raw.Row.Time.UTC().Unix(), Freq: raw.Row.FreqKHz})
		}
	}
	return spots, accepted, bad, skipped, focal
}

func studyAuditArchiveBindings(day *studyRawDay, pairs map[int64]*studyPair) error {
	focal := 0
	for i := range day.Rows {
		raw := &day.Rows[i]
		pair := pairs[int64(i+1)]
		if !studyFocalRow(*raw) {
			if pair != nil {
				return errors.New("terminal decision binds a nonfocal archive row")
			}
			continue
		}
		focal++
		if pair == nil {
			return errors.New("terminal union is missing a focal archive ID")
		}
		for _, d := range pair.Arms {
			if d == nil {
				return errors.New("terminal union is missing a mandatory arm's focal raw ID")
			}
			if d.Raw != raw.Row || d.Ordinal != raw.Ordinal || d.ReceiverFamily != raw.Family || !raw.A || d.EpisodeID != raw.Episode ||
				!studyDecisionBinding(*d, day.InputSHA256) || d.DecisionAtUTC.Before(raw.Row.Time) ||
				d.DecisionAfterCutoff != d.DecisionAtUTC.After(raw.Row.Time.Add(studyDecisionSeconds*time.Second)) {
				return errors.New("terminal raw payload/family/episode/clock does not bind its actual archive ordinal")
			}
			if err := studyAuditLabels(reflect.ValueOf(d)); err != nil {
				return err
			}
		}
	}
	if len(pairs) != focal {
		return errors.New("terminal union contains an extra archive ID")
	}
	return nil
}

func studyAuditApplied(d *studyDecision) bool { return d != nil && d.Status == "output" && d.Applied }

func studyAuditMakeReceipt(d *studyDecision, legacy bool) studyAuditReceipt {
	return studyAuditReceipt{Status: d.Status, Applied: d.Applied, ActualApplied: studyAuditApplied(d), FinalIdentity: d.FinalIdentity,
		FinalObserved: d.FinalObserved, DecisionAtUTC: d.DecisionAtUTC, WinnerKey: spot.NormalizeCallsign(d.FinalObserved), LegacyStable: legacy}
}

func studyAuditStable(d *studyDecision, spots map[string][]stabilitypkg.Spot, cfg replayStabilityConfig) bool {
	if !studyAuditApplied(d) {
		return false
	}
	winner := spot.NormalizeCallsign(d.FinalObserved)
	if winner == "" || d.DecisionAtUTC.Unix() <= 0 || d.Raw.FreqKHz <= 0 {
		return false
	}
	result := stabilitypkg.EvaluateFollowOn([]stabilitypkg.Correction{{Ts: d.DecisionAtUTC.UTC().Unix(), Winner: winner, Freq: d.Raw.FreqKHz, Band: d.Raw.Band}}, spots,
		d.Raw.Time.UTC().Unix(), stabilitypkg.Config{BucketMinutes: 60, WindowMinutes: cfg.WindowMinutes, MinFollowOn: cfg.MinFollowOn, FreqToleranceHz: cfg.FreqToleranceHz})
	return result.TotalCount == 1 && result.StableCount == 1
}
