// File role: Preserve complete experimental pair counts without selecting a grid
// winner. Receipt and episode changes are separate; overlap is reported rather
// than choosing a loss/retarget/retime tie policy. Fractions use exact integers.
package main

import (
	"errors"
	"fmt"
)

type studyMethodEvalCounts struct {
	RawIDs                 int64                  `json:"all_terminal_ids"`
	Control                studyAuditFraction     `json:"control"`
	Variant                studyAuditFraction     `json:"variant"`
	Added                  int64                  `json:"added_applied_receipts"`
	Lost                   int64                  `json:"lost_applied_receipts"`
	Common                 int64                  `json:"common_applied_receipts"`
	Retargeted             int64                  `json:"retargeted_baseline_receipts"`
	Retimed                int64                  `json:"retimed_baseline_receipts"`
	RetargetedRetimed      int64                  `json:"retargeted_and_retimed_baseline_receipts"`
	AffectedBaselineIDs    int64                  `json:"affected_baseline_id_union"`
	SemanticChanged        int64                  `json:"semantic_target_changed"`
	LiteralChanged         int64                  `json:"literal_target_changed"`
	ScorerKeyChanged       int64                  `json:"scorer_key_changed"`
	OutsideCells           int64                  `json:"new_identity_renames_outside_active_cells"`
	NewWrongR1             int64                  `json:"all_cells_new_r1_strong_disagreeing_renames"`
	NewWrongR2             int64                  `json:"all_cells_new_r2_strong_disagreeing_renames"`
	NewlyAssessableWrongR1 int64                  `json:"newly_assessable_r1_disagreement_same_target"`
	NewlyAssessableWrongR2 int64                  `json:"newly_assessable_r2_disagreement_same_target"`
	Statuses               map[string]int64       `json:"terminal_status_pairs"`
	Classes                map[string]int64       `json:"applied_pair_classes"`
	Outcomes               map[string]int64       `json:"reference_and_delivery_outcomes"`
	ControlReferences      studyReferenceCoverage `json:"control_reference_coverage"`
	VariantReferences      studyReferenceCoverage `json:"variant_reference_coverage"`
	ExactRateComparison    string                 `json:"exact_variant_vs_control_rate"`
}
type studyMethodEvalEpisodeChanges struct {
	Added             int64 `json:"episodes_with_added_receipt"`
	Lost              int64 `json:"episodes_with_lost_receipt"`
	Retargeted        int64 `json:"episodes_with_retargeted_receipt"`
	Retimed           int64 `json:"episodes_with_retimed_receipt"`
	RetargetedRetimed int64 `json:"episodes_with_both_retarget_and_retime"`
	Affected          int64 `json:"affected_baseline_episode_union"`
}

func studyMethodEvalNewCounts() studyMethodEvalCounts {
	return studyMethodEvalCounts{Statuses: make(map[string]int64), Classes: make(map[string]int64), Outcomes: make(map[string]int64), ExactRateComparison: "undefined",
		ControlReferences: studyReferenceCoverage{R1Unknown: make(map[string]int), R2Unknown: make(map[string]int)}, VariantReferences: studyReferenceCoverage{R1Unknown: make(map[string]int), R2Unknown: make(map[string]int)}}
}

func studyMethodEvalAccount(c *studyMethodEvalCounts, control, variant *studyDecision, a, b studyAuditReceipt, active map[string]bool) error {
	c.RawIDs++
	for _, item := range []struct {
		r studyAuditReceipt
		f *studyAuditFraction
	}{{a, &c.Control}, {b, &c.Variant}} {
		if item.r.ActualApplied {
			item.f.Applied++
			if item.r.CurrentStable {
				item.f.Stable++
			}
		}
	}
	class := studyAuditPairClass(a, b)
	c.Classes[class]++
	c.Statuses[a.Status+"/"+b.Status]++
	c.Outcomes[studyPairedClass(control, variant)]++
	if !a.ActualApplied && b.ActualApplied {
		c.Added++
	}
	if a.ActualApplied && !b.ActualApplied {
		c.Lost++
		c.AffectedBaselineIDs++
	}
	if a.ActualApplied && b.ActualApplied {
		c.Common++
		retarget := a.FinalIdentity != b.FinalIdentity || a.FinalObserved != b.FinalObserved || a.WinnerKey != b.WinnerKey
		retime := !a.DecisionAtUTC.Equal(b.DecisionAtUTC)
		if retarget {
			c.Retargeted++
		}
		if retime {
			c.Retimed++
		}
		if retarget && retime {
			c.RetargetedRetimed++
		}
		if retarget || retime {
			c.AffectedBaselineIDs++
		}
		if a.FinalIdentity != b.FinalIdentity {
			c.SemanticChanged++
		}
		if a.FinalObserved != b.FinalObserved {
			c.LiteralChanged++
		}
		if a.WinnerKey != b.WinnerKey {
			c.ScorerKeyChanged++
		}
		if control.FinalIdentity == variant.FinalIdentity {
			if !control.R1.Strong && variant.R1.Strong && variant.R1.Identity != variant.FinalIdentity {
				c.NewlyAssessableWrongR1++
			}
			if !control.R2.Strong && variant.R2.Strong && variant.R2.Identity != variant.FinalIdentity {
				c.NewlyAssessableWrongR2++
			}
		}
	}
	if studyDeliveredCorrection(variant) && (!studyDeliveredCorrection(control) || control.FinalIdentity != variant.FinalIdentity) {
		if !active[fmt.Sprintf("%s/%d", variant.Raw.Mode, variant.ConfiguredDistance)] {
			c.OutsideCells++
		}
		if variant.R1.Strong && variant.R1.Identity != variant.FinalIdentity {
			c.NewWrongR1++
		}
		if variant.R2.Strong && variant.R2.Identity != variant.FinalIdentity {
			c.NewWrongR2++
		}
	}
	if err := studyMethodEvalReference(&c.ControlReferences, control); err != nil {
		return err
	}
	if err := studyMethodEvalReference(&c.VariantReferences, variant); err != nil {
		return err
	}
	studyMethodEvalRefresh(c)
	return nil
}

func studyMethodEvalReference(c *studyReferenceCoverage, d *studyDecision) error {
	c.Denominator++
	for _, item := range []struct {
		r       studyReference
		n       *int
		unknown map[string]int
	}{{d.R1, &c.R1Strong, c.R1Unknown}, {d.R2, &c.R2Strong, c.R2Unknown}} {
		if item.r.Strong {
			*item.n++
		} else {
			if _, exists := item.unknown[item.r.Reason]; !exists && len(item.unknown) >= 32 {
				return errors.New("reference reason vocabulary exceeds 32")
			}
			item.unknown[item.r.Reason]++
		}
	}
	if d.R1.Strong && d.R2.Strong && d.R1.Identity != d.R2.Identity {
		c.StrongLabelDisagreements++
	}
	return nil
}

func studyMethodEvalRefresh(c *studyMethodEvalCounts) {
	c.Control.Defined, c.Variant.Defined = c.Control.Applied > 0, c.Variant.Applied > 0
	switch studyAuditCompareFractions(c.Control, c.Variant) {
	case "on_higher":
		c.ExactRateComparison = "variant_higher"
	case "off_higher":
		c.ExactRateComparison = "control_higher"
	case "equal":
		c.ExactRateComparison = "equal"
	default:
		c.ExactRateComparison = "undefined"
	}
}

func studyMethodEvalObjective(c studyMethodEvalCounts) bool {
	return c.Control.Applied > 0 && c.Variant.Applied > c.Control.Applied && (studyAuditCompareFractions(c.Control, c.Variant) == "on_higher" || studyAuditCompareFractions(c.Control, c.Variant) == "equal")
}

func studyMethodEvalMerge(dst *studyMethodEvalCounts, src studyMethodEvalCounts) error {
	dst.RawIDs += src.RawIDs
	dst.Control.Applied += src.Control.Applied
	dst.Control.Stable += src.Control.Stable
	dst.Variant.Applied += src.Variant.Applied
	dst.Variant.Stable += src.Variant.Stable
	dst.Added += src.Added
	dst.Lost += src.Lost
	dst.Common += src.Common
	dst.Retargeted += src.Retargeted
	dst.Retimed += src.Retimed
	dst.RetargetedRetimed += src.RetargetedRetimed
	dst.AffectedBaselineIDs += src.AffectedBaselineIDs
	dst.SemanticChanged += src.SemanticChanged
	dst.LiteralChanged += src.LiteralChanged
	dst.ScorerKeyChanged += src.ScorerKeyChanged
	dst.OutsideCells += src.OutsideCells
	dst.NewWrongR1 += src.NewWrongR1
	dst.NewWrongR2 += src.NewWrongR2
	dst.NewlyAssessableWrongR1 += src.NewlyAssessableWrongR1
	dst.NewlyAssessableWrongR2 += src.NewlyAssessableWrongR2
	for _, pair := range []struct{ a, b map[string]int64 }{{dst.Statuses, src.Statuses}, {dst.Classes, src.Classes}, {dst.Outcomes, src.Outcomes}} {
		for k, v := range pair.b {
			if _, exists := pair.a[k]; !exists && len(pair.a) >= 32 {
				return errors.New("evaluation count vocabulary exceeds 32")
			}
			pair.a[k] += v
		}
	}
	for _, pair := range []struct {
		a *studyReferenceCoverage
		b studyReferenceCoverage
	}{{&dst.ControlReferences, src.ControlReferences}, {&dst.VariantReferences, src.VariantReferences}} {
		pair.a.Denominator += pair.b.Denominator
		pair.a.R1Strong += pair.b.R1Strong
		pair.a.R2Strong += pair.b.R2Strong
		pair.a.StrongLabelDisagreements += pair.b.StrongLabelDisagreements
		for _, maps := range []struct{ a, b map[string]int }{{pair.a.R1Unknown, pair.b.R1Unknown}, {pair.a.R2Unknown, pair.b.R2Unknown}} {
			for k, v := range maps.b {
				if _, exists := maps.a[k]; !exists && len(maps.a) >= 32 {
					return errors.New("pooled reference vocabulary exceeds 32")
				}
				maps.a[k] += v
			}
		}
	}
	studyMethodEvalRefresh(dst)
	return nil
}

// Each mask belongs to one frozen day episode, bounded by that day's focal rows.
// Counting masks preserves overlaps while keeping receipt movement distinct from
// the normalized additional identities used by the existing reference floors.
func studyMethodEvalEpisodeMask(a, b studyAuditReceipt) uint8 {
	var mask uint8
	if !a.ActualApplied && b.ActualApplied {
		mask |= 1
	}
	if a.ActualApplied && !b.ActualApplied {
		mask |= 2
	}
	if a.ActualApplied && b.ActualApplied {
		if a.FinalIdentity != b.FinalIdentity || a.FinalObserved != b.FinalObserved || a.WinnerKey != b.WinnerKey {
			mask |= 4
		}
		if !a.DecisionAtUTC.Equal(b.DecisionAtUTC) {
			mask |= 8
		}
	}
	return mask
}
func studyMethodEvalEpisodeTotals(masks map[string]uint8) studyMethodEvalEpisodeChanges {
	var out studyMethodEvalEpisodeChanges
	for _, mask := range masks {
		if mask&1 != 0 {
			out.Added++
		}
		if mask&2 != 0 {
			out.Lost++
		}
		if mask&4 != 0 {
			out.Retargeted++
		}
		if mask&8 != 0 {
			out.Retimed++
		}
		if mask&12 == 12 {
			out.RetargetedRetimed++
		}
		if mask&14 != 0 {
			out.Affected++
		}
	}
	return out
}
func studyMethodEvalMergeEpisodes(a *studyMethodEvalEpisodeChanges, b studyMethodEvalEpisodeChanges) {
	a.Added += b.Added
	a.Lost += b.Lost
	a.Retargeted += b.Retargeted
	a.Retimed += b.Retimed
	a.RetargetedRetimed += b.RetargetedRetimed
	a.Affected += b.Affected
}
