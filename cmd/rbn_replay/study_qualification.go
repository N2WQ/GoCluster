// Qualification collapses only repeats within frozen episode/mode/distance cells.
// Unknown incremental outputs remain in each coverage denominator; any new
// strong disagreement survives the aggregation and prevents activation.
package main

import "fmt"

type studyEpisodeIdentity struct {
	Episode, Mode, Identity string
}

// The day owns at most one entry per delivered OFF row, including corrections
// outside active cells. Precomputing before accounting makes receipt movement
// within an episode irrelevant to the count of additional normalized identities.
type studyEpisodeCorrections map[studyEpisodeIdentity]struct{}

func studyOffEpisodeCorrections(pairs map[int64]*studyPair) studyEpisodeCorrections {
	identities := make(studyEpisodeCorrections)
	for _, pair := range pairs {
		if off := pair.Arms[1]; studyDeliveredCorrection(off) {
			identities[studyEpisodeIdentity{off.EpisodeID, off.Raw.Mode, off.FinalIdentity}] = struct{}{}
		}
	}
	return identities
}

func studyAccountEpisode(ep *studyEpisodeCell, off, on *studyDecision, cell string, offIdentities studyEpisodeCorrections) {
	onHere := studyDeliveredCorrection(on) && fmt.Sprintf("%s/%d", on.Raw.Mode, on.ConfiguredDistance) == cell
	offHere := studyDeliveredCorrection(off) && fmt.Sprintf("%s/%d", off.Raw.Mode, off.ConfiguredDistance) == cell
	if onHere {
		ep.OnSupportedR1 = ep.OnSupportedR1 || on.R1.Strong && on.R1.Identity == on.FinalIdentity
		ep.OnSupportedR2 = ep.OnSupportedR2 || on.R2.Strong && on.R2.Identity == on.FinalIdentity
	}
	if offHere {
		ep.OffSupportedR1 = ep.OffSupportedR1 || off.R1.Strong && off.R1.Identity == off.FinalIdentity
		ep.OffSupportedR2 = ep.OffSupportedR2 || off.R2.Strong && off.R2.Identity == off.FinalIdentity
	}
	if !onHere {
		return
	}
	// Contradictions are raw-row outcome changes, not the gain denominator.
	// A target already delivered elsewhere in the OFF episode still cannot
	// conceal a new rename disagreeing with this row's strong frozen label.
	if !studyDeliveredCorrection(off) || off.FinalIdentity != on.FinalIdentity {
		ep.NewWrongR1 = ep.NewWrongR1 || on.R1.Strong && on.R1.Identity != on.FinalIdentity
		ep.NewWrongR2 = ep.NewWrongR2 || on.R2.Strong && on.R2.Identity != on.FinalIdentity
	}
	if _, alreadyDelivered := offIdentities[studyEpisodeIdentity{on.EpisodeID, on.Raw.Mode, on.FinalIdentity}]; alreadyDelivered {
		return
	}
	ep.Incremental = true
	ep.CoveredR1 = ep.CoveredR1 && on.R1.Strong
	ep.CoveredR2 = ep.CoveredR2 && on.R2.Strong
	ep.Joint = ep.Joint && on.R1.Strong && on.R2.Strong
}

func studyBool(v bool) int {
	if v {
		return 1
	}
	return 0
}

func studySummarizeDate(date string, cells map[string]map[string]*studyEpisodeCell) map[string]studyCellDate {
	result := make(map[string]studyCellDate, len(cells))
	for cell, episodes := range cells {
		count := studyCellDate{DateUTC: date}
		for _, ep := range episodes {
			count.NetSupportedR1 += studyBool(ep.OnSupportedR1) - studyBool(ep.OffSupportedR1)
			count.NetSupportedR2 += studyBool(ep.OnSupportedR2) - studyBool(ep.OffSupportedR2)
			count.NewWrongR1 += studyBool(ep.NewWrongR1)
			count.NewWrongR2 += studyBool(ep.NewWrongR2)
			if !ep.Incremental {
				continue
			}
			count.Incremental++
			count.Joint += studyBool(ep.Joint)
			count.CoveredR1 += studyBool(ep.CoveredR1)
			count.CoveredR2 += studyBool(ep.CoveredR2)
		}
		result[cell] = count
	}
	return result
}

func studyQualifyCell(cell string, dates []studyCellDate) studyCellVerdict {
	v := studyCellVerdict{Cell: cell, Dates: dates, Reasons: []string{}}
	var total, covered1, covered2, wrong1, wrong2, dates50 int
	for _, d := range dates {
		v.Joint += d.Joint
		total, covered1, covered2 = total+d.Incremental, covered1+d.CoveredR1, covered2+d.CoveredR2
		wrong1, wrong2 = wrong1+d.NewWrongR1, wrong2+d.NewWrongR2
		if d.Joint >= 50 {
			dates50++
		}
		if d.NetSupportedR1 <= 0 || d.NetSupportedR2 <= 0 {
			v.Reasons = append(v.Reasons, d.DateUTC+": no positive gain under both controls")
		}
	}
	if total > 0 {
		v.CoverageR1, v.CoverageR2 = 100*float64(covered1)/float64(total), 100*float64(covered2)/float64(total)
	}
	if v.Joint < 200 || dates50 < 2 {
		v.Reasons = append(v.Reasons, "requires >=200 jointly strong additional episodes and >=50 on each of two dates")
	}
	if wrong1 > 0 || wrong2 > 0 {
		v.Reasons = append(v.Reasons, "new strong contradiction under at least one reference")
	}
	// Integer comparison fixes the exact 80% boundary without float rounding.
	if total == 0 || covered1*5 < total*4 || covered2*5 < total*4 {
		v.Reasons = append(v.Reasons, "each reference must cover >=80% of all incremental episode IDs")
	}
	v.Pass = len(v.Reasons) == 0
	return v
}
