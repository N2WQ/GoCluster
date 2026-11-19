// File role: Account actual receipt pairs and compare fractions exactly.
// At most 32*8M receipts per arm makes stable*applied < 6.6e16: all
// cross-products fit int64. No rounded percentage determines a comparison.
package main

func studyAuditNewCounts() studyAuditCounts {
	return studyAuditCounts{Statuses: make(map[string]int64), ExactRateComparison: "undefined"}
}

func studyAuditPairClass(off, on studyAuditReceipt) string {
	if !off.ActualApplied && !on.ActualApplied {
		return "neither"
	}
	if !off.ActualApplied {
		return "on_only"
	}
	if !on.ActualApplied {
		return "off_only"
	}
	retarget := off.FinalIdentity != on.FinalIdentity || off.FinalObserved != on.FinalObserved || off.WinnerKey != on.WinnerKey
	retime := !off.DecisionAtUTC.Equal(on.DecisionAtUTC)
	if retarget && retime {
		return "common_retarget_and_retime"
	}
	if retarget {
		return "common_retarget"
	}
	if retime {
		return "common_retime"
	}
	return "common_same"
}

func studyAuditAccount(counts *studyAuditCounts, arms [3]studyAuditReceipt) {
	counts.RawUnion++
	for i, receipt := range arms {
		if receipt.ActualApplied {
			counts.Arms[i].Applied++
			if receipt.CurrentStable {
				counts.Arms[i].Stable++
			}
		}
	}
	off, on := arms[1], arms[2]
	counts.Statuses[off.Status+"/"+on.Status]++
	switch studyAuditPairClass(off, on) {
	case "neither":
		counts.Neither++
	case "on_only":
		counts.ONOnly++
		counts.ONOnlyFraction.Applied++
		if on.CurrentStable {
			counts.ONOnlyFraction.Stable++
		}
	case "off_only":
		counts.OFFOnly++
		counts.OFFOnlyFraction.Applied++
		if off.CurrentStable {
			counts.OFFOnlyFraction.Stable++
		}
	case "common_same":
		counts.Common++
		counts.CommonSame++
	case "common_retarget":
		counts.Common++
		counts.CommonRetargeted++
	case "common_retime":
		counts.Common++
		counts.CommonRetimed++
	case "common_retarget_and_retime":
		counts.Common++
		counts.CommonRetargeted++
		counts.CommonRetimed++
		counts.CommonRetargetedRetimed++
	}
	if off.ActualApplied && on.ActualApplied {
		if off.FinalIdentity != on.FinalIdentity {
			counts.CommonSemanticChanged++
		}
		if off.FinalObserved != on.FinalObserved {
			counts.CommonLiteralChanged++
		}
		if off.WinnerKey != on.WinnerKey {
			counts.CommonScorerKeyChanged++
		}
		switch {
		case off.CurrentStable && on.CurrentStable:
			counts.Stable11++
		case off.CurrentStable:
			counts.Stable10++
		case on.CurrentStable:
			counts.Stable01++
		default:
			counts.Stable00++
		}
	}
	studyAuditRefreshFractions(counts)
}

func studyAuditMergeCounts(dst *studyAuditCounts, src studyAuditCounts) {
	dst.RawUnion += src.RawUnion
	for i := range dst.Arms {
		dst.Arms[i].Stable += src.Arms[i].Stable
		dst.Arms[i].Applied += src.Arms[i].Applied
	}
	dst.Common += src.Common
	dst.CommonSame += src.CommonSame
	dst.CommonRetargeted += src.CommonRetargeted
	dst.CommonSemanticChanged += src.CommonSemanticChanged
	dst.CommonLiteralChanged += src.CommonLiteralChanged
	dst.CommonScorerKeyChanged += src.CommonScorerKeyChanged
	dst.CommonRetimed += src.CommonRetimed
	dst.CommonRetargetedRetimed += src.CommonRetargetedRetimed
	dst.ONOnly += src.ONOnly
	dst.OFFOnly += src.OFFOnly
	dst.ONOnlyFraction.Stable += src.ONOnlyFraction.Stable
	dst.ONOnlyFraction.Applied += src.ONOnlyFraction.Applied
	dst.OFFOnlyFraction.Stable += src.OFFOnlyFraction.Stable
	dst.OFFOnlyFraction.Applied += src.OFFOnlyFraction.Applied
	dst.Neither += src.Neither
	dst.Stable00 += src.Stable00
	dst.Stable01 += src.Stable01
	dst.Stable10 += src.Stable10
	dst.Stable11 += src.Stable11
	for status, n := range src.Statuses {
		dst.Statuses[status] += n
	}
	studyAuditRefreshFractions(dst)
}

func studyAuditRefreshFractions(counts *studyAuditCounts) {
	for i := range counts.Arms {
		counts.Arms[i].Defined = counts.Arms[i].Applied > 0
	}
	counts.ONOnlyFraction.Defined = counts.ONOnlyFraction.Applied > 0
	counts.OFFOnlyFraction.Defined = counts.OFFOnlyFraction.Applied > 0
	counts.ExactRateComparison = studyAuditCompareFractions(counts.Arms[1], counts.Arms[2])
}

func studyAuditCompareFractions(off, on studyAuditFraction) string {
	if off.Applied == 0 || on.Applied == 0 {
		return "undefined"
	}
	left, right := on.Stable*off.Applied, off.Stable*on.Applied
	if left > right {
		return "on_higher"
	}
	if left < right {
		return "off_higher"
	}
	return "equal"
}
