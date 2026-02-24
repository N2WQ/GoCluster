package main

import (
	"fmt"
	"sort"
	"strings"

	"dxcluster/spot"
	"dxcluster/stats"

	"github.com/dustin/go-humanize"
)

func formatResolverSummaryFromMetrics(metrics spot.SignalResolverMetrics) string {
	agreementPercent := 0
	if metrics.DecisionsComparable > 0 {
		agreementPercent = int((metrics.DecisionAgreement * 100) / metrics.DecisionsComparable)
	}
	return fmt.Sprintf(
		"Resolver: %s (C) / %s (P) / %s (U) / %s (S) | agr %s/%s (%d%%) | d %s (SP) / %s (DW) / %s (UC) | q=%d drop %s (Q) / %s (K) / %s (C) / %s (R) | pressure %s (C) / %s (R) evict %s (C) / %s (R) hw %s (C) / %s (R)",
		humanize.Comma(int64(metrics.StateConfident)),
		humanize.Comma(int64(metrics.StateProbable)),
		humanize.Comma(int64(metrics.StateUncertain)),
		humanize.Comma(int64(metrics.StateSplit)),
		humanize.Comma(int64(metrics.DecisionAgreement)),
		humanize.Comma(int64(metrics.DecisionsComparable)),
		agreementPercent,
		humanize.Comma(int64(metrics.DisagreeSplitCorrected)),
		humanize.Comma(int64(metrics.DisagreeConfidentDifferentWinner)),
		humanize.Comma(int64(metrics.DisagreeUncertainCorrected)),
		metrics.QueueDepth,
		humanize.Comma(int64(metrics.DropQueueFull)),
		humanize.Comma(int64(metrics.DropMaxKeys)),
		humanize.Comma(int64(metrics.DropMaxCandidates)),
		humanize.Comma(int64(metrics.DropMaxReporters)),
		humanize.Comma(int64(metrics.CapPressureCandidates)),
		humanize.Comma(int64(metrics.CapPressureReporters)),
		humanize.Comma(int64(metrics.EvictedCandidates)),
		humanize.Comma(int64(metrics.EvictedReporters)),
		humanize.Comma(int64(metrics.HighWaterCandidates)),
		humanize.Comma(int64(metrics.HighWaterReporters)),
	)
}

func formatCorrectionDecisionSummary(tracker *stats.Tracker) string {
	if tracker == nil {
		return "CorrGate: n/a"
	}
	total := tracker.CorrectionDecisionTotal()
	applied := tracker.CorrectionDecisionApplied()
	rejected := tracker.CorrectionDecisionRejected()
	fallback := tracker.CorrectionFallbackApplied()
	prior := tracker.CorrectionPriorBonusUsed()
	reasons := formatTopCounterSummary(tracker.CorrectionDecisionReasons(), 2)
	paths := formatTopCounterSummary(tracker.CorrectionDecisionPaths(), 2)
	return fmt.Sprintf("CorrGate: %s (T) / %s (A) / %s (R) / %s (FB) / %s (PB) [%s] [%s]",
		humanize.Comma(int64(total)),
		humanize.Comma(int64(applied)),
		humanize.Comma(int64(rejected)),
		humanize.Comma(int64(fallback)),
		humanize.Comma(int64(prior)),
		reasons,
		paths,
	)
}

func formatTopCounterSummary(counts map[string]uint64, limit int) string {
	if len(counts) == 0 {
		return "none"
	}
	type pair struct {
		key   string
		count uint64
	}
	items := make([]pair, 0, len(counts))
	for k, v := range counts {
		items = append(items, pair{key: k, count: v})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count != items[j].count {
			return items[i].count > items[j].count
		}
		return items[i].key < items[j].key
	})
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, fmt.Sprintf("%s:%s", items[i].key, humanize.Comma(int64(items[i].count))))
	}
	return strings.Join(out, " ")
}
