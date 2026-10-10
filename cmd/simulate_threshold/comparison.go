package main

import "fmt"

type thresholdComparison struct {
	applied, rejected, unknown              int
	retained, removed, added, unknownPassed int
	baselines                               map[thresholdSettings]int
	addedByDistance                         map[int]int
	unknownOutcomes                         map[string]int
	addedSamples                            []decisionRecord
}

// Maps live for the invocation and have at most one key per selected row;
// distance keys are limited by the loader to 1..3. Samples retain at most ten.
func compareThresholds(decisions []decisionRecord, settings thresholdSettings) thresholdComparison {
	r := thresholdComparison{baselines: make(map[thresholdSettings]int),
		addedByDistance: make(map[int]int), unknownOutcomes: make(map[string]int)}
	for i := range decisions {
		d := &decisions[i]
		r.baselines[thresholdSettings{d.minReports, d.minAdvantage, d.minConfidence}]++
		passes := checkThresholds(d, settings.reports, settings.advantage, settings.confidence)
		switch classifyDecision(d.decision) {
		case "applied":
			r.applied++
			if passes {
				r.retained++
			} else {
				r.removed++
			}
		case "rejected":
			r.rejected++
			if passes {
				r.added++
				r.addedByDistance[d.distance]++
				if len(r.addedSamples) < 10 {
					r.addedSamples = append(r.addedSamples, *d)
				}
			}
		default:
			r.unknown++
			label := "<NULL>"
			if d.decision.Valid {
				label = fmt.Sprintf("%q", d.decision.String)
			}
			r.unknownOutcomes[label]++
			if passes {
				r.unknownPassed++
			}
		}
	}
	return r
}
