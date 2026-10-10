package main

import (
	"fmt"
	"strings"
)

type distance3Scenario struct {
	name                                    string
	extraReports, extraAdvantage, extraConf int
}

type distance3Comparison struct {
	retained, removed, added, unknownPassed int
}

// These three scalar predicates are hypothetical, not a replay of other gates.
// Unknown outcomes stay separate from recognized rejected rows even if they pass.
func compareDistance3(cases []distance3Case, scenario distance3Scenario) distance3Comparison {
	var result distance3Comparison
	for _, c := range cases {
		passes := c.winnerSupport >= 3+scenario.extraReports &&
			c.winnerSupport-c.subjectSupport >= 1+scenario.extraAdvantage &&
			c.winnerConfidence >= 60+scenario.extraConf
		switch classifyDistance3(c.decision) {
		case "applied":
			if passes {
				result.retained++
			} else {
				result.removed++
			}
		case "rejected":
			if passes {
				result.added++
			}
		default:
			if passes {
				result.unknownPassed++
			}
		}
	}
	return result
}

func writeDistance3Scenarios(b *strings.Builder, cases []distance3Case) {
	fmt.Fprintln(b, "\nHYPOTHETICAL SCALAR GATE COMPARISONS")
	fmt.Fprintln(b, "Assumed bases: 3 winner reports, advantage 1, logged score 60; listed extras are added.")
	fmt.Fprintln(b, "These assumptions are not loaded current settings; other correction gates are not replayed.")
	fmt.Fprintln(b, "Stored counts/scores are assumed valid. Unknown outcomes are excluded from recognized totals and net changes.")
	s := summarizeDistance3(cases)
	scenarios := []distance3Scenario{
		{"Assumed baseline", 0, 1, 5},
		{"Lower advantage", 0, 0, 5},
		{"Lower score", 0, 1, 3},
		{"Lower both", 0, 0, 3},
		{"Extra report", 1, 1, 5},
		{"Extra report and higher advantage/score", 1, 2, 7},
	}
	for _, scenario := range scenarios {
		c := compareDistance3(cases, scenario)
		fmt.Fprintf(b, "\n%s: extras reports=%d, advantage=%d, score=%d\n",
			scenario.name, scenario.extraReports, scenario.extraAdvantage, scenario.extraConf)
		fmt.Fprintf(b, "  Retained applied: %d; removed applied: %d; added rejected: %d\n", c.retained, c.removed, c.added)
		fmt.Fprintf(b, "  Recognized rows passing: %d/%d (%s); net change from applied: %+d\n",
			c.retained+c.added, s.applied+s.rejected,
			distance3Percent(c.retained+c.added, s.applied+s.rejected), c.added-c.removed)
		fmt.Fprintf(b, "  Unknown rows passing (separate): %d/%d\n", c.unknownPassed, s.unknown)
	}
}
