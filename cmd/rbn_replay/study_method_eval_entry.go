// File role: Detect only the explicit experimental evaluation CLI surface.
// This is separate from both prediction replay and legacy activation readers.
package main

import "strings"

func studyMethodEvaluationCLIFlag(argument string) bool {
	if !strings.HasPrefix(argument, "-") {
		return false
	}
	name := strings.TrimPrefix(strings.TrimPrefix(argument, "-"), "-")
	name, _, _ = strings.Cut(name, "=")
	return name == "study-method-eval-plan" || name == "study-method-eval-freeze" || name == "study-method-eval-out"
}
