// File role: Enforces spot age at core acceptance and final output admission.
// Related docs: spot/README.md#live-stabilizer-output, docs/decisions/ADR-0341-stabilizer-live-output.md.
// Related tests: internal/cluster/output_age_test.go.
package cluster

import "time"

// rejectExpiredOutput samples the age clock at this boundary. An expired held
// path returns immediately, so its fixed drop counter increments once even when
// it crossed several earlier checks. Zero/future timestamps retain isStale's
// existing sentinel behavior; the ingress gate still owns its original rules.
func (p *outputPipeline) rejectExpiredOutput(ctx *outputSpotContext) bool {
	now := time.Now().UTC()
	if p.outputAgeNow != nil {
		now = p.outputAgeNow()
	}
	if !isStaleAt(ctx.spot, p.spotPolicy, now) {
		return false
	}
	if ctx.stabilizerHeld && p.tracker != nil {
		p.tracker.IncrementStabilizerAgeDropped()
	}
	return true
}
