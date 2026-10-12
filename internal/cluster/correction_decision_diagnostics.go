// File role: Projects selected resolver evaluations onto the existing bounded
// call stream. Recognition and delivery occur later and are not event claims.
package cluster

import (
	"fmt"
	"log"
	"strings"

	"dxcluster/spot"
	"dxcluster/ui"
)

// Empty reason keeps ordinary unchanged/no-snapshot traffic silent. Construct
// the message only for a selected decision, after any accepted identity change.
func emitResolverDecision(dash ui.Surface, entry *spot.Spot, snapshot spot.ResolverSnapshot, input, proposed, reason string, suppressed bool) {
	if reason == "" {
		return
	}
	call := normalizedDXCall(entry)
	outcome, basis, share := "rejected", "unavailable", "--%"
	if snapshot.EvidenceLoss {
		outcome, basis = "retained", "unavailable_loss"
	} else {
		if !strings.EqualFold(call, input) {
			outcome = "corrected"
		}
		if entry.ConfidencePercentOK {
			share = fmt.Sprintf("%d%%", entry.ConfidencePercent)
			// A known zero numerator keeps the population's weighted basis.
			switch snapshot.EffectiveSupportBasis() {
			case spot.ResolverSupportBasisWeighted:
				basis = "weighted"
			case spot.ResolverSupportBasisReceivers:
				basis = "receivers"
			}
		}
	}
	if proposed == "" {
		proposed = "--"
	}
	message := fmt.Sprintf("Call decision: phase=evaluation outcome=%s reason=%s input=%s call=%s proposed=%s at=%.1fkHz share=%s basis=%s recognition=separate suppressed=%t",
		outcome, reason, input, call, proposed, entry.Frequency, share, basis, suppressed)
	if dash != nil {
		dash.AppendCall(message)
	} else {
		log.Print(message)
	}
}
