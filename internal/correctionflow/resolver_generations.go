// File role: Keeps fixed-size key lifecycle witnesses on temporal proposals.
// Current eligible publications own correction authority; saved rows/counts do not.
package correctionflow

import "dxcluster/spot"

// mergeResolverGenerationWitnesses unions the decoded window's influencing
// lifecycles. Conflicting nonzero IDs cannot be collapsed into a newer witness.
// Production selections share their request key; incompatible anchors abstain.
func mergeResolverGenerationWitnesses(dst *ResolverPrimarySelection, src ResolverPrimarySelection) {
	if dst == nil {
		return
	}
	dst.generationConflict = dst.generationConflict || src.generationConflict
	for slot, generation := range src.generationWitnesses {
		if generation == 0 {
			continue
		}
		if src.Snapshot.Key.Band == "" || src.Snapshot.Key.Mode == "" || dst.Snapshot.Key != src.Snapshot.Key {
			dst.generationConflict = true
			continue
		}
		if previous := dst.generationWitnesses[slot]; previous != 0 && previous != generation {
			dst.generationConflict = true
			continue
		}
		dst.generationWitnesses[slot] = generation
	}
}

// Even a bucket that the latest selector now excludes can have influenced the
// decoded proposal. Check its captured lifecycle, not only current contributors.
// Lookup also denies logically retired cells before delayed physical sweeping.
func resolverGenerationWitnessesCurrentDiagnostics(resolver *spot.SignalResolver, key spot.ResolverSignalKey, saved ResolverPrimarySelection, diagnostics *TemporalReleaseDiagnostics) bool {
	if diagnostics != nil {
		diagnostics.ExpectedGenerationWitnesses = saved.generationWitnesses
		for i := range saved.generationWitnesses {
			diagnostics.GenerationChecks[i].Expected = saved.generationWitnesses[i]
		}
	}
	if saved.generationConflict {
		if diagnostics != nil {
			diagnostics.GenerationConflict = true
		}
		return false
	}
	for slot, generation := range saved.generationWitnesses {
		if diagnostics != nil {
			diagnostics.GenerationChecks[slot].Expected = generation
		}
		if generation == 0 {
			if diagnostics != nil {
				diagnostics.GenerationChecks[slot].Status = "no_witness"
			}
			continue
		}
		if saved.Snapshot.Key != key || resolver == nil {
			if diagnostics != nil {
				diagnostics.GenerationChecks[slot].Status = "invalid_anchor"
			}
			return false
		}
		witnessKey := key
		witnessKey.Bucket += int64(slot - 2)
		current, ok := resolver.Lookup(witnessKey)
		if diagnostics != nil {
			check := &diagnostics.GenerationChecks[slot]
			diagnostics.budget.take(256)
			check.Key = cloneDiagnosticKey(&diagnostics.budget, witnessKey)
			check.Status = "read"
			check.Present = ok
			check.Current = current.Generation
		}
		if !ok || current.Generation != generation {
			return false
		}
	}
	return true
}
