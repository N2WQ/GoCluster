// File role: Validates Custom SCP receiver capacity before runtime or replay
// creates owners. This contract also applies to callers that bypass YAML loading.
// Related docs: docs/decisions/ADR-0288-historical-requirements-respect-storage-capacity.md.
// Related tests: config/call_correction_capacity_test.go,
// internal/cluster/custom_scp_capacity_delivery_test.go,
// cmd/rbn_replay/custom_scp_consumer_test.go.
package config

import "fmt"

// ValidateCustomSCPCapacity rejects enabled history requirements that cannot be
// satisfied by one Custom SCP key. It preserves each consumer's existing raw
// defaults and does not constrain score units or the separate FT history store.
// Ownership: this is a pure startup check; it creates no runtime state.
func (c CallCorrectionConfig) ValidateCustomSCPCapacity() error {
	if !c.Enabled || !c.CustomSCP.Enabled {
		return nil
	}
	capacity := c.CustomSCP.MaxSpottersPerKey
	if capacity <= 0 {
		// Match the store constructor's algorithm bound for unnormalized callers.
		capacity = 64
	}
	check := func(key string, requirement int) error {
		if requirement > capacity {
			return fmt.Errorf("%s: effective requirement %d exceeds call_correction.custom_scp.max_spotters_per_key=%d", key, requirement, capacity)
		}
		return nil
	}
	// Dedicated floors remain consumers even with all optional history rails off.
	for _, floor := range [...]struct {
		key                   string
		requirement, fallback int
	}{
		{"s_floor_min_unique_spotters_exact", c.CustomSCP.SFloorMinUniqueSpottersExact, 2},
		{"s_floor_min_unique_h3_cells_exact", c.CustomSCP.SFloorMinUniqueH3CellsExact, 2},
		{"s_floor_min_unique_spotters_family", c.CustomSCP.SFloorMinUniqueSpottersFamily, 2},
		{"s_floor_min_unique_h3_cells_family", c.CustomSCP.SFloorMinUniqueH3CellsFamily, 3},
	} {
		minimum := floor.requirement
		if minimum <= 0 {
			minimum = floor.fallback
		}
		if err := check("call_correction.custom_scp."+floor.key, minimum); err != nil {
			return err
		}
	}
	distance1Allowed := c.MaxEditDistance < 0 || c.MaxEditDistance >= 1
	distance2Allowed := c.MaxEditDistance < 0 || c.MaxEditDistance >= 2
	validation := (distance1Allowed && customSCPTruncationValidationEnabled(c.FamilyPolicy.Truncation, distance2Allowed)) ||
		(c.BayesBonus.Enabled && ((distance1Allowed && c.BayesBonus.RequireCandidateValidated) ||
			(distance2Allowed && c.BayesBonus.RequireSubjectUnvalidatedDistance2)))
	// Plain/slash identities can differ at distance zero, so Plus1 remains a
	// consumer there. Bayes requires distance 1/2; truncation requires at least 1.
	counts := c.ResolverRecentPlus1Enabled || (c.BayesBonus.Enabled && distance1Allowed)
	if counts || validation {
		if err := check("call_correction.custom_scp.resolver_min_unique_h3_cells", c.CustomSCP.ResolverMinUniqueH3Cells); err != nil {
			return err
		}
	}
	base := max(c.RecentBandRecordMinUniqueSpotters, c.CustomSCP.ResolverMinUniqueSpotters)
	baseKeys := "call_correction.recent_band_record_min_unique_spotters, call_correction.custom_scp.resolver_min_unique_spotters"
	if validation {
		minimum := base
		if minimum <= 0 {
			minimum = 2
		}
		if err := check(baseKeys, minimum); err != nil {
			return err
		}
	}
	if counts {
		// Bayes still uses this report gate and publishes counts with zero scales.
		minimum := max(base, c.ResolverRecentPlus1MinUniqueWinner)
		if err := check(baseKeys+", call_correction.resolver_recent_plus1_min_unique_winner", minimum); err != nil {
			return err
		}
	}
	if c.StabilizerEnabled {
		common := c.RecentBandRecordMinUniqueSpotters
		if common <= 0 {
			common = 2
		}
		minimum := max(common, c.CustomSCP.StabilizerMinUniqueSpotters)
		if err := check("call_correction.recent_band_record_min_unique_spotters, call_correction.custom_scp.stabilizer_min_unique_spotters", minimum); err != nil {
			return err
		}
		if err := check("call_correction.custom_scp.stabilizer_min_unique_h3_cells", c.CustomSCP.StabilizerMinUniqueH3Cells); err != nil {
			return err
		}
		if c.StabilizerEditNeighborEnabled {
			if err := check("call_correction.stabilizer_edit_neighbor_min_spotters", max(minimum, c.StabilizerEditNeighborMinSpotters)); err != nil {
				return err
			}
		}
	}
	return nil
}

func customSCPTruncationValidationEnabled(t CallCorrectionTruncationFamilyConfig, distance2Allowed bool) bool {
	if !t.Enabled || (!t.AllowPrefixMatch && !t.AllowSuffixMatch) {
		return false
	}
	delta := t.MaxLengthDelta
	if delta <= 0 {
		delta = 1
	}
	return (t.RelaxAdvantage.Enabled && (t.RelaxAdvantage.RequireCandidateValidated || t.RelaxAdvantage.RequireSubjectUnvalidated)) ||
		(t.LengthBonus.Enabled && t.LengthBonus.Max > 0 && (t.LengthBonus.RequireCandidateValidated || t.LengthBonus.RequireSubjectUnvalidated)) ||
		(distance2Allowed && t.Delta2Rails.Enabled && delta >= 2 && (t.Delta2Rails.RequireCandidateValidated || t.Delta2Rails.RequireSubjectUnvalidated))
}
