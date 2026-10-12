// File role: Rejects receiver requirements that an actual legacy history owner
// cannot satisfy. Startup callers select their consumers and supply the store's
// constructor capacity; this check creates no runtime state or YAML defaults.
// Related tests: config/legacy_history_capacity_test.go.
package config

import "fmt"

// LegacyHistoryConsumers identifies the consumers an owner actually runs.
// Replay has no support floor, reverse engineering runs only resolver gates,
// and the separate live FT store supplies only the support floor.
type LegacyHistoryConsumers struct {
	SupportFloor bool
	Stabilizer   bool
	Resolver     bool
}

// ValidateLegacyHistoryCapacity rejects impossible enabled receiver gates for
// a constructed legacy store. The caller supplies its actual positive capacity
// and diagnostic name before creating owners. It preserves raw consumer
// sentinels; normalized YAML values remain the loader's responsibility. FT's
// floor is active independently of global correction. Custom SCP score/cell
// requirements do not apply to this store, but existing receiver-threshold
// folding still applies when a tool passes a CustomSCP-enabled configuration.
func (c CallCorrectionConfig) ValidateLegacyHistoryCapacity(store string, capacity int, consumers LegacyHistoryConsumers) error {
	check := func(key string, requirement int) error {
		if requirement > capacity {
			return fmt.Errorf("%s: effective requirement %d exceeds %s receiver capacity=%d", key, requirement, store, capacity)
		}
		return nil
	}
	commonKey := "call_correction.recent_band_record_min_unique_spotters"
	common := c.RecentBandRecordMinUniqueSpotters
	if common <= 0 {
		common = 2
	}
	if consumers.SupportFloor && c.RecentBandBonusEnabled {
		if err := check(commonKey, common); err != nil {
			return err
		}
	}
	if c.Enabled && consumers.Stabilizer && c.StabilizerEnabled {
		minimum, key := common, commonKey
		if c.CustomSCP.Enabled {
			minimum = max(minimum, c.CustomSCP.StabilizerMinUniqueSpotters)
			key += ", call_correction.custom_scp.stabilizer_min_unique_spotters"
		}
		if err := check(key, minimum); err != nil {
			return err
		}
		if c.StabilizerEditNeighborEnabled {
			editMinimum := c.StabilizerEditNeighborMinSpotters
			if editMinimum <= 0 {
				editMinimum = minimum
			}
			if c.CustomSCP.Enabled {
				editMinimum = max(editMinimum, minimum)
			}
			if err := check("call_correction.stabilizer_edit_neighbor_min_spotters", editMinimum); err != nil {
				return err
			}
		}
	}
	if !c.Enabled || !consumers.Resolver {
		return nil
	}
	distance1Allowed := c.MaxEditDistance < 0 || c.MaxEditDistance >= 1
	distance2Allowed := c.MaxEditDistance < 0 || c.MaxEditDistance >= 2
	validation := (distance1Allowed && customSCPTruncationValidationEnabled(c.FamilyPolicy.Truncation, distance2Allowed)) ||
		(c.BayesBonus.Enabled && ((distance1Allowed && c.BayesBonus.RequireCandidateValidated) ||
			(distance2Allowed && c.BayesBonus.RequireSubjectUnvalidatedDistance2)))
	base, key := c.RecentBandRecordMinUniqueSpotters, commonKey
	if c.CustomSCP.Enabled {
		base = max(base, c.CustomSCP.ResolverMinUniqueSpotters)
		key += ", call_correction.custom_scp.resolver_min_unique_spotters"
	}
	if validation {
		minimum := base
		if minimum <= 0 {
			minimum = 2
		}
		if err := check(key, minimum); err != nil {
			return err
		}
	}
	// Plain/slash identities can differ at distance zero, so Plus1 remains a
	// consumer there. Bayes consumes the report minimum at distance 1/2 even
	// with zero scales. Raw legacy Plus1 zero has no Boolean-default fallback.
	if c.ResolverRecentPlus1Enabled || (c.BayesBonus.Enabled && distance1Allowed) {
		minimum := c.ResolverRecentPlus1MinUniqueWinner
		key = "call_correction.resolver_recent_plus1_min_unique_winner"
		if c.CustomSCP.Enabled {
			minimum = max(minimum, base)
			key += ", " + commonKey + ", call_correction.custom_scp.resolver_min_unique_spotters"
		}
		return check(key, minimum)
	}
	return nil
}
