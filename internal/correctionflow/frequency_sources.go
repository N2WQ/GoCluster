// File role: Read current eligible frequency evidence without advancing owners.
// Temporal retention copies only a fixed presence/lifecycle vector. Every new
// assessment rereads rows; lifecycle equality is never a cached-count guarantee.
package correctionflow

import (
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// ReadFrequencySources is a separate actual read, not a reconstructed earlier
// selector cut. It performs no Step/drain and promises no atomic multi-key cut.
func ReadFrequencySources(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, now time.Time) [5]FrequencySource {
	var sources [5]FrequencySource
	if resolver == nil {
		return sources
	}
	radius := 0
	if cfg.ResolverNeighborhoodEnabled {
		radius = min(2, max(1, cfg.ResolverNeighborhoodBucketRadius))
	}
	for offset := -radius; offset <= radius; offset++ {
		requested := key
		requested.Bucket += int64(offset)
		snapshot, present := resolver.LookupEligible(requested, now)
		sources[offset+2] = FrequencySourceFromSnapshot(requested, snapshot, true, present)
	}
	return sources
}

// FrequencyLifecycleVector retains no strings, publications or receiver rows.
// Read/present bits distinguish an unread slot, a missing key and generation0.
type FrequencyLifecycleVector struct {
	ReadMask, PresentMask uint8
	Generations           [5]uint64
}

func FrequencySourceLifecycles(sources [5]FrequencySource) FrequencyLifecycleVector {
	var vector FrequencyLifecycleVector
	for slot := range sources {
		s := &sources[slot]
		if s.Read {
			vector.ReadMask |= 1 << slot
		}
		if s.Present {
			vector.PresentMask |= 1 << slot
			vector.Generations[slot] = s.Snapshot.Generation
		}
	}
	return vector
}
