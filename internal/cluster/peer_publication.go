// File role: Owns immutable original payloads for locally generated peer spots.
// Records follow existing bounded output holds; local corrections never replace
// their fields. Incoming native relay owns its separate original frame copy.
package cluster

import (
	"time"

	"dxcluster/spot"
)

// originalPeerPublication owns copied field values, not a Spot pointer. Strings
// are immutable: replacing local fields cannot alter this accepted-source tuple.
// Temporal, FT and stabilizer slots own its lifetime; no separate index or queue
// retains it. The primary-normalized input is captured before output preparation.
type originalPeerPublication struct {
	dxCall, deCall, comment, spotterIP string
	frequency                          float64
	stamp                              time.Time
	source                             spot.SourceType
	isTestSpotter                      bool
}

func captureOriginalPeerPublication(s *spot.Spot) *originalPeerPublication {
	if s == nil {
		return nil
	}
	return &originalPeerPublication{
		dxCall: s.DXCall, deCall: s.DECall, comment: s.Comment, spotterIP: s.SpotterIP,
		frequency: s.Frequency, stamp: s.Time, source: s.SourceType, isTestSpotter: s.IsTestSpotter,
	}
}

// publicationSpot is a private synchronous adapter for the existing peer API.
// It carries no local metadata, confidence, formatting caches or mutation state.
func (s *originalPeerPublication) publicationSpot() spot.Spot {
	return spot.Spot{
		DXCall: s.dxCall, DECall: s.deCall, Frequency: s.frequency, Time: s.stamp,
		Comment: s.comment, SpotterIP: s.spotterIP, SourceType: s.source, IsTestSpotter: s.isTestSpotter,
	}
}
