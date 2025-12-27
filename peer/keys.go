package peer

import (
	"fmt"

	"dxcluster/spot"
)

// Purpose: Build dedupe keys for peer frames and spots.
// Key aspects: Encodes frame type and relevant identifiers for uniqueness.
// Upstream: Peer dedupe caches.
// Downstream: fmt.Sprintf.
func pc92Key(f *Frame) string {
	return fmt.Sprintf("pc92:%s:%s", f.Type, f.Raw)
}

// Purpose: Build a dedupe key for a DX spot frame.
// Key aspects: Uses DX/DE/frequency/time to preserve ordering.
// Upstream: Peer dedupe caches for DX frames.
// Downstream: fmt.Sprintf.
func dxKey(f *Frame, s *spot.Spot) string {
	return fmt.Sprintf("dx:%s:%s:%s:%.1f:%d", f.Type, s.DXCall, s.DECall, s.Frequency, s.Time.Unix())
}

// Purpose: Build a dedupe key for WWV/WCY frames.
// Key aspects: Uses raw frame content.
// Upstream: Peer dedupe caches.
// Downstream: fmt.Sprintf.
func wwvKey(f *Frame) string {
	return fmt.Sprintf("wwv:%s:%s", f.Type, f.Raw)
}

// Purpose: Build a dedupe key for PC93 announcement frames.
// Key aspects: Uses raw frame content.
// Upstream: Peer dedupe caches.
// Downstream: fmt.Sprintf.
func pc93Key(f *Frame) string {
	return fmt.Sprintf("pc93:%s:%s", f.Type, f.Raw)
}

// Purpose: Build a dedupe key for PC26 raw passthrough frames.
// Key aspects: Uses raw frame content.
// Upstream: Peer dedupe caches.
// Downstream: fmt.Sprintf.
func pc26Key(f *Frame) string {
	return fmt.Sprintf("pc26:%s", f.Raw)
}
