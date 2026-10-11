package cluster

import (
	"testing"
	"time"
	"unsafe"

	"dxcluster/spot"
)

var peerPublicationRecordSink *originalPeerPublication
var peerPublicationSpotSink spot.Spot

// Capture owns one compact record regardless of local metadata/cache size;
// adapting that record must not allocate a cloned Spot or formatting state.
func BenchmarkOriginalPeerPublication(b *testing.B) {
	s := spot.NewSpot("K1ABD", "N0AAA", 14020.13, "CW")
	s.SourceType, s.Comment, s.SpotterIP = spot.SourceManual, "  original comment  ", "2001:db8::1"
	s.Time = time.Date(2026, 10, 11, 6, 7, 0, 0, time.UTC)
	original := captureOriginalPeerPublication(s)
	b.Run("capture", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(unsafe.Sizeof(originalPeerPublication{})), "record-bytes")
		for i := 0; i < b.N; i++ {
			peerPublicationRecordSink = captureOriginalPeerPublication(s)
		}
	})
	b.Run("synchronous_adapter", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			peerPublicationSpotSink = original.publicationSpot()
		}
	})
}
