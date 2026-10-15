package pskreporter

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"dxcluster/spot"
)

// This unchanged-behavior harness is copied verbatim into baseline/candidate.
// The controller freezes one recent timestamp for all pairs and profiles;
// the historical parser-only epoch would be rejected by handlePayload's age
// check. No worker, network connection or production clock seam is involved.
func BenchmarkPSKRReportPresenceControls(b *testing.B) {
	for _, test := range []struct {
		name, mode string
		frequency  int64
		compat     bool
		pathOnly   bool
	}{
		{"FT8_fast", "FT8", 14074000, false, false},
		{"FT8_compat", "FT8", 14074000, true, false},
		{"WSPR_fast", "WSPR", 14097000, false, true},
		{"WSPR_compat", "WSPR", 14097000, true, true},
	} {
		b.Run(test.name, func(b *testing.B) {
			raw := os.Getenv("GOCLUSTER_PSKR_ZERO_BENCH_UNIX")
			if raw == "" {
				b.Skip("qualification requires one controller-frozen recent GOCLUSTER_PSKR_ZERO_BENCH_UNIX")
			}
			observed, err := strconv.ParseInt(raw, 10, 64)
			now := time.Now().Unix()
			if err != nil || observed <= 0 || observed > now || now-observed >= 3600 {
				b.Fatalf("control timestamp must be positive, not future and less than one hour old: %q", raw)
			}
			sender := `"K1ABC"`
			if test.compat {
				sender = `"K1\u0041BC"`
			}
			payload := []byte(fmt.Sprintf(`{"f":%d,"md":%q,"rp":-7,"t":%d,"sc":%s,"sl":"FN42","rc":"N0CALL","rl":"EM10"}`,
				test.frequency, test.mode, observed, sender))
			if _, fast := parsePSKRPayloadFast(payload); fast == test.compat {
				b.Fatalf("control parser path fast=%t, compat=%t", fast, test.compat)
			}
			client := NewClient("localhost", 1883, nil, "", 1, 0, 0, 0, nil, nil, false, 2, 0)
			var output, opposite <-chan *spot.Spot = client.spotChan, client.pathOnlyChan
			if test.pathOnly {
				output, opposite = client.pathOnlyChan, client.spotChan
			}
			var completed int
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for range b.N {
				client.handlePayload(payload)
				var got *spot.Spot
				select {
				case got = <-output:
				default:
					b.Fatal("nonzero control did not reach its intended route")
				}
				if got == nil || !got.HasReport || got.Report != -7 || got.DXCall != "K1ABC" || got.DECall != "N0CALL" ||
					got.Mode != test.mode || got.ModeNorm != test.mode || got.Frequency != float64(test.frequency)/1000 ||
					got.ObservedFrequency != 0 || !got.Time.Equal(time.Unix(observed, 0)) ||
					got.DXMetadata.Grid != "FN42" || got.DEMetadata.Grid != "EM10" ||
					got.SourceType != spot.SourcePSKReporter || got.SourceNode != "PSKREPORTER" || got.IsHuman ||
					got.ModeProvenance != spot.ModeProvenanceSourceExplicit || got.Comment != "" ||
					len(output) != 0 || len(opposite) != 0 {
					b.Fatalf("control tuple/route changed: %+v", got)
				}
				completed++
			}
			b.StopTimer()
			if completed != b.N || client.spotDropCounter.Total() != 0 || client.pathOnlyDropCounter.Total() != 0 || client.parseErrorCounter.Total() != 0 {
				b.Fatalf("incomplete control work: completed=%d want=%d normal drops=%d path-only drops=%d parse errors=%d", completed, b.N,
					client.spotDropCounter.Total(), client.pathOnlyDropCounter.Total(), client.parseErrorCounter.Total())
			}
			b.ReportMetric(float64(completed), "reports")
			b.Logf("PSKR report control mode=%s compat=%t path_only=%t report=-7 observation_unix=%d completed=%d payload_sha256=%x",
				test.mode, test.compat, test.pathOnly, observed, completed, sha256.Sum256(payload))
		})
	}
}
