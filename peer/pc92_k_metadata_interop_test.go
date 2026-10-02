package peer

import (
	"fmt"
	"testing"
	"time"
)

func TestDXSpiderReferenceKSubjectNumericReplacement(t *testing.T) {
	for _, tc := range kNumericReplacementCases() {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := startDXReference(t, false, "N0CALL")
			now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			result := r.frameAt("PC92^N0CALL^43200^K^5N0CALL:5457:633^0^0^H99^", now)
			referenceField(t, result.RouteNodes["N0CALL"], "version", "5457")
			referenceField(t, result.RouteNodes["N0CALL"], "build", "633")
			result = r.frameAt("PC92^N0CALL^43201^K^5N0CALL"+tc.suffix+"^0^0^H99^", now.Add(time.Second))
			referenceField(t, result.RouteNodes["N0CALL"], "version", tc.version)
			referenceField(t, result.RouteNodes["N0CALL"], "build", tc.build)
			result = r.frameAt("PC92^N0CALL^43202^K^5N0CALL:5459:635^0^0^H99^", now.Add(2*time.Second))
			referenceField(t, result.RouteNodes["N0CALL"], "version", "5459")
			referenceField(t, result.RouteNodes["N0CALL"], "build", "635")
		})
	}
}

func TestDXSpiderReferenceSubjectNumericActionIsolation(t *testing.T) {
	for _, action := range []string{"A", "C", "D"} {
		for _, subject := range []string{"5N0CALL", "5N0CALL:0:0", ""} {
			t.Run(action+"/"+subject, func(t *testing.T) {
				r, _ := startDXReference(t, false, "N0CALL")
				now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
				result := r.frameAt("PC92^N0CALL^43200^C^5N0CALL:5457:633^1K1USER^H99^", now)
				referenceField(t, result.RouteNodes["N0CALL"], "version", "5457")
				referenceField(t, result.RouteNodes["N0CALL"], "build", "633")
				result = r.frameAt(fmt.Sprintf("PC92^N0CALL^43201^%s^%s^1K1USER^H99^", action, subject), now.Add(time.Second))
				referenceField(t, result.RouteNodes["N0CALL"], "version", "5457")
				referenceField(t, result.RouteNodes["N0CALL"], "build", "633")
			})
		}
	}
	t.Run("K_absent_IP", func(t *testing.T) {
		r, _ := startDXReference(t, false, "N0CALL")
		now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
		result := r.frameAt("PC92^N0CALL^43200^A^^5N3NODE:5457:633:192.0.2.1^H99^", now, "N3NODE")
		referenceField(t, result.RouteNodes["N3NODE"], "ip", "192.0.2.1")
		result = r.frameAt("PC92^N3NODE^43201^K^5N3NODE:5457:633^0^0^H99^", now.Add(time.Second), "N3NODE")
		referenceField(t, result.RouteNodes["N3NODE"], "build", "633")
		result = r.frameAt("PC92^N3NODE^43202^K^5N3NODE^0^0^H99^", now.Add(2*time.Second), "N3NODE")
		referenceField(t, result.RouteNodes["N3NODE"], "version", "0")
		referenceField(t, result.RouteNodes["N3NODE"], "build", "0")
		referenceField(t, result.RouteNodes["N3NODE"], "ip", "192.0.2.1")
	})
}
