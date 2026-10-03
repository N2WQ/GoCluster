//go:build qualification

package dedup

import (
	"testing"
	"time"

	"dxcluster/internal/qualificationstage"
	"dxcluster/spot"
)

func TestQualificationStageMarkers(t *testing.T) {
	count := 0
	r, ok := qualificationstage.Install(func(event qualificationstage.Event) {
		count++
		if event.Stage != qualificationstage.PrimaryReady || event.Comment != "QID0000000" || event.Worker != -1 {
			t.Error(event)
		}
	})
	if !ok {
		t.Fatal("observer occupied")
	}
	defer qualificationstage.Remove(r)
	d := NewDeduplicator(time.Minute, false, 4)
	s := spot.NewSpot("K1ABC", "DL1AAA", 14020, "CW")
	s.Comment = "QID0000000"
	d.processSpot(s)
	d.processSpot(s)
	if count != 1 || len(d.outputChan) != 1 {
		t.Fatalf("accepted/duplicate markers=%d output=%d", count, len(d.outputChan))
	}
}
