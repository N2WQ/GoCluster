//go:build qualification

package qualificationstage

import "testing"

func TestQualificationStageMarkers(t *testing.T) {
	var got Event
	r, ok := Install(func(event Event) { got = event })
	if !ok {
		t.Fatal("first install refused")
	}
	defer Remove(r)
	if _, ok := Install(func(Event) { t.Fatal("replaced observer") }); ok {
		t.Fatal("second install accepted")
	}
	Observe(WorkerStarted, "QID0000003", 2)
	if got.Stage != WorkerStarted || got.Comment != "QID0000003" || got.Worker != 2 {
		t.Fatal(got)
	}
	if !Remove(r) {
		t.Fatal("owner removal refused")
	}
	Observe(PrimaryReady, "QID0000004", -1)
	if got.Comment != "QID0000003" {
		t.Fatal("removed callback invoked")
	}
}
