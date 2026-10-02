//go:build qualification

package cluster

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestQualificationMissingExamplesPacketBound(t *testing.T) {
	rows := make([]qualificationRecipientResult, 100)
	for i := range rows {
		r := &rows[i]
		r.Name = fmt.Sprintf("client-%d", i)
		r.Required, r.Received, r.MissingEnqueue = 9_999_999, 9_999_983, 16
		r.Cohorts = make([]qualificationCohort, 46)
		for j := range r.Cohorts {
			r.Cohorts[j] = qualificationCohort{InputMinute: j - 1, RequiredSpots: 9_999_999,
				Enqueue: qualificationLatency{Count: 9_999_999, P99UpperMS: 1024000, Over5MS: 9_999_999, Over25MS: 9_999_999,
					UncertaintyCross5MS: 9_999_999, UncertaintyCross25MS: 9_999_999}}
		}
		for id := 9_999_984; id <= 9_999_999; id++ {
			r.MissingEnqueueExamples = append(r.MissingEnqueueExamples, qualificationMissingExample{ID: id, Spot: true, DXCall: qualificationDXCall(id)})
		}
	}
	var wire bytes.Buffer
	if err := qualificationWritePacket(&wire, qualificationReply{Enqueue: rows}); err != nil {
		t.Fatalf("bounded diagnostics exceed unchanged RPC limit: %v", err)
	}
	t.Logf("maximal diagnostic reply payload=%d limit=%d", wire.Len()-4, qualificationRPCMaxBytes)
	var reply qualificationReply
	if err := qualificationReadPacket(&wire, &reply); err != nil || !reflect.DeepEqual(rows, reply.Enqueue) {
		t.Fatalf("maximal diagnostic reply did not survive bounded RPC: %v", err)
	}
}

func TestQualificationMissingExamplesPreserveCounts(t *testing.T) {
	for _, tc := range []struct {
		name          string
		spot          bool
		read, enqueue bool
		missingRead   int
		missingQueue  int
	}{
		{"spot-both", true, false, false, 1, 1},
		{"spot-read", true, false, true, 1, 0},
		{"spot-enqueue", true, true, false, 0, 1},
		{"message-read", false, false, false, 1, 0},
		{"message-complete", false, true, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newQualificationOracle(1, 1, 0, 1)
			o.epoch = time.Now()
			_, _, err := o.add(tc.spot, -1, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			at := o.epoch.Add(time.Duration(o.inputs[0].started.Load()-1) + time.Millisecond)
			if tc.read {
				o.observedID(o.clients[0], 0, qualificationDXCall(0), at, false)
			}
			if tc.enqueue {
				o.observedID(o.clients[0], 0, qualificationDXCall(0), at, true)
			}
			got := o.results(false)[0]
			if got.Required != 1 || got.Received != 1-tc.missingRead || got.MissingRead != tc.missingRead || got.MissingEnqueue != tc.missingQueue {
				t.Fatalf("changed delivery accounting: %+v", got)
			}
			want := qualificationMissingExample{ID: 0, Spot: tc.spot}
			if tc.spot {
				want.DXCall = "DL1AAAAAAAA"
			}
			for _, pair := range []struct {
				examples []qualificationMissingExample
				count    int
			}{{got.MissingReadExamples, tc.missingRead}, {got.MissingEnqueueExamples, tc.missingQueue}} {
				if len(pair.examples) != pair.count || (pair.count != 0 && pair.examples[0] != want) {
					t.Fatalf("missing diagnostic changed: got %+v want %+v count=%d", pair.examples, want, pair.count)
				}
			}
			if (o.failures.Load() != 0) != (tc.missingRead+tc.missingQueue != 0) {
				t.Fatalf("diagnostics changed failure verdict: %+v", got)
			}
		})
	}
}

func TestQualificationMissingExamplesBoundAndRecipients(t *testing.T) {
	const inputs = 40
	o := newQualificationOracle(inputs, 2, 2, 1)
	o.epoch = time.Now()
	for range inputs {
		if _, _, err := o.add(true, 0, 2, 2); err != nil {
			t.Fatal(err)
		}
	}
	rows := o.results(false)
	for _, index := range []int{0, 3} {
		row := rows[index]
		if row.Required != inputs || row.Received != 0 || row.MissingRead != inputs || len(row.MissingReadExamples) != 16 {
			t.Fatalf("cap hid missing required inputs: %+v", row)
		}
		for id, example := range row.MissingReadExamples {
			if example.ID != id || !example.Spot || example.DXCall != qualificationDXCall(id) {
				t.Fatalf("examples are not first ordered omissions: %+v", row)
			}
		}
	}
	if rows[0].MissingEnqueue != inputs || len(rows[0].MissingEnqueueExamples) != 16 || rows[3].MissingEnqueue != 0 || len(rows[3].MissingEnqueueExamples) != 0 {
		t.Fatal("enqueue ownership or cap changed")
	}
	for _, index := range []int{1, 2} {
		if row := rows[index]; row.Required != 0 || row.MissingRead != 0 || row.MissingEnqueue != 0 || len(row.MissingReadExamples)+len(row.MissingEnqueueExamples) != 0 {
			t.Fatalf("invented an unrequired recipient: %+v", row)
		}
	}
	if o.failures.Load() != 2 {
		t.Fatalf("cap changed failure count: %d", o.failures.Load())
	}
}

func TestQualificationMissingExamplesRemoteOwnership(t *testing.T) {
	for _, omit := range []bool{false, true} {
		parent := newQualificationOracle(3, 1, 0, 1)
		parent.epoch = time.Now()
		for _, spot := range []bool{true, false, true} {
			if _, _, err := parent.add(spot, -1, 0, 0); err != nil {
				t.Fatal(err)
			}
		}
		child := newQualificationOracleInputs(parent.inputs, 1, 0, 1)
		child.epoch = parent.epoch
		for id := range parent.used {
			at := parent.epoch.Add(time.Duration(parent.inputs[id].started.Load()-1) + time.Millisecond)
			parent.observedID(parent.clients[0], id, qualificationDXCall(id), at, false)
			if parent.inputs[id].spot && (!omit || id != 2) {
				child.observedID(child.clients[0], id, qualificationDXCall(id), at, true)
			}
		}
		rows, err := child.enqueueResults(parent.used)
		if err != nil {
			t.Fatal(err)
		}
		var wire bytes.Buffer
		if err := qualificationWritePacket(&wire, qualificationReply{Enqueue: rows}); err != nil {
			t.Fatal(err)
		}
		var reply qualificationReply
		if err := qualificationReadPacket(&wire, &reply); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rows, reply.Enqueue) {
			t.Fatal("RPC changed child-owned examples")
		}
		if err := parent.acceptEnqueueResults(reply); err != nil {
			t.Fatal(err)
		}
		got := parent.results(true)[0]
		if got.MissingRead != 0 || !reflect.DeepEqual(got.MissingEnqueueExamples, rows[0].MissingEnqueueExamples) || (got.MissingEnqueue != 0) != omit || (parent.failures.Load() != 0) != omit {
			t.Fatalf("parent empty enqueue bitset replaced child evidence: %+v", got)
		}
		if omit && (got.MissingEnqueue != 1 || len(got.MissingEnqueueExamples) != 1 || got.MissingEnqueueExamples[0].ID != 2) {
			t.Fatalf("lost exact child omission: %+v", got)
		}
	}
}

func TestQualificationMissingExamplesRejectInvalidRemote(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*qualificationRecipientResult)
	}{
		{"over-cap", func(r *qualificationRecipientResult) {
			r.MissingEnqueueExamples = append(r.MissingEnqueueExamples, r.MissingEnqueueExamples[0])
		}},
		{"missing-example", func(r *qualificationRecipientResult) { r.MissingEnqueueExamples = r.MissingEnqueueExamples[:15] }},
		{"duplicate-id", func(r *qualificationRecipientResult) { r.MissingEnqueueExamples[1] = r.MissingEnqueueExamples[0] }},
		{"unordered-id", func(r *qualificationRecipientResult) {
			r.MissingEnqueueExamples[0], r.MissingEnqueueExamples[1] = r.MissingEnqueueExamples[1], r.MissingEnqueueExamples[0]
		}},
		{"negative-id", func(r *qualificationRecipientResult) { r.MissingEnqueueExamples[0].ID = -1 }},
		{"undeclared-id", func(r *qualificationRecipientResult) { r.MissingEnqueueExamples[15].ID = 25 }},
		{"nonspot-field", func(r *qualificationRecipientResult) { r.MissingEnqueueExamples[0].Spot = false }},
		{"wrong-call", func(r *qualificationRecipientResult) { r.MissingEnqueueExamples[0].DXCall = "DL1OTHER" }},
		{"message-id", func(r *qualificationRecipientResult) {
			r.MissingEnqueueExamples[15] = qualificationMissingExample{ID: 20}
		}},
		{"wrong-recipient", func(r *qualificationRecipientResult) {
			r.MissingEnqueueExamples[15] = qualificationMissingExample{ID: 21, Spot: true, DXCall: qualificationDXCall(21)}
		}},
		{"unpublished-id", func(r *qualificationRecipientResult) {
			r.MissingEnqueueExamples[15] = qualificationMissingExample{ID: 22, Spot: true, DXCall: qualificationDXCall(22)}
		}},
		{"child-read-evidence", func(r *qualificationRecipientResult) {
			r.MissingReadExamples = []qualificationMissingExample{r.MissingEnqueueExamples[0]}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newQualificationOracle(26, 1, 0, 1)
			o.epoch = time.Now()
			for id := range 23 {
				client := -1
				if id == 21 {
					client = 1
				}
				if _, _, err := o.add(id != 20, client, 0, 0); err != nil {
					t.Fatal(err)
				}
			}
			child := newQualificationOracleInputs(o.inputs, 1, 0, 1)
			rows, err := child.enqueueResults(o.used)
			if err != nil {
				t.Fatal(err)
			}
			o.inputs[22].started.Store(0)
			tc.mutate(&rows[0])
			if err := o.acceptEnqueueResults(qualificationReply{Enqueue: rows}); err == nil {
				t.Fatal("invalid child evidence accepted")
			}
			if o.remoteEnqueue != nil {
				t.Fatal("invalid child evidence changed remote ownership")
			}
		})
	}
}
