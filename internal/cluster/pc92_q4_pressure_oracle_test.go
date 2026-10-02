//go:build qualification

package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"dxcluster/peer"
)

func q4BytePressureEvidence() []peer.QualificationTransportState {
	return []peer.QualificationTransportState{
		{Call: "DL8PAA", DataCount: 128, ControlCount: 127, ActiveBytes: 65538, DataBytes: 1002816, ControlBytes: 1046654, DataCapacity: 128},
		{Call: "DL1PAA", DataCount: 127, ControlCount: 127, ActiveBytes: 8194, DataBytes: 1043198, ControlBytes: 1046654, DataCapacity: 128},
	}
}

func TestQ4TransportPressureCapturedEvidence(t *testing.T) {
	d := &q4Runtime{peers: make([]*q4Socket, 2)}
	for _, test := range []struct {
		name      string
		change    func([]peer.QualificationTransportState) []peer.QualificationTransportState
		countOnly bool
		ready     bool
		hard      bool
	}{
		{name: "captured-Q4A-byte-population", ready: true},
		{name: "captured-Q4B-full-count-small-bytes", change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].DataBytes = 929952
			return s
		}},
		{name: "missing-active-write", change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ActiveBytes = 0
			return s
		}},
		{name: "too-few-large-active-writes", change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ActiveBytes = 8194
			return s
		}},
		{name: "control-byte-target", change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ControlBytes = 999999
			return s
		}},
		{name: "lost-owner", hard: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState { return s[:1] }},
		{name: "data-quota-overflow", hard: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].DataBytes = 1<<20 + 1
			return s
		}},
		{name: "control-quota-overflow", hard: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ControlBytes = 1<<20 + 1
			return s
		}},
		{name: "active-quota-overflow", hard: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ActiveBytes = 65539
			return s
		}},
		{name: "data-count-overflow", hard: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].DataCount = 129
			return s
		}},
		{name: "control-count-overflow", hard: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ControlCount = 129
			return s
		}},
		{name: "count-lane-125-minimum-128-maximum", countOnly: true, ready: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ControlCount = 128
			s[1].DataCount, s[1].ControlCount = 125, 125
			for i := range s {
				s[i].DataBytes, s[i].ControlBytes = 20000, 24000
			}
			return s
		}},
		{name: "count-lane-124-insufficient", countOnly: true, change: func(s []peer.QualificationTransportState) []peer.QualificationTransportState {
			s[0].ControlCount = 128
			s[1].DataCount = 124
			return s
		}},
		{name: "count-lane-no-128-maximum", countOnly: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			states := q4BytePressureEvidence()
			if test.change != nil {
				states = test.change(states)
			}
			if err := d.validateTransportBounds(states); (err != nil) != test.hard {
				t.Fatalf("hard-boundary classification: err=%v want hard=%v", err, test.hard)
			}
			if err := d.validateTransportPressure(states, test.countOnly); (err == nil) != test.ready {
				t.Fatalf("pressure readiness: err=%v want ready=%v", err, test.ready)
			}
		})
	}
}

func TestQ4TransportPressureAdmissionBarrier(t *testing.T) {
	t.Run("wait-before-refill", func(t *testing.T) {
		d := &q4Runtime{ctx: t.Context(), peers: make([]*q4Socket, 2)}
		calls := 0
		err := d.awaitTransportPressure(time.Now().Add(time.Second), false, func() []peer.QualificationTransportState {
			calls++
			states := q4BytePressureEvidence()
			switch calls {
			case 1:
				states[0].DataBytes = 929952
			case 2:
				states[0].ActiveBytes = 0
			}
			return states
		})
		if err != nil || calls != 3 {
			t.Fatalf("refill was released before all populations were observed: calls=%d err=%v", calls, err)
		}
	})
	for _, name := range []string{"late-success", "canceled-before-sampling", "canceled-during-sampling", "owner-loss", "hard-overflow", "incomplete-timeout"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			d := &q4Runtime{ctx: ctx, peers: make([]*q4Socket, 2)}
			deadline := time.Now().Add(time.Second)
			wantErr, wantCalls := error(nil), 1
			switch name {
			case "canceled-before-sampling":
				cancel()
				wantErr, wantCalls = context.Canceled, 0
			case "canceled-during-sampling":
				wantErr = context.Canceled
			case "late-success", "incomplete-timeout":
				deadline = time.Now().Add(10 * time.Millisecond)
				wantErr = context.DeadlineExceeded
			}
			calls := 0
			err := d.awaitTransportPressure(deadline, false, func() []peer.QualificationTransportState {
				calls++
				states := q4BytePressureEvidence()
				switch name {
				case "late-success":
					time.Sleep(max(0, time.Until(deadline)) + time.Millisecond)
				case "canceled-during-sampling":
					cancel()
				case "owner-loss":
					return states[:1]
				case "hard-overflow":
					states[0].DataBytes = 1<<20 + 1
				case "incomplete-timeout":
					states[0].DataBytes = 929952
				}
				return states
			})
			if err == nil || wantErr != nil && !errors.Is(err, wantErr) {
				t.Fatalf("barrier accepted invalid evidence or wrong failure: %v", err)
			}
			if name != "incomplete-timeout" && calls != wantCalls {
				t.Fatalf("hard failure/cancellation retried: calls=%d want%d", calls, wantCalls)
			}
		})
	}
}
