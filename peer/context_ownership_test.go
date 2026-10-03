package peer

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestV15ContextParentsStableAcrossChurn(t *testing.T) {
	for _, limit := range []int{1, 64} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			m := newProtocolTestManager(t)
			m.cfg.MaxPeers = limit // initialize exactly the selected owner-pool boundary
			if err := m.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			parents := make([]context.Context, len(m.contextOwners))
			if len(parents) != limit+128 {
				t.Fatalf("parents=%d want=%d", len(parents), limit+128)
			}
			for i := range parents {
				parents[i] = m.contextOwners[i].parent
			}
			for cycle := range 20 {
				operations := make([]*contextOperation, len(parents))
				for i := range operations {
					operations[i] = m.beginContextOperation()
					if operations[i] == nil || operations[i].owner.parent != parents[i] {
						t.Fatalf("cycle %d slot %d changed permanent parent", cycle, i)
					}
				}
				if m.beginContextOperation() != nil {
					t.Fatal("context operation exceeded transport owner capacity")
				}
				for _, operation := range operations {
					m.endContextOperation(operation)
					if operation.ctx.Err() != context.Canceled || operation.owner.active {
						t.Fatal("slot was released before cancellation")
					}
				}
			}
			m.Stop()
			for _, parent := range parents {
				if parent.Err() != context.Canceled {
					t.Fatal("Stop retained an uncanceled permanent parent")
				}
			}
		})
	}
}

func TestV15OutboundContextPreservesParentContract(t *testing.T) {
	type valueKey struct{}
	cause := errors.New("parent canceled by test")
	deadline := time.Now().Add(time.Minute)
	parent, deadlineCancel := context.WithDeadline(context.WithValue(t.Context(), valueKey{}, "retained"), deadline)
	defer deadlineCancel()
	parent, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	m := newProtocolTestManager(t)
	if err := m.Start(parent); err != nil {
		t.Fatal(err)
	}
	operation := m.beginContextOperation()
	if operation == nil {
		t.Fatal("operation refused")
	}
	gotDeadline, ok := operation.ctx.Deadline()
	if !ok || !gotDeadline.Equal(deadline) || operation.ctx.Value(valueKey{}) != "retained" {
		t.Fatal("operation lost manager parent value or deadline")
	}
	if operation.ctx == operation.owner.parent {
		t.Fatal("operation did not own an explicit root")
	}
	cancel(cause)
	if operation.ctx.Err() != context.Canceled || context.Cause(operation.ctx) != cause { //nolint:errorlint // Assert the exact inherited cause; matching an error chain would weaken this ownership test.
		t.Fatal("operation lost manager parent cancellation cause")
	}
	m.endContextOperation(operation)
}
