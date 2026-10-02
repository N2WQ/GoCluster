package peer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func stagedAllocationFixture(t *testing.T, size int) (*Frame, *PC92Record) {
	t.Helper()
	prefix, suffix := "PC92^N2AAA^43200^C^5N2AAA:", "^H1^"
	frame, err := ParseFrame(prefix + strings.Repeat("1", size-len(prefix)-len(suffix)) + suffix)
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodePC92(frame)
	if err != nil {
		t.Fatal(err)
	}
	return frame, record
}

func TestPC92StagingChargesBackingWithinIndividualAndGlobalBounds(t *testing.T) {
	for _, tc := range []struct {
		name, boundary                     string
		wireSize, candidates, perCandidate int
	}{
		{"individual count", "count", 128, 1, 257},
		{"individual bytes", "bytes", 2016, 1, 256},
		{"global count", "count", 128, 33, 256},
		{"global bytes", "bytes", 2016, 128, 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _, _, _ := controllerTestOwner(t)
			m := p.manager
			frame, record := stagedAllocationFixture(t, tc.wireSize)
			var owners []*session
			refused := false
			for i := 0; i < tc.candidates && !refused; i++ {
				s := &session{remoteCall: fmt.Sprintf("N%dPEER", i)}
				m.candidates.Set(s, &candidateState{})
				owners = append(owners, s)
				for j := 0; j < tc.perCandidate; j++ {
					if err := m.stagePC92Record(s, frame, record); err != nil {
						refused = true
						break
					}
				}
			}
			if !refused {
				t.Fatal("fixture did not exercise its staging capacity refusal")
			}
			var totalRecords, totalBytes int
			for _, c := range m.candidates.All() {
				bytes := pointerAllocationBytes(cap(c.staged) * 16)
				for _, wire := range c.staged {
					bytes += allocationBytes(len(wire))
				}
				if c.bytes != bytes || bytes > 512<<10 || len(c.staged) > 256 {
					t.Fatalf("candidate backing is uncharged or over cap: %d/%d records=%d", c.bytes, bytes, len(c.staged))
				}
				totalBytes += bytes
				totalRecords += len(c.staged)
			}
			if totalRecords != m.stagedRecords || totalBytes != m.stagedBytes || totalBytes > 16<<20 || totalRecords > 8192 {
				t.Fatalf("global staging reservation mismatch: %d records %d bytes", totalRecords, totalBytes)
			}
			if tc.boundary == "bytes" && totalRecords == tc.candidates*tc.perCandidate {
				t.Fatal("byte pressure reached only the record count limit")
			}
			if p.graph.nodes.Len() != 0 || p.graph.freshness.Len() != 0 {
				t.Fatal("staging pressure acquired global authority")
			}
			for _, s := range owners {
				m.releaseCandidate(s)
			}
			if m.stagedRecords != 0 || m.stagedBytes != 0 || m.candidates.Len() != 0 {
				t.Fatal("candidate release retained primary storage or its reservation")
			}
		})
	}
}

func TestPC92StagingReservationFollowsActiveEstablishmentDrain(t *testing.T) {
	p, source, destination, _ := controllerTestOwner(t)
	m := p.manager
	m.candidates.Set(source, &candidateState{})
	frame, err := ParseFrame(fmt.Sprintf("PC92^N2AAA^%d^C^5N2AAA^1K1USER^H2^", utcSecond(time.Now())))
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodePC92(frame)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.stagePC92Record(source, frame, record); err != nil {
		t.Fatal(err)
	}
	reserved := m.stagedBytes
	// Block the real forwarding handoff after candidate removal. The active
	// owner must still reserve this batch while other candidates can be added.
	destination.queueMu.Lock()
	var unlock sync.Once
	done := make(chan struct{})
	var establishmentErr error
	go func() {
		reply := make(chan error, 1)
		establishmentErr = p.request(protocolRequest{kind: "establish", source: source, done: reply})
		if errors.Is(establishmentErr, errReplayPending) {
			p.serviceReplay()
			establishmentErr = <-reply
		}
		close(done)
	}()
	defer func() {
		unlock.Do(destination.queueMu.Unlock)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("establishment drain did not terminate")
		}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.RLock()
		_, pending := m.candidates.Get(source)
		bytes, records := m.stagedBytes, m.stagedRecords
		m.mu.RUnlock()
		if !pending {
			if bytes != reserved || records != 1 {
				t.Fatal("active staged batch lost its reservation before drain completion")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("controller did not transfer candidate ownership")
		}
		time.Sleep(time.Millisecond)
	}
	unlock.Do(destination.queueMu.Unlock)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("establishment drain did not finish after forwarding resumed")
	}
	if establishmentErr != nil || m.stagedBytes != 0 || m.stagedRecords != 0 {
		t.Fatalf("completed drain retained reservation: err=%v bytes=%d records=%d", establishmentErr, m.stagedBytes, m.stagedRecords)
	}
}
