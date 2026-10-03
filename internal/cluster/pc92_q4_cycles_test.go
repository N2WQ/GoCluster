//go:build qualification

package cluster

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"

	"dxcluster/peer"
)

func (s *q4Socket) fragment(data string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	_, err := io.WriteString(s.conn, data)
	return err
}

func (d *q4Runtime) cycle(plan peer.QualificationStagingPlan) error {
	var candidates []*q4Socket
	defer func() { q4Close(candidates) }()
	for range 128 {
		call, password := "", ""
		if d.phase == "b" {
			configured := d.r.cfg.Peering.Peers[63]
			call, password = configured.RemoteCallsign, configured.Password
		}
		s, err := d.open(call, password, false)
		if err != nil {
			return err
		}
		candidates = append(candidates, s)
	}
	before, err := d.await(func(s peer.QualificationState) bool { return s.Pending == 128 && s.Established == len(d.peers) }, 5*time.Second)
	if err != nil {
		return err
	}
	value, accepted, present, err := d.r.peerManager.QualificationOriginWatermark(d.ctx, "N0AAAA")
	if err != nil {
		return err
	}
	cycle := q4Cycle{Name: plan.Name, AtSeconds: time.Since(d.started).Seconds(), Before: before}
	if d.phase == "b" {
		wire, err := peer.QualificationStagingWire(d.topology.Now(), plan.WireBytes)
		if err != nil {
			return err
		}
		for i, n := range plan.Records {
			for range n {
				if err := candidates[i].write(wire); err != nil {
					return err
				}
			}
		}
		if plan.TailBytes > 0 {
			tail, err := peer.QualificationStagingWire(d.topology.Now(), plan.TailBytes)
			if err != nil {
				return err
			}
			if err := candidates[0].write(tail); err != nil {
				return err
			}
		}
		cycle.Filled, err = d.await(func(s peer.QualificationState) bool {
			return s.Pending == 128 && s.StagedRecords == plan.ExpectedRecords && s.StagedBytes == plan.ExpectedBytes
		}, 5*time.Second)
		if err != nil {
			return fmt.Errorf("%s: %w", plan.Name, err)
		}
	} else {
		cycle.Filled = before
	}
	gotValue, gotAccepted, gotPresent, err := d.r.peerManager.QualificationOriginWatermark(d.ctx, "N0AAAA")
	if err != nil {
		return err
	}
	cycle.WatermarkUnchanged = value == gotValue && accepted == gotAccepted && present == gotPresent
	if !cycle.WatermarkUnchanged || cycle.Filled.Nodes != before.Nodes || cycle.Filled.Users != before.Users || cycle.Filled.Edges != before.Edges || cycle.Filled.Ingress != before.Ingress {
		return fmt.Errorf("%s: unestablished candidates changed global authority", plan.Name)
	}
	var releaseWrites func()
	if d.pressure != 0 {
		releaseWrites, err = d.queuePressure(d.pressure == 2)
		if err != nil {
			return err
		}
		defer releaseWrites()
		if err := d.fillCaches(true); err != nil {
			return err
		}
	}
	cycle.Pressured, cycle.PressureProcessHeapBytes, cycle.PressureRuntimeStacks, err = d.readerPressure(candidates)
	if err != nil {
		return err
	}
	if err := q4EnabledOwnership(cycle.Pressured); err != nil {
		return err
	}
	if releaseWrites != nil {
		cycle.Transports = d.r.peerManager.QualificationTransports()
		releaseWrites()
		if cycle.Pressured.Established != len(d.peers) || cycle.Pressured.SpotKeys != 131072 || cycle.Pressured.PC92Keys != 65536 || cycle.Pressured.PC93Keys != 65536 || cycle.Pressured.BulletinKeys != 8192 {
			return fmt.Errorf("Q4 simultaneous cache/transport pressure missed its owned population: %+v", cycle.Pressured)
		}
		if err := d.validateTransportPressure(cycle.Transports, d.pressure == 2); err != nil {
			return err
		}
		d.lastFull, d.pressure = time.Now(), 0
		d.pressureWindows++
	}
	switch {
	case plan.CompleteRace:
		var writers sync.WaitGroup
		for _, s := range candidates {
			writers.Add(1)
			go func() { defer writers.Done(); _ = s.write("PC20^") }()
		}
		writers.Wait()
		if _, err := d.await(func(s peer.QualificationState) bool {
			return s.Established == 64 && s.Pending == 0 && s.StagedBytes == 0 && s.StagedRecords == 0
		}, 5*time.Second); err != nil {
			return err
		}
		newValue, newAccepted, newPresent, err := d.r.peerManager.QualificationOriginWatermark(d.ctx, "N0AAAA")
		if err != nil {
			return err
		}
		if !newPresent || newValue == value && newAccepted == accepted {
			return fmt.Errorf("winner did not acquire staged freshness authority")
		}
	case plan.AwaitDeadline:
		wait := time.Duration(d.r.cfg.Peering.Timeouts.InitSeconds) * time.Second
		if d.phase == "a" {
			wait = time.Duration(d.r.cfg.Peering.Timeouts.LoginSeconds) * time.Second
		}
		if _, err := d.await(func(s peer.QualificationState) bool {
			return s.Pending == 0 && s.StagedRecords == 0 && s.StagedBytes == 0
		}, wait+5*time.Second); err != nil {
			return err
		}
	case plan.OverflowCandidate >= 0:
		wire, err := peer.QualificationStagingWire(d.topology.Now(), 128)
		if err != nil {
			return err
		}
		if err := candidates[plan.OverflowCandidate].write(wire); err != nil {
			return err
		}
		if _, err := d.await(func(s peer.QualificationState) bool { return s.Pending == 127 && s.Established == len(d.peers) }, 5*time.Second); err != nil {
			return err
		}
	}
	q4Close(candidates)
	cycle.Released, err = d.await(func(s peer.QualificationState) bool {
		return s.Pending == 0 && s.Established == len(d.peers) && s.StagedRecords == 0 && s.StagedBytes == 0
	}, 5*time.Second)
	if err != nil {
		return err
	}
	d.report.Cycles = append(d.report.Cycles, cycle)
	return nil
}

// Maximum unterminated wire is read by each real owned session. No reader
// deadline is disabled or renewed. Completing PC99 drops the harmless unknown
// record; prelogin candidates are closed without submitting an invalid login.
func (d *q4Runtime) readerPressure(candidates []*q4Socket) (peer.QualificationState, uint64, uint64, error) {
	partial := "PC99^" + strings.Repeat("x", 65536-len("PC99^"))
	for _, s := range d.peers {
		if err := s.fragment(partial); err != nil {
			return peer.QualificationState{}, 0, 0, err
		}
	}
	for _, s := range candidates {
		if err := s.fragment(partial); err != nil {
			return peer.QualificationState{}, 0, 0, err
		}
	}
	target := int64((len(d.peers) + len(candidates)) * (65536 + 4096))
	state, err := d.await(func(s peer.QualificationState) bool { return s.ReaderBackingBytes >= target }, 5*time.Second)
	if err != nil {
		return state, 0, 0, fmt.Errorf("full reader backing: %w", err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	for _, s := range d.peers {
		if err := s.fragment("~"); err != nil {
			return state, memory.HeapAlloc, memory.StackInuse, err
		}
	}
	if d.phase == "b" {
		for _, s := range candidates {
			if err := s.fragment("~"); err != nil {
				return state, memory.HeapAlloc, memory.StackInuse, err
			}
		}
	}
	return state, memory.HeapAlloc, memory.StackInuse, nil
}
