//go:build qualification

package cluster

import (
	"fmt"
	"time"

	"dxcluster/peer"
)

// Cache fill is ordinary socket traffic and cache expiration remains real.
// Small batches wait for admission, avoiding a synthetic mailbox overflow.
func (d *q4Runtime) fillCaches(full bool) error {
	classes := [...]string{"spot", "pc92", "pc93", "bulletin"}
	targets := [4]int{131072, 65536, 65536, 8192}
	if !full {
		targets[0] -= 512
		targets[1] -= 512
	}
	deadline := time.Now().Add(8 * time.Minute)
	for class, target := range targets {
		for {
			before := d.r.peerManager.QualificationCacheCounts()[class]
			if before >= target {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("Q4 %s cache fill deadline: got%d want%d", classes[class], before, target)
			}
			count := min(32, target-before)
			for range count {
				size, hop := 0, 1
				if class == 0 {
					hop = 2
				} else if class == 2 {
					size = 128
				} else if class == 3 {
					size = 256
				}
				wire, err := d.generator.Wire(classes[class], d.sequence[class], size, hop)
				if err != nil {
					return err
				}
				d.sequence[class]++
				if err := d.peers[0].write(wire); err != nil {
					return err
				}
			}
			until := time.Now().Add(3 * time.Second)
			for d.r.peerManager.QualificationCacheCounts()[class] < before+count && time.Now().Before(until) {
				if err := qualificationWaitContext(d.ctx, time.Millisecond); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *q4Runtime) validateTransportPressure(states []peer.QualificationTransportState, countOnly bool) error {
	if len(states) != len(d.peers) {
		return fmt.Errorf("Q4 pressure lost established owners: got%d want%d", len(states), len(d.peers))
	}
	maxControl, maxData, largeActive := 0, 0, 0
	for _, state := range states {
		if state.ActiveBytes == 0 || state.ControlCount > 128 || state.DataCount > state.DataCapacity || state.ControlBytes > 1<<20 || state.DataBytes > 1<<20 {
			return fmt.Errorf("Q4 transport ownership boundary: %+v", state)
		}
		maxControl = max(maxControl, state.ControlCount)
		maxData = max(maxData, state.DataCount)
		if state.ActiveBytes >= 65536 {
			largeActive++
		}
		if countOnly {
			if state.DataCount < 125 || state.ControlCount < 125 {
				return fmt.Errorf("Q4 count-pressure population too low: %+v", state)
			}
		} else if state.DataBytes < 1000000 || state.ControlBytes < 1000000 {
			return fmt.Errorf("Q4 byte-pressure population too low: %+v", state)
		}
	}
	if countOnly && (maxControl != 128 || maxData != 128) {
		return fmt.Errorf("Q4 count limits not reached: control%d data%d", maxControl, maxData)
	}
	if !countOnly && largeActive < len(d.peers)-1 {
		return fmt.Errorf("Q4 near-maximum active writes got%d want%d", largeActive, len(d.peers)-1)
	}
	return nil
}

// The latch delays actual writes within their original2s deadline. All queue
// records are admitted through real peer frames; source exclusion determines
// the reported per-peer occupancy rather than an invented counter target.
func (d *q4Runtime) queuePressure(countOnly bool) (func(), error) {
	calls := make([]string, len(d.peers))
	for i := range calls {
		calls[i] = d.r.cfg.Peering.Peers[i].RemoteCallsign
	}
	release, err := d.r.peerManager.QualificationHoldWrites(d.ctx, calls)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			release()
		}
	}()
	size := 8128
	if countOnly {
		size = 128
	}
	// One near-maximum active write on every recipient except its ingress.
	activeSize := 65536
	if countOnly {
		activeSize = size
	}
	wire, err := d.generator.Wire("pc92", d.sequence[1], activeSize, 2)
	if err != nil {
		return nil, err
	}
	d.sequence[1]++
	if err := d.peers[0].write(wire); err != nil {
		return nil, err
	}
	until := time.Now().Add(time.Second)
	for {
		active := 0
		for _, state := range d.r.peerManager.QualificationTransports() {
			if state.ActiveBytes > 0 {
				active++
			}
		}
		if active >= len(d.peers)-1 {
			break
		}
		if time.Now().After(until) {
			return nil, fmt.Errorf("Q4 active-write population got%d want%d", active, len(d.peers)-1)
		}
		if err := qualificationWaitContext(d.ctx, time.Millisecond); err != nil {
			return nil, err
		}
	}
	// Two balanced rounds leave most recipients with127 queued records.
	// A small third record allows count-only lanes to reach128; byte-bound
	// lanes stop earlier because channel backing shares their1MiB quota.
	control := 129
	if countOnly {
		control++
	}
	for class := range 2 {
		n := control
		if class == 0 {
			n = 130
		}
		for i := range n {
			kind := "pc92"
			if class == 0 {
				kind = "spot"
			}
			wire, err := d.generator.Wire(kind, d.sequence[class], size, 2)
			if err != nil {
				return nil, err
			}
			d.sequence[class]++
			if err := d.peers[i%len(d.peers)].write(wire); err != nil {
				return nil, err
			}
		}
	}
	failed = false
	return release, nil
}
