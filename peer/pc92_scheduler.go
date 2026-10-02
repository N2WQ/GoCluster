package peer

import (
	"context"
	"errors"
	"time"
)

// Scheduling stays on the authority owner. Each turn handles at most one
// lifecycle request, received record or staged record, rotating ready classes.
// Real service time, not a queued ticker timestamp, drives due work. Individual
// graph transactions and consistent diagnostic captures remain indivisible.
func (p *protocolController) run(ctx context.Context) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	defer p.retireReplays()
	var nextPublish, nextMaintenance time.Time
	class, maintenance := 0, 0
	for {
		if ctx.Err() != nil {
			return
		}
		p.consumeWake()
		now := p.elapsedNow()
		if !now.Before(nextPublish) {
			p.tick(now)
			nextPublish = now.Add(membershipServiceInterval)
		}
		if !now.Before(nextMaintenance) {
			p.maintain(maintenance, p.elapsedNow())
			maintenance = (maintenance + 1) % 6
			nextMaintenance = p.elapsedNow().Add(25 * time.Millisecond)
		}
		worked := false
		for range 3 {
			turn := class
			class = (class + 1) % 3
			switch turn {
			case 0:
				select {
				case req := <-p.lifecycle:
					p.dispatch(req)
					worked = true
				default:
				}
			case 1:
				select {
				case work := <-p.input:
					p.consumeInput(work)
					worked = true
				default:
				}
			case 2:
				worked = p.serviceReplay()
			}
			if worked {
				break
			}
		}
		if worked {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.wake:
			p.membershipWake()
		case req := <-p.lifecycle:
			p.dispatch(req)
			class = 1
		case work := <-p.input:
			p.consumeInput(work)
			class = 2
		}
	}
}

func (p *protocolController) consumeWake() {
	select {
	case <-p.wake:
		p.membershipWake()
	default:
	}
}

// A wake never moves the next publication service point. Unrelated churn or
// requests therefore cannot restart the ordinary membership allowance.
func (p *protocolController) membershipWake() {
	p.dirty = true
	p.drainFailures()
}

func (p *protocolController) dispatch(req protocolRequest) {
	err := p.request(req)
	if !errors.Is(err, errReplayPending) {
		req.done <- err
	}
}

func (p *protocolController) consumeInput(work protocolInput) {
	p.queueMu.Lock()
	p.queued[work.class]--
	p.bytes[work.class] -= work.charge
	p.queueMu.Unlock()
	if f, err := ParseFrame(work.wire); err == nil {
		p.receive(f, work.source, work.at)
	}
}

// Splitting the six bounded owners gives due publication/cancellation a service
// point between full-cache expiry cohorts and graph/projection work. At the
// qualified load each cache is revisited in150ms plus measured owner work,
// leaving margin within the existing one-second cleanup requirement.
func (p *protocolController) maintain(phase int, now time.Time) {
	switch phase {
	case 0:
		p.pc92.prune(now)
	case 1:
		p.pc93.prune(now)
	case 2:
		p.manager.dedupe.prune(now)
	case 3:
		p.manager.bulletinDedupe.prune(now)
	case 4:
		wall := p.authorityWallNow().UTC()
		safe := !p.clockGate && wall.After(p.lastExpiryWall) && p.timestamps.ClockSafe(wall) == nil
		if wall.After(p.lastExpiryWall) {
			p.lastExpiryWall = wall
		}
		p.graph.expire(p.qualificationAuthorityTime(now), safe, p.directNodes())
	case 5:
		p.project(now)
		p.sampleStats()
	}
}
