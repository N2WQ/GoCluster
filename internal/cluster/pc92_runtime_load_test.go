//go:build qualification

package cluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

// A fixed six-ms schedule independently emits ten duplicates per tick. Q3
// repeats 24s at1k new/s,120s with no new keys,156s at10k/min in each five-minute
// cycle:50k new keys per cycle and continuous100k/min duplicate arrivals.
func qualificationNewPerTick(p qualificationProfile, elapsed time.Duration) int {
	if !p.burst {
		return 1
	}
	within := elapsed % (5 * time.Minute)
	if within < 24*time.Second {
		return 6
	}
	if within < 144*time.Second {
		return 0
	}
	return 1
}

func qualificationFaultMask(p qualificationProfile, elapsed time.Duration) uint64 {
	if !p.burst {
		return 0
	}
	if (elapsed >= 5*time.Minute && elapsed < 8*time.Minute) || (elapsed >= 15*time.Minute && elapsed < 18*time.Minute) {
		return uint64(255) << 56
	}
	return 0
}

func (d *qualificationDriver) load() error {
	var recent [1024]string
	recentCount, recentNext, duplicateNext := 0, 0, 0
	var spotAt, pc92At, pc93At, wwvAt time.Duration
	sampleAt := time.Minute
	fixture := d.topology.Load()
	var priorMask uint64
	for {
		next := min(spotAt, pc92At, pc93At, wwvAt, sampleAt)
		if next >= d.profile.load {
			break
		}
		if err := qualificationWaitContext(d.ctx, time.Until(d.oracle.epoch.Add(next))); err != nil {
			return err
		}
		lag := time.Since(d.oracle.epoch) - next
		if lag > d.maxLag {
			d.maxLag = lag
		}
		if lag > time.Second {
			return fmt.Errorf("producer behind declared schedule by%s", lag)
		}
		mask := qualificationFaultMask(d.profile, next)
		if mask != priorMask {
			fixture.SetUnavailablePeers(mask)
			if mask != 0 {
				d.faultWG.Add(1)
				go func(start time.Duration) {
					defer d.faultWG.Done()
					if err := d.exerciseStalledPeers(start); err != nil {
						d.oracle.fail("fault recovery: %v", err)
					}
				}(next)
			} else {
				state, err := d.service.QualificationSnapshot(d.ctx)
				if err != nil {
					return err
				}
				if state.Established != d.profile.peers || state.Ingress != 262144 {
					return fmt.Errorf("Q3 fault window ended without full recovery: %+v", state)
				}
			}
			priorMask = mask
		}
		if spotAt == next {
			for i := 0; i < qualificationNewPerTick(d.profile, spotAt); i++ {
				all := ^uint64(0)
				if d.profile.peers < 64 {
					all = (uint64(1) << uint(d.profile.peers)) - 1
				}
				id, token, err := d.oracle.add(true, -1, all&^(mask|1), all&^1)
				if err != nil {
					return err
				}
				line := qualificationSpotFrame(id, d.newSpots, token, time.Now().UTC())
				if len(line) > 512 {
					return fmt.Errorf("ordinary spot exceeds512B")
				}
				if err := d.sendPeer(0, line); err != nil {
					return err
				}
				recent[recentNext] = line
				recentNext = (recentNext + 1) % len(recent)
				if recentCount < len(recent) {
					recentCount++
				}
				d.newSpots++
			}
			var batch strings.Builder
			for range 10 {
				if recentCount == 0 {
					return fmt.Errorf("duplicate schedule has no admitted input")
				}
				batch.WriteString(recent[duplicateNext%recentCount])
				duplicateNext++
				d.duplicates++
			}
			if err := d.sendPeer(0, batch.String()); err != nil {
				return err
			}
			spotAt += 6 * time.Millisecond
		}
		if pc92At == next {
			peerIndex, line, err := fixture.Next(fixture.Now(), d.pc92Count)
			if err != nil {
				return err
			}
			if err := d.sendPeer(peerIndex, line); err != nil {
				return err
			}
			d.pc92Count++
			pc92At += 10 * time.Millisecond
		}
		if pc93At == next {
			target, client := "ALL", -1
			if d.pc93Count%2 == 1 {
				client = (d.pc93Count / 2) % 100
				target = fmt.Sprintf("DL%dCAA", client+1)
			}
			_, token, err := d.oracle.add(false, client, 0, 0)
			if err != nil {
				return err
			}
			stamp, err := d.messageStamp.NextAt(fixture.Now())
			if err != nil {
				return err
			}
			line := fmt.Sprintf("PC93^%s^%s^%s^DL1AAA^*^%s^H2^", fixture.MessageOrigin(), stamp, target, token)
			if err := d.sendPeer(0, line); err != nil {
				return err
			}
			d.pc93Count++
			pc93At += 600 * time.Millisecond
		}
		if wwvAt == next {
			_, token, err := d.oracle.add(false, -1, 0, 0)
			if err != nil {
				return err
			}
			now := time.Now().UTC()
			line := fmt.Sprintf("PC23^%s^%02d^100^5^2^%s^DL1AAA^DL1PAA^H2^", now.Format("02-Jan-2006"), now.Hour(), token)
			if err := d.sendPeer(0, line); err != nil {
				return err
			}
			d.wwvCount++
			wwvAt += 3 * time.Second
		}
		if sampleAt == next {
			state, err := d.service.QualificationSnapshot(d.ctx)
			if err != nil {
				return err
			}
			d.samples = append(d.samples, state)
			d.t.Logf("%s minute%d new=%d dup=%d PC92=%d checker_failures=%d", d.profile.name, int(sampleAt/time.Minute), d.newSpots, d.duplicates, d.pc92Count, d.oracle.failures.Load())
			if state.SpotRefused+state.PC92Refused+state.PC93Refused+state.PC93InputRefused+state.BulletinRefused != 0 || state.ClockGated || state.PublicationGated {
				return fmt.Errorf("unexpected capacity/clock gate: %+v", state)
			}
			sampleAt += time.Minute
		}
	}
	return qualificationWaitContext(d.ctx, time.Until(d.oracle.epoch.Add(d.profile.load)))
}

func qualificationSpotFrame(id, spotIndex int, token string, now time.Time) string {
	kind := "PC61"
	if spotIndex%10 >= 4 {
		kind = "PC11"
	}
	if spotIndex%10 >= 8 {
		kind = "PC26"
	}
	mode := "CW"
	freq := 14001.0 + float64((spotIndex/2)%100)*0.6
	if spotIndex%2 == 1 {
		mode = "SSB"
		freq = 14200 + float64((spotIndex/2)%20)*5
	}
	line := fmt.Sprintf("%s^%.1f^%s^%s^%s^%s %s^DL1AAA^DL1PAA^", kind, freq, qualificationDXCall(id), now.Format("02-Jan-2006"), now.Format("1504Z"), token, mode)
	if kind == "PC61" {
		line += "192.0.2.1^"
	}
	if kind == "PC26" {
		line += "^"
	}
	return line + "H10^\r\n"
}

func (d *qualificationDriver) exerciseStalledPeers(start time.Duration) error {
	// The declared three-minute fault envelope includes a one-second pre-stall
	// drain and a final minute for reconnect plus verified ingress restoration.
	if err := qualificationWaitContext(d.ctx, time.Until(d.oracle.epoch.Add(start+time.Second))); err != nil {
		return err
	}
	d.linksMu.RLock()
	old := append([]*qualificationSocket(nil), d.peers[56:64]...)
	d.linksMu.RUnlock()
	for _, link := range old {
		link.expectedClose.Store(true)
		link.paused.Store(true)
	}
	if err := qualificationWaitContext(d.ctx, time.Until(d.oracle.epoch.Add(start+2*time.Minute))); err != nil {
		return err
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(d.cfg.Peering.ListenPort))
	for i, link := range old {
		_ = link.conn.Close()
		link.paused.Store(false)
		<-link.done
		replacement, err := d.connectPeer(address, 56+i)
		if err != nil {
			return err
		}
		d.linksMu.Lock()
		d.peers[56+i] = replacement
		d.all = append(d.all, replacement)
		d.linksMu.Unlock()
		go replacement.read()
	}
	return d.topology.Load().RestoreIngress(d.ctx, d.service, uint64(255)<<56, d.sendPeer)
}

func TestQualificationSpotFixtureAndBurstContract(t *testing.T) {
	for _, mode := range []string{"CW", "SSB"} {
		parsed := spot.ParseSpotComment("QID0000001 "+mode, 14250)
		if parsed.Comment != "QID0000001" {
			t.Fatalf("comment token changed for%s: %+v", mode, parsed)
		}
		s := spot.NewSpot(qualificationDXCall(1), "DL1AAA", 14250, parsed.Mode)
		s.Comment = parsed.Comment
		cloned := s.Clone()
		cloned.DXCall, cloned.DXCallNorm = "DL2RENAMED", "DL2RENAMED"
		if id, ok := qualificationToken(cloned.FormatDXCluster()); !ok || id != 1 {
			t.Fatalf("renamed formatted spot lost correlation: %s", cloned.FormatDXCluster())
		}
	}
	p, err := runtimeQualificationProfile("q3")
	if err != nil {
		t.Fatal(err)
	}
	newKeys := 0
	for at := time.Duration(0); at < p.load; at += 6 * time.Millisecond {
		newKeys += qualificationNewPerTick(p, at)
		if newKeys > 20000+int((at+6*time.Millisecond)/(6*time.Millisecond)) {
			t.Fatal("burst exceeded r*t+20k envelope")
		}
	}
	if newKeys != 300000 {
		t.Fatalf("Q3 total=%d", newKeys)
	}
	if qualificationDXCall(455399) == qualificationDXCall(0) || !spot.IsValidCallsign(qualificationDXCall(455399)) {
		t.Fatal("fixture identity overflow")
	}
}
