//go:build !qualification

package cluster

import (
	"bufio"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/spot"
)

// The arrival-side production stage must synchronously deliver the expired
// groups before returning with the current report held. The existing P2 fixture
// supplies actual TCP/DIAG and peer recipients, sealed storage and reopening.
func TestP4FTExpiryDeliveredBeforeLateArrival(t *testing.T) {
	t.Chdir(t.TempDir())
	previous := spot.CurrentDXClusterLayout()
	if err := spot.SetDXClusterLineLength(78); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := spot.SetDXClusterLineLength(previous.LineLength); err != nil {
			t.Error(err)
		}
	})
	for _, floor := range []bool{false, true} {
		name, newGlyph := "floor_disabled", "?"
		if floor {
			name, newGlyph = "ft_recent_floor_preserved", "S"
		}
		t.Run(name, func(t *testing.T) {
			cfg := config.CallCorrectionConfig{RecentBandBonusEnabled: floor, RecentBandWindowSeconds: 12 * 60 * 60, RecentBandRecordMinUniqueSpotters: 2}
			f := newAmbiguityDeliveryFixture(t, nil, cfg)
			p4FTEnableReceiver(t, f.normalConn, f.normalReader)
			p4FTEnableReceiver(t, f.diagConn, f.diagReader)
			p := f.pipeline
			p.ftConfidence, p.ftRecentBandStore = newFTConfidenceController(cfg), newFTRecentBandStore(cfg)
			base := time.Now().UTC().Truncate(time.Second).Add(-8 * time.Second)
			oldSpots := []*spot.Spot{}
			for i, receiver := range []string{"N0AAA", "N0BBB"} {
				ctx := p4FTContext("K1ABC", receiver, "FT8", base.Add(time.Duration(i)*time.Second))
				ctx.spot.SourceType = spot.SourceManual
				ctx.spot.Comment, ctx.spot.SpotterIP = " ", "192.0.2.1"
				prepared, ok := p.prepareSpotContext(ctx.spot)
				prepared.peerOriginal = captureOriginalPeerPublication(ctx.spot)
				if !ok || p.applyFTConfidenceStage(&prepared, base.Add(time.Duration(i)*time.Second)) {
					t.Fatal("delivery fixture failed to hold old two-receiver burst")
				}
				oldSpots = append(oldSpots, ctx.spot)
			}
			key, _, _ := buildFTConfidenceKey(oldSpots[0])
			old := p.ftConfidence.pending[key]
			if old == nil || len(old.contexts) != 2 || len(old.spotters) != 2 || !old.due.Equal(base.Add(7*time.Second)) || f.ring.GetCount() != 0 {
				t.Fatal("delivery fixture did not establish undelivered old A+B burst")
			}
			retireAt := old.due
			lateCtx := p4FTContext("K1ABC", "N0CCC", "FT8", retireAt)
			lateCtx.spot.SourceType = spot.SourceManual
			lateCtx.spot.Comment, lateCtx.spot.SpotterIP = " ", "192.0.2.1"
			late, ok := p.prepareSpotContext(lateCtx.spot)
			late.peerOriginal = captureOriginalPeerPublication(lateCtx.spot)
			if !ok || p.applyFTConfidenceStage(&late, retireAt) {
				t.Fatal("late current report did not enter its new held burst")
			}
			current := p.ftConfidence.pending[key]
			if current == nil {
				t.Fatal("late arrival lost its pending generation")
			}
			if f.ring.GetCount() != 2 || len(old.contexts) != 2 || len(old.spotters) != 2 || current == old ||
				len(current.contexts) != 1 || len(current.spotters) != 1 || late.spot.Confidence != "" || p.ftConfidence.pendingSpots != 1 {
				t.Errorf("old P burst was not synchronously retired before current hold: ring=%d oldContexts=%d oldDE=%d pending=%d currentGlyph=%q",
					f.ring.GetCount(), len(old.contexts), len(old.spotters), p.ftConfidence.pendingSpots, late.spot.Confidence)
			}
			for _, s := range oldSpots {
				if s.Confidence != "P" || s.ConfidencePercentOK {
					t.Error("late third DE upgraded old P or fabricated a model percentage")
				}
			}
			// This is the existing timer/shutdown release entry point, now consuming
			// only the new singleton. Its support floor is deliberately observable.
			p.releaseDueFT(current.due, false)
			p.releaseDueFT(current.due, false)
			if f.ring.GetCount() != 3 || len(p.ftConfidence.pending) != 0 || p.ftConfidence.pendingSpots != 0 || late.spot.Confidence != newGlyph || late.spot.ConfidencePercentOK {
				t.Errorf("timer rail repeated old output or changed FT floor: ring=%d newGlyph=%q want=%q", f.ring.GetCount(), late.spot.Confidence, newGlyph)
			}
			want := []struct {
				receiver, glyph string
				at              time.Time
			}{
				{"N0AAA", "P", base}, {"N0BBB", "P", base.Add(time.Second)}, {"N0CCC", newGlyph, retireAt},
			}
			for _, destination := range []struct {
				name       string
				conn       net.Conn
				reader     *bufio.Reader
				diagnostic bool
			}{
				{"normal TCP", f.normalConn, f.normalReader, false}, {"DIAG TCP", f.diagConn, f.diagReader, true},
			} {
				for _, row := range want {
					got := correctionEvidenceReadDX(t, destination.conn, destination.reader)
					if expected := p4FTWire(row.receiver, row.glyph, destination.diagnostic, row.at); got != expected {
						t.Errorf("%s delivered old/new FT order or glyph mismatch:\n got %q\nwant %q", destination.name, got, expected)
					}
				}
			}
			for _, row := range want {
				fields := strings.Split(peerAdmissionReadSpot(t, f.peer), "^")
				if len(fields) < 9 {
					t.Fatalf("FT peer receipt lacked PC61 fields: %v", fields)
				}
				frequency, err := strconv.ParseFloat(fields[1], 64)
				if err != nil || fields[0] != "PC61" || frequency != 14074.1 || fields[2] != "K1ABC" || fields[6] != row.receiver ||
					fields[3] != row.at.Format("02-Jan-2006") || fields[4] != row.at.Format("1504Z") || fields[7] != "N0CALL-1" {
					t.Errorf("FT peer delivered identity/date/DE order mismatch: %v", fields)
				}
			}
			p4FTAssertStored(t, "sealed ring", f.ring.GetRecent(4), newGlyph, true)
			f.writer.Stop()
			reopened, err := archive.NewWriter(f.archiveCfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reopened.Stop)
			rows, err := reopened.Recent(4)
			if err != nil {
				t.Fatal(err)
			}
			p4FTAssertStored(t, "reopened archive", rows, newGlyph, false)
		})
	}
}

// FT is excluded from the default human mode selection. Real command replies
// establish admission and finish any read pause before publishing the burst.
func p4FTEnableReceiver(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	for _, command := range []struct {
		line    string
		replies []string
	}{
		{"PASS RULE 1 MODE=FT8", []string{"Rule 1 set.", "Rule 1 replaced."}},
		{"RESUME", []string{"Live spots resumed.", "Live spots were not paused."}},
	} {
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(conn, command.line+"\r\n"); err != nil {
			t.Fatal(err)
		}
		acknowledged := false
		for i := 0; i < 16; i++ {
			line := customSCPDeliveryReadLine(t, reader)
			t.Logf("FT admission reply: %q", line)
			if slices.Contains(command.replies, strings.TrimSuffix(line, "\r\n")) {
				acknowledged = true
				break
			}
		}
		if !acknowledged {
			t.Fatalf("FT fixture did not receive %s acknowledgement", command.line)
		}
	}
}

// Literal documented 78-column wire positions; no production formatter and no
// confidence ratio are used to derive this FT oracle. DIAG marks it unavailable.
func p4FTWire(receiver, glyph string, diagnostic bool, at time.Time) string {
	line := []byte(strings.Repeat(" ", 78))
	copy(line, "DX de "+receiver+":     14074.10")
	copy(line[27:], "K1ABC")
	comment := "FT8"
	if diagnostic {
		comment += " --%"
	}
	copy(line[39:], comment)
	line[71] = glyph[0]
	copy(line[73:], at.Format("1504Z"))
	return string(line) + "\r\n"
}

func p4FTAssertStored(t *testing.T, destination string, rows []*spot.Spot, newGlyph string, sealed bool) {
	t.Helper()
	if len(rows) != 3 {
		t.Fatalf("%s count=%d want three exactly-once outputs", destination, len(rows))
	}
	seen := make(map[string]bool)
	for _, s := range rows {
		glyph := "P"
		if s.DECallNorm == "N0CCC" {
			glyph = newGlyph
		}
		if seen[s.DECallNorm] || (s.DECallNorm != "N0AAA" && s.DECallNorm != "N0BBB" && s.DECallNorm != "N0CCC") ||
			s.DXCallNorm != "K1ABC" || s.ModeNorm != "FT8" || s.Frequency != 14074.1 || s.Confidence != glyph || s.ConfidencePercentOK {
			t.Errorf("%s changed FT membership/glyph or fabricated percentage: %+v", destination, s)
		}
		seen[s.DECallNorm] = true
		if sealed && s.SnapshotForAsync() != s {
			t.Errorf("%s spot was not sealed before fanout", destination)
		}
	}
}

func TestP4FTArrivalAndTimerRailsDeliverExactlyOnce(t *testing.T) {
	rb := buffer.NewRingBuffer(8)
	p := newTestFTOutputPipeline(config.CallCorrectionConfig{}, rb)
	base := time.Unix(1_700_000_000, 0).UTC()
	first := p4FTContext("K1AAA", "N0AAA", "FT8", base)
	if p.applyFTConfidenceStage(&first, base) {
		t.Fatal("first FT context was not held")
	}
	late := p4FTContext("K1BBB", "N0BBB", "FT8", base)
	if p.applyFTConfidenceStage(&late, base.Add(6*time.Second)) || rb.GetCount() != 1 {
		t.Fatal("arrival rail did not deliver unrelated expired group before holding current")
	}
	due, ok := p.ftConfidence.NextDue()
	if !ok || !due.Equal(base.Add(12*time.Second)) {
		t.Fatal("new burst lost its timer deadline")
	}
	// scheduleFTTimer uses a release clock at due, producing a real immediate
	// timer wakeup without changing any configured duration or sleeping.
	p.scheduleFTTimer(due)
	t.Cleanup(p.stopFTTimer)
	select {
	case <-p.ftTimerCh:
	case <-time.After(time.Second):
		t.Fatal("FT timer did not positively wake at its deadline")
	}
	p.releaseDueFT(due, false)
	p.scheduleFTTimer(due)
	p.releaseDueFT(due, true)
	if rb.GetCount() != 2 || p.ftTimer != nil || p.ftTimerCh != nil || len(p.ftConfidence.pending) != 0 || p.ftConfidence.pendingSpots != 0 || p.ftConfidence.queue.Len() != 0 {
		t.Fatal("timer/forced release duplicated output or retained controller/timer state")
	}
}

func TestP4FTConfiguredThresholdsSurviveLateArrival(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		PMinUniqueSpotters: 3, VMinUniqueSpotters: 4,
		FT8QuietGapSeconds: 7, FT8HardCapSeconds: 14,
		FT4QuietGapSeconds: 4, FT4HardCapSeconds: 9,
		FT2QuietGapSeconds: 2, FT2HardCapSeconds: 5,
	}
	for _, tc := range []struct {
		mode  string
		quiet time.Duration
	}{{"FT8", 7 * time.Second}, {"FT4", 4 * time.Second}, {"FT2", 2 * time.Second}} {
		t.Run(tc.mode, func(t *testing.T) {
			rb := buffer.NewRingBuffer(8)
			p := newTestFTOutputPipeline(cfg, rb)
			base := time.Unix(1_700_000_000, 0).UTC()
			old := []*spot.Spot{}
			for i, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
				at := base.Add(time.Duration(i) * 250 * time.Millisecond)
				ctx := p4FTContext("K1ABC", receiver, tc.mode, base)
				if p.applyFTConfidenceStage(&ctx, at) {
					t.Fatal("configured old burst was not held")
				}
				old = append(old, ctx.spot)
			}
			arrival := base.Add(500*time.Millisecond + tc.quiet)
			late := p4FTContext("K1ABC", "N0DDD", tc.mode, base)
			if p.applyFTConfidenceStage(&late, arrival) || rb.GetCount() != 3 {
				t.Fatal("configured late fourth receiver did not retire old P before current hold")
			}
			for _, s := range old {
				if s.Confidence != "P" {
					t.Fatalf("configured old three-receiver glyph=%q want P", s.Confidence)
				}
			}
			p.releaseDueFT(arrival.Add(tc.quiet), false)
			if rb.GetCount() != 4 || late.spot.Confidence != "?" || late.spot.ConfidencePercentOK {
				t.Fatal("configured new singleton borrowed old support or a model percentage")
			}
		})
	}
}
