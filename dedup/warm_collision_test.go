package dedup

import (
	"encoding/hex"
	"testing"
	"time"
)

func TestWarmQualificationPrimaryCollision19367(t *testing.T) {
	// The retained 15-minute baseline lost ID 19367 on all 100 clients.
	// Its input ledger fixes these wire minutes; Python struct encoding froze
	// the expected bytes independently of DedupeKey. ID 16953 arrived first.
	literals := []collisionSpot{
		{
			Call: "DL1AAZZCCBB", Frequency: 14275, Mode: "SSB", Time: "2026-10-02T20:01:00Z",
			Primary: "7c0dc06a00000000c3370000444c31414141000000000000000000444c3141415a5a4343424200000000",
		},
		{
			Call: "DL1BBCCQQXX", Frequency: 14041.8, Mode: "CW", Time: "2026-10-02T20:02:00Z",
			Primary: "b80dc06a00000000d9360000444c31414141000000000000000000444c31424243435151585800000000",
		},
	}
	d := NewDeduplicator(120*time.Second, true, 4)
	for _, literal := range literals {
		s := literal.spot(t)
		key := s.DedupeKey()
		if hex.EncodeToString(key[:]) != literal.Primary || s.Hash32() != 0x2ad54793 {
			t.Fatalf("historical collision encoding changed: call=%s key=%x hash=%08x", literal.Call, key, s.Hash32())
		}
		d.processSpot(s)
		requirePrimaryOutput(t, d, s)
	}
	for _, literal := range literals {
		d.processSpot(literal.spot(t))
		requirePrimaryOutput(t, d, nil)
	}
	if _, duplicates, size := d.GetStats(); duplicates != 2 || size != 2 {
		t.Fatalf("duplicates=%d size=%d", duplicates, size)
	}
}
