package dedup

import (
	"encoding/hex"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestSecondaryDedupeKeyLiteralVectors(t *testing.T) {
	base := func() *spot.Spot {
		return &spot.Spot{DXCall: " k1abc ", Frequency: 14074, Band: "20m", DEMetadata: spot.CallMetadata{ADIF: 291, CQZone: 5}, DEGrid2: "fn"}
	}
	// Band(4), ADIF(2), grid/zone(2), DX(12), source(1), zeros(11).
	grid := "32304d002301464e4b3141424300000000000000010000000000000000000000"
	zone := "32304d00230105004b3141424300000000000000010000000000000000000000"
	for _, tt := range []struct {
		name   string
		mode   SecondaryKeyMode
		mutate func(*spot.Spot)
		want   string
	}{
		{"grid", SecondaryKeyGrid2, func(*spot.Spot) {}, grid},
		{"zone", SecondaryKeyCQZone, func(*spot.Spot) {}, zone},
		{"missing_grid", SecondaryKeyGrid2, func(s *spot.Spot) { s.DEGrid2 = "" }, grid[:12] + "0000" + grid[16:]},
		{"one_grid_byte", SecondaryKeyGrid2, func(s *spot.Spot) { s.DEGrid2 = "f" }, grid[:12] + "4600" + grid[16:]},
		{"grid_fallback", SecondaryKeyGrid2, func(s *spot.Spot) { s.DEGrid2 = ""; s.DEGridNorm = "fn20" }, grid},
		{"invalid_zone", SecondaryKeyCQZone, func(s *spot.Spot) { s.DEMetadata.CQZone = 41 }, zone[:12] + "0000" + zone[16:]},
		{"zero_zone", SecondaryKeyCQZone, func(s *spot.Spot) { s.DEMetadata.CQZone = 0 }, zone[:12] + "0000" + zone[16:]},
		{"skimmer", SecondaryKeyGrid2, func(s *spot.Spot) { s.SourceType = spot.SourceRBN }, grid[:40] + "02" + grid[42:]},
		{"band_fallback", SecondaryKeyGrid2, func(s *spot.Spot) { s.Band = "" }, grid},
		{"fixed_dx_width", SecondaryKeyGrid2, func(s *spot.Spot) { s.DXCallNorm = "ABCDEFGHIJKLMNO" }, grid[:16] + "4142434445464748494a4b4c" + grid[40:]},
		{"adif_low_16", SecondaryKeyGrid2, func(s *spot.Spot) { s.DEMetadata.ADIF += 65536 }, grid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := base()
			tt.mutate(s)
			key := secondaryKey(s, s.DEMetadata.ADIF, tt.mode)
			if got := hex.EncodeToString(key[:]); got != tt.want {
				t.Fatalf("key=%s want=%s", got, tt.want)
			}
		})
	}
}

func TestPrimaryDedupeWindowAndUpgradePolicy(t *testing.T) {
	base := time.Unix(60, 0)
	key := [42]byte{1}
	for _, offset := range []time.Duration{-time.Minute - 1, -time.Minute, -time.Minute + 1, 0, time.Minute - 1, time.Minute, time.Minute + 1} {
		cache := map[[42]byte]cachedEntry{key: {when: base}}
		got, _ := isDuplicateLocked(cache, key, base.Add(offset), time.Minute)
		want := offset > -time.Minute && offset < time.Minute
		if got != want {
			t.Fatalf("offset=%s duplicate=%t want=%t", offset, got, want)
		}
	}
	for _, window := range []time.Duration{0, -time.Second} {
		d := NewDeduplicator(window, false, 4)
		s := policySpot(base, false, -10)
		for range 2 {
			d.processSpot(s)
			requirePrimaryOutput(t, d, s)
		}
		if processed, duplicates, size := d.GetStats(); processed != 2 || duplicates != 0 || size != 1 {
			t.Fatalf("disabled primary stats=%d/%d/%d", processed, duplicates, size)
		}
	}
	for _, prefer := range []bool{false, true} {
		d := NewDeduplicator(time.Minute, prefer, 4)
		first := policySpot(base, false, -10)
		d.processSpot(first)
		requirePrimaryOutput(t, d, first)
		plain := policySpot(base.Add(time.Second), false, -10)
		d.processSpot(plain)
		requirePrimaryOutput(t, d, nil)
		shard := d.shardFor(first.Hash32())
		if !shard.cache[first.DedupeKey()].when.Equal(base) {
			t.Fatal("ordinary duplicate renewed age")
		}
		reported := policySpot(base.Add(2*time.Second), true, -20)
		d.processSpot(reported)
		requirePrimaryOutput(t, d, reported)
		stronger := policySpot(base.Add(3*time.Second), true, -5)
		d.processSpot(stronger)
		if prefer {
			requirePrimaryOutput(t, d, stronger)
		} else {
			requirePrimaryOutput(t, d, nil)
		}
		last := reported
		if prefer {
			last = stronger
		}
		entry := shard.cache[first.DedupeKey()]
		if !entry.when.Equal(last.Time) || entry.snr != last.Report || !entry.hasReport {
			t.Fatal("upgrade cache state differs")
		}
		for _, report := range []int{last.Report, last.Report - 1} {
			repeat := policySpot(base.Add(4*time.Second), true, report)
			d.processSpot(repeat)
			requirePrimaryOutput(t, d, nil)
		}
	}
}

func TestSecondaryDedupeWindowAndUpgradePolicy(t *testing.T) {
	base := time.Unix(60, 0)
	for _, window := range []time.Duration{179 * time.Second, 359 * time.Second, 479 * time.Second} {
		for _, offset := range []time.Duration{-window - 1, -window, -window + 1, 0, window - 1, window, window + 1} {
			d := NewSecondaryDeduper(window, false)
			if !d.ShouldForward(policySpot(base, false, -10)) {
				t.Fatal("first spot suppressed")
			}
			got := d.ShouldForward(policySpot(base.Add(offset), false, -10))
			want := offset <= -window || offset >= window
			if got != want {
				t.Fatalf("window=%s offset=%s forward=%t want=%t", window, offset, got, want)
			}
		}
	}
	for _, window := range []time.Duration{0, -time.Second} {
		d := NewSecondaryDeduper(window, false)
		for range 2 {
			if !d.ShouldForward(policySpot(base, false, -10)) {
				t.Fatal("disabled secondary suppressed")
			}
		}
		if processed, duplicates, size := d.GetStats(); processed != 0 || duplicates != 0 || size != 0 {
			t.Fatal("disabled secondary retained state")
		}
	}
	for _, prefer := range []bool{false, true} {
		d := NewSecondaryDeduper(time.Minute, prefer)
		first := policySpot(base, false, -10)
		if !d.ShouldForward(first) || d.ShouldForward(policySpot(base.Add(time.Second), false, -10)) {
			t.Fatal("initial/repeat policy differs")
		}
		key := secondaryKey(first, 291, SecondaryKeyGrid2)
		entry := d.shards[uint32Hash(key[:])&63].cache[key]
		if !entry.when.Equal(base) {
			t.Fatal("ordinary duplicate renewed age")
		}
		if !d.ShouldForward(policySpot(base.Add(2*time.Second), true, -20)) {
			t.Fatal("report upgrade suppressed")
		}
		if got := d.ShouldForward(policySpot(base.Add(3*time.Second), true, -5)); got != prefer {
			t.Fatal("stronger policy differs")
		}
		lastReport := -20
		if prefer {
			lastReport = -5
		}
		for _, report := range []int{lastReport, lastReport - 1} {
			if d.ShouldForward(policySpot(base.Add(4*time.Second), true, report)) {
				t.Fatal("equal/weaker repeat forwarded")
			}
		}
	}
	d := NewSecondaryDeduper(time.Minute, false)
	for _, adif := range []int{0, -1} {
		s := policySpot(base, false, -10)
		s.DEMetadata.ADIF = adif
		if !d.ShouldForward(s) {
			t.Fatal("missing ADIF suppressed")
		}
	}
	if !d.ShouldForward(nil) {
		t.Fatal("nil secondary input suppressed")
	}
	if processed, _, size := d.GetStats(); processed != 0 || size != 0 {
		t.Fatal("bypass input retained state")
	}
}

func policySpot(at time.Time, report bool, snr int) *spot.Spot {
	s := spot.NewSpot("K1ABC", "W1XYZ", 14074, "FT8")
	s.Time, s.HasReport, s.Report = at, report, snr
	s.DEMetadata.ADIF, s.DEMetadata.CQZone = 291, 5
	s.DEGrid2 = "FN"
	return s
}
