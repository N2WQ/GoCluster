package dedup

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"dxcluster/spot"
	"github.com/zeebo/xxh3"
)

type collisionSpot struct {
	Call, Time, Mode, Primary, Secondary string
	Frequency                            float64
}

func (s collisionSpot) spot(t *testing.T) *spot.Spot {
	t.Helper()
	at, err := time.Parse(time.RFC3339, s.Time)
	if err != nil {
		t.Fatal(err)
	}
	out := spot.NewSpot(s.Call, "DL1AAA", s.Frequency, s.Mode)
	out.Time, out.SourceType = at, spot.SourcePeer
	out.DEMetadata = spot.CallMetadata{ADIF: 230, CQZone: 14}
	return out
}

func TestQualificationCollisionPairs(t *testing.T) {
	// Literal inputs/encodings freeze the independently reconciled historical
	// pairs. No CTY file, current workload generator, or new encoder supplies
	// their expected values.
	data, err := os.ReadFile("testdata/q1-collisions.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Pairs []struct {
			ID            int
			Kind, Hash    string
			First, Second collisionSpot
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Pairs) != 9 {
		t.Fatal("need all nine retained collision pairs")
	}
	wantIDs := map[int]bool{90004: true, 163460: true, 238635: true, 269385: true, 284511: true, 313619: true, 358299: true, 358514: true, 450793: true}
	for _, pair := range fixture.Pairs {
		if !wantIDs[pair.ID] {
			t.Fatalf("unexpected or repeated pair %d", pair.ID)
		}
		delete(wantIDs, pair.ID)
		t.Run(fmt.Sprintf("%s-%d", pair.Kind, pair.ID), func(t *testing.T) {
			first, second := pair.First.spot(t), pair.Second.spot(t)
			for _, item := range []struct {
				spot    *spot.Spot
				literal collisionSpot
			}{{first, pair.First}, {second, pair.Second}} {
				primary := item.spot.DedupeKey()
				secondary := secondaryKey(item.spot, 230, SecondaryKeyCQZone)
				if hex.EncodeToString(primary[:]) != item.literal.Primary || hex.EncodeToString(secondary[:]) != item.literal.Secondary {
					t.Fatal("historical complete encoding changed")
				}
				var hash uint32
				if pair.Kind == "primary" {
					hash = item.spot.Hash32()
				} else {
					hash = uint32(xxh3.Hash(secondary[:]))
				}
				if fmt.Sprintf("%08x", hash) != pair.Hash {
					t.Fatalf("historical hash=%08x want=%s", hash, pair.Hash)
				}
			}
			if pair.Kind == "primary" {
				if first.DedupeKey() == second.DedupeKey() {
					t.Fatal("fixture lacks distinct complete keys")
				}
				d := NewDeduplicator(120*time.Second, true, 4)
				for _, s := range []*spot.Spot{first, second} {
					d.processSpot(s)
					requirePrimaryOutput(t, d, s)
				}
				for _, s := range []*spot.Spot{first, second} {
					d.processSpot(s)
					requirePrimaryOutput(t, d, nil)
				}
				if _, duplicates, size := d.GetStats(); duplicates != 2 || size != 2 {
					t.Fatalf("duplicates=%d size=%d", duplicates, size)
				}
			} else {
				if secondaryKey(first, 230, SecondaryKeyCQZone) == secondaryKey(second, 230, SecondaryKeyCQZone) {
					t.Fatal("fixture lacks distinct complete keys")
				}
				d := NewSecondaryDeduperWithKey(479*time.Second, false, SecondaryKeyCQZone)
				for _, s := range []*spot.Spot{first, second} {
					if !d.ShouldForward(s) {
						t.Fatal("distinct colliding key suppressed")
					}
				}
				for _, s := range []*spot.Spot{first, second} {
					if d.ShouldForward(s) {
						t.Fatal("true repeat forwarded")
					}
				}
				if _, duplicates, size := d.GetStats(); duplicates != 2 || size != 2 {
					t.Fatalf("duplicates=%d size=%d", duplicates, size)
				}
			}
		})
	}
}

func requirePrimaryOutput(t *testing.T, d *Deduplicator, want *spot.Spot) {
	t.Helper()
	select {
	case got := <-d.outputChan:
		if got != want {
			t.Fatalf("output=%v want=%v", got, want)
		}
	default:
		if want != nil {
			t.Fatalf("missing output for %s", want.DXCall)
		}
	}
}
