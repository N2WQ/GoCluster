//go:build qualification

package peer

import "testing"

func TestQualificationCapacityWireUsesOrdinaryParsersAndExactSizes(t *testing.T) {
	g := NewQualificationCapacityGenerator(newQualificationTopology(true, []string{"DL1PAA"}))
	for _, size := range []int{0, 8128, 65536} {
		for _, class := range []string{"spot", "pc92"} {
			wire, err := g.Wire(class, size, size, 2)
			if err != nil {
				t.Fatal(err)
			}
			if size > 0 && len(wire) != size {
				t.Fatalf("%s wire%d want%d", class, len(wire), size)
			}
			frame, err := ParseFrame(wire)
			if err != nil {
				t.Fatal(err)
			}
			if class == "pc92" {
				if _, err := DecodePC92(frame); err != nil {
					t.Fatal(err)
				}
			} else {
				spot, err := parseSpotFromFrame(frame, "DL1PAA")
				if err != nil {
					t.Fatal(err)
				}
				if key := dxKey(frame, spot); len(key) != 360 {
					t.Fatalf("reachable maximum-scale spot key length%d want360", len(key))
				}
			}
		}
	}
	for _, class := range []string{"pc93", "bulletin"} {
		size := 128
		if class == "bulletin" {
			size = 256
		}
		wire, err := g.Wire(class, 1, size, 1)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		var key string
		if class == "pc93" {
			if _, ok := parsePC93(frame); !ok {
				t.Fatal("capacity PC93 was rejected")
			}
			key = pc93Key(frame)
		} else {
			if _, ok := parseWWV(frame); !ok {
				t.Fatal("capacity bulletin was rejected")
			}
			key = wwvKey(frame)
		}
		if len(key) != size {
			t.Fatalf("%s canonical key%d want%d", class, len(key), size)
		}
	}
}
