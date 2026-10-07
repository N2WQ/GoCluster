package telnet

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
)

func TestParseGrid2List(t *testing.T) {
	for _, tc := range []struct {
		input string
		grids []string
		bad   []string
	}{
		{"AA,AR,RA,RR", []string{"AA", "AR", "RA", "RR"}, []string{}},
		{"fn05 fn,em", []string{"FN", "EM"}, []string{}},
		{"ZZ,AS,SA,A1,1A,éA,A", []string{}, []string{"ZZ", "AS", "SA", "A1", "1A", "éA", "A"}},
		{"FN,ZZ", []string{"FN"}, []string{"ZZ"}},
	} {
		t.Run(tc.input, func(t *testing.T) {
			grids, bad := parseGrid2List(tc.input)
			if !reflect.DeepEqual(grids, tc.grids) || !reflect.DeepEqual(bad, tc.bad) {
				t.Fatalf("got (%v, %v), want (%v, %v)", grids, bad, tc.grids, tc.bad)
			}
		})
	}
}

func TestGrid2InvalidCommandsDoNotMutate(t *testing.T) {
	for _, name := range []string{"DXGRID2", "DEGRID2"} {
		for _, action := range []filterAction{actionAllow, actionBlock} {
			for _, input := range []string{"ZZ", "FN,ZZ"} {
				t.Run(fmt.Sprintf("%s/%d/%s", name, action, input), func(t *testing.T) {
					c := newTestClient()
					c.filter.SetDXGrid2Prefix("EM", true)
					c.filter.SetDEGrid2Prefix("IO", false)
					before := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()
					handler := newGrid2Handler(name, func(f *filter.Filter, grid string, allowed bool) {
						if name == "DXGRID2" {
							f.SetDXGrid2Prefix(grid, allowed)
						} else {
							f.SetDEGrid2Prefix(grid, allowed)
						}
					})
					response, mutated := handler.apply(c, action, []string{input})
					if mutated || (!strings.Contains(response, "Usage:") && !strings.Contains(response, "Unknown 2-character grid:")) {
						t.Fatalf("invalid command returned (%q, %v)", response, mutated)
					}
					if !reflect.DeepEqual(before, filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{})) {
						t.Fatal("invalid command changed filter configuration")
					}
				})
			}
		}
	}
}

func FuzzParseGrid2List(f *testing.F) {
	for _, seed := range []string{"FN05,EM", "ZZ", "AA,RR", "FN,ZZ", "éA"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		grids, _ := parseGrid2List(input)
		for _, grid := range grids {
			if len(grid) != 2 || strings.Trim(grid, "ABCDEFGHIJKLMNOPQR") != "" {
				t.Fatalf("accepted invalid prefix %q from %q", grid, input)
			}
		}
	})
}
