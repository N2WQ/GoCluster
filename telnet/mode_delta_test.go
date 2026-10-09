// File role: MODE effective feedback agrees with the actual matcher, including
// dormant and nonstandard stored rules that survive marked persistence.
package telnet

import (
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestModeSnapshotUsesActualRawFallback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rules filter.StringRules
		want  string
		open  bool
	}{
		{"open", filter.StringRules{AllowAll: true, Block: map[string]bool{"FT8": true, "FALSECUSTOM": false}}, supportedModesExcept("FT8"), true},
		{"contradictory", filter.StringRules{AllowAll: true, Allow: map[string]bool{"CW": true, "FALSECUSTOM": false}}, "CW", false},
		{"false only", filter.StringRules{AllowAll: true, Allow: map[string]bool{"CW": false, "FALSECUSTOM": false}}, "NONE", false},
		{"noncanonical only", filter.StringRules{AllowAll: true, Allow: map[string]bool{" CW ": true, "cw": true, "PSK31": true}}, "NONE", false},
		{"custom", filter.StringRules{Allow: map[string]bool{"CW": true, "CUSTOM": true, "FALSECUSTOM": false}, Block: map[string]bool{"CW": true}}, "CUSTOM", false},
		{"block all", filter.StringRules{BlockAll: true, AllowAll: true, Allow: map[string]bool{"CW": true}}, "NONE", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient()
			f := c.filter
			f.Modes, f.BlockModes, f.AllModes, f.BlockAllModes = tc.rules.Allow, tc.rules.Block, tc.rules.AllowAll, tc.rules.BlockAll
			snapshot := snapshotModeFilter(f)
			if snapshot.enabled != tc.want || snapshot.allowAll != tc.open || strings.Contains(snapshot.allow+snapshot.block, "FALSECUSTOM") || strings.Contains(snapshot.allow, "PSK31") {
				t.Fatalf("wrong effective MODE snapshot: %+v", snapshot)
			}
			for _, mode := range append(filter.SupportedModes(), "CUSTOM", "FALSECUSTOM") {
				listed := false
				for _, value := range strings.Split(snapshot.enabled, ", ") {
					listed = listed || value == mode
				}
				// The open snapshot enumerates the current taxonomy; its ALL
				// fallback separately describes custom/future acceptance.
				want := listed || snapshot.allowAll && !f.BlockModes[mode] && !f.BlockAllModes
				if got := f.Matches(&spot.Spot{Band: "20m", Mode: mode, ModeNorm: mode}); got != want {
					t.Fatalf("MODE feedback disagrees for %s: actual=%v snapshot=%+v", mode, got, snapshot)
				}
			}
		})
	}
	complete := newTestClient().filter
	complete.Modes = make(map[string]bool)
	for _, mode := range filter.SupportedModes() {
		complete.Modes[mode] = true
	}
	if snapshotModeFilter(complete).allowAll {
		t.Fatal("complete current taxonomy was promoted to an unseen-mode fallback")
	}
}

func TestModeHumanDeltaReadbackAndMatching(t *testing.T) {
	c := newTestClient()
	engine := newFilterCommandEngine()
	detail := func() string {
		t.Helper()
		cfg := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Filters
		var h humanResponse
		if err := writeHumanEffectiveCategory(&h, readbackCategory{name: "MODE", rules: cfg.Modes}, cfg, configurationReadbackStatus{}); err != nil {
			t.Fatal(err)
		}
		return string(h.data)
	}
	for _, command := range []string{"PASS MODE ALL", "REJECT MODE FT8", "PASS MODE FT8"} {
		response, handled := engine.Handle(c, command)
		if !handled || !strings.Contains(response, "Effective MODE:") {
			t.Fatalf("%s: %q", command, response)
		}
		for _, mode := range []string{"CW", "CUSTOM", filter.UnknownModeToken} {
			if !c.filter.Matches(&spot.Spot{Band: "20m", Mode: mode, ModeNorm: mode}) {
				t.Fatalf("%s changed unlisted %s visibility", command, mode)
			}
		}
	}
	response := detail()
	if !strings.Contains(response, "PASS: ALL") || !strings.Contains(response, "Unknown modes are included") {
		t.Fatalf("detail lost open MODE fallback: %q", response)
	}
	for _, command := range []string{"REJECT MODE ALL", "PASS MODE CW", "REJECT MODE CW"} {
		if _, handled := engine.Handle(c, command); !handled {
			t.Fatalf("unhandled %s", command)
		}
	}
	if c.filter.AllModes || c.filter.BlockAllModes || len(c.filter.Modes) != 0 {
		t.Fatal("last restrictive allow removal did not leave restrictive empty state")
	}
	response = detail()
	if !strings.Contains(response, "PASS: NONE") || !strings.Contains(response, "Unknown modes are hidden") {
		t.Fatalf("detail reopened an empty restrictive state: %q", response)
	}
}
