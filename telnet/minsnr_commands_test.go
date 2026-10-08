package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

func minSNRPresetModified(c *Client) bool {
	cfg := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
	return attachReadbackConfiguration(configurationReadbackStatus{}, c, cfg).Preset.Modified
}

func TestMinSNRCommandGrammarAndResets(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			e := newFilterCommandEngine()
			for _, tc := range []struct {
				line string
				want map[string]int
			}{
				{"PASS MINSNR CW,RTTY 10", map[string]int{"CW": 10, "RTTY": 10}},
				{"REJECT MINSNR FT8,FT4 -10", map[string]int{"CW": 10, "RTTY": 10, "FT8": -10, "FT4": -10}},
				{"PASS MINSNR cw 0", map[string]int{"CW": 0, "RTTY": 10, "FT8": -10, "FT4": -10}},
				{"REJECT MINSNR cw 0", map[string]int{"CW": 0, "RTTY": 10, "FT8": -10, "FT4": -10}},
				{"PASS MINSNR FT8,FT4 ALL", map[string]int{"CW": 0, "RTTY": 10}},
				{"REJECT MINSNR RTTY NONE", map[string]int{"CW": 0}},
			} {
				response, handled := e.Handle(c, tc.line)
				if !handled || strings.Contains(response, "Invalid") || !maps.Equal(c.filter.MinSNR, tc.want) {
					t.Fatalf("%q: %q thresholds=%v", tc.line, response, c.filter.MinSNR)
				}
				record, err := filter.LoadUserRecord(c.callsign)
				if err != nil || !maps.Equal(record.MinSNR, tc.want) {
					t.Fatalf("saved thresholds for %q: %v %+v", tc.line, err, record)
				}
			}
			e.Handle(c, "PASS MINSNR ALL -5")
			for _, mode := range filter.SupportedModes() {
				if value, ok := c.filter.MinSNR[mode]; !ok || value != -5 {
					t.Fatalf("ALL omitted mode %q", mode)
				}
			}
			for _, reset := range []string{"PASS MINSNR ALL ALL", "REJECT MINSNR ALL NONE", "RESET FILTER", "PASS NOFILTER"} {
				c.filter.MinSNR["RETIRED"] = 7
				if response, _ := e.Handle(c, reset); len(c.filter.MinSNR) != 0 {
					t.Fatalf("%q retained thresholds: %q %v", reset, response, c.filter.MinSNR)
				}
				e.Handle(c, "PASS MINSNR CW 1")
			}
			c.filter.MinSNR["RETIRED"] = 7
			e.Handle(c, "REJECT MINSNR RETIRED NONE")
			if _, retained := c.filter.MinSNR["RETIRED"]; retained {
				t.Fatal("explicit dormant clear failed")
			}
		})
	}
}

func TestMinSNRCommandAtomicRejection(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	e := newFilterCommandEngine()
	e.Handle(c, "PASS MINSNR CW,RTTY 10")
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	disk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"PASS MINSNR CW,BOGUS 1", "REJECT MINSNR FT8,BOGUS -1", "PASS MINSNR ALL,CW 1",
		"PASS MINSNR CW NONE", "REJECT MINSNR CW ALL", "PASS MINSNR CW -1.5",
		"PASS MINSNR CW 9999999999999999999999999999", "PASS MINSNR CW,,RTTY 0", "PASS MINSNR CW, ALL",
	} {
		response, handled := e.Handle(c, line)
		after, err := os.ReadFile(path)
		if !handled || (!strings.Contains(response, "Invalid") && !strings.Contains(response, "Usage:")) || err != nil || !bytes.Equal(disk, after) || !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
			t.Fatalf("non-atomic rejection %q: %q err=%v", line, response, err)
		}
	}
}

func TestMinSNRCommandUnionBound(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.MinSNR = make(map[string]int)
	for i := range filter.MaxMinSNREntries {
		c.filter.MinSNR[fmt.Sprintf("RETIRED%d", i)] = i
	}
	before := maps.Clone(c.filter.MinSNR)
	for _, command := range []string{"PASS MINSNR CW,RTTY 0", "REJECT MINSNR ALL -10"} {
		response, _ := newFilterCommandEngine().Handle(c, command)
		if !strings.Contains(response, "limit") || !maps.Equal(before, c.filter.MinSNR) {
			t.Fatalf("union cap failed: %q %v", response, c.filter.MinSNR)
		}
	}
}

func TestMinSNRCommandClearExactDormantAlias(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	// FT-8 aliases FT8 today, but a previously retained exact key is dormant.
	// Clear must resolve that key first, preserving the active FT8 threshold.
	c.filter.MinSNR = map[string]int{"FT-8": 17, "FT8": -10}
	response, _ := newFilterCommandEngine().Handle(c, "REJECT MINSNR FT-8 NONE")
	if !maps.Equal(c.filter.MinSNR, map[string]int{"FT8": -10}) {
		t.Fatalf("dormant clear rebound its alias: %q %v", response, c.filter.MinSNR)
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || !maps.Equal(record.MinSNR, map[string]int{"FT8": -10}) {
		t.Fatalf("dormant clear did not persist exactly: %v", err)
	}
}

func TestMinSNRHumanPersistenceFailure(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	e := newFilterCommandEngine()
	e.Handle(c, "PASS MINSNR CW 10")
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.saveConfigurationFn = func(string, filter.Configuration, *filter.PresetReference, []string) error {
		return errors.New("injected save failure")
	}
	response, _ := e.Handle(c, "PASS MINSNR CW 0")
	after, err := os.ReadFile(path)
	if c.filter.MinSNR["CW"] != 0 || !strings.Contains(response, "0 dB") || err != nil || !bytes.Equal(before, after) {
		t.Fatalf("human live-first behavior changed: %q err=%v", response, err)
	}
}

func TestMinSNRPresetReconnectContinuity(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	e := newFilterCommandEngine()
	e.Handle(c, "PASS MINSNR CW 0")
	e.Handle(c, "REJECT MINSNR FT8 -10")
	c.filter.MinSNR["RETIRED"] = 9
	if response, _ := s.handlePresetCommand(c, "SAVE PRESET SNR"); !strings.Contains(response, "Saved") {
		t.Fatal(response)
	}
	want := map[string]int{"CW": 0, "FT8": -10, "RETIRED": 9}
	e.Handle(c, "PASS MINSNR CW 1")
	if !minSNRPresetModified(c) {
		t.Fatal("threshold edit did not mark preset modified")
	}
	e.Handle(c, "PASS MINSNR CW 0")
	if minSNRPresetModified(c) {
		t.Fatal("restored threshold still marked modified")
	}
	e.Handle(c, "PASS MINSNR ALL ALL")
	if response, _ := s.handlePresetCommand(c, "LOAD PRESET SNR"); !strings.Contains(response, "Loaded") || !maps.Equal(want, c.filter.MinSNR) {
		t.Fatal(response, c.filter.MinSNR)
	}
	reconnected := configurationTestClient(s, c.callsign)
	if _, err := s.restoreAndRegisterClient(reconnected, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(want, reconnected.filter.MinSNR) || minSNRPresetModified(reconnected) {
		t.Fatal("reconnect lost thresholds or applied baseline", reconnected.filter.MinSNR)
	}
	s.unregisterClient(reconnected)
}

func FuzzMinSNRCommands(f *testing.F) {
	for _, line := range []string{"PASS MINSNR CW 0", "REJECT MINSNR FT8 -10", "PASS MINSNR CW,BOGUS 1", "PASS MINSNR ALL ALL", "REJECT MINSNR RETIRED NONE", "PASS MINSNR CW 1.5"} {
		f.Add(line)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > 4096 {
			return
		}
		c := &Client{filter: filter.NewFilter()}
		c.filter.MinSNR = map[string]int{"CW": 7, "FT8": -12, "RETIRED": 3}
		before := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()
		tokens := strings.Fields(line)
		if len(tokens) < 2 || !strings.EqualFold(tokens[1], "MINSNR") {
			return
		}
		action := actionAllow
		if strings.EqualFold(tokens[0], "REJECT") {
			action = actionBlock
		} else if !strings.EqualFold(tokens[0], "PASS") {
			return
		}
		_, changed := newMinSNRHandler().apply(c, action, tokens[2:])
		if !changed && !before.Equal(filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{})) {
			t.Fatal("rejected command partially mutated configuration")
		}
		if err := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).ValidateMinSNRRules(); err != nil {
			t.Fatal("command published an unbounded threshold map", err)
		}
	})
}
