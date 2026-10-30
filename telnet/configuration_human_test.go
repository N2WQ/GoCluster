package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

func assertHumanWire(t *testing.T, response string) {
	t.Helper()
	if len(response) > 65536 || !strings.HasSuffix(response, "\r\n") {
		t.Fatalf("invalid response length/framing: %d", len(response))
	}
	for _, line := range strings.Split(strings.TrimSuffix(response, "\r\n"), "\r\n") {
		if len(line) > 78 {
			t.Fatalf("line exceeds 78 columns: %q", line)
		}
		for _, value := range []byte(line) {
			if value < 32 || value > 126 {
				t.Fatalf("nonprintable/non-ASCII line: %q", line)
			}
		}
	}
}

func unquoteHumanPieces(t *testing.T, response string) string {
	t.Helper()
	var value strings.Builder
	for _, line := range strings.Split(response, "\r\n") {
		start := strings.IndexByte(line, '"')
		if start < 0 {
			continue
		}
		quoted, err := strconv.QuotedPrefix(line[start:])
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := strconv.Unquote(quoted)
		if err != nil {
			t.Fatal(err)
		}
		value.WriteString(decoded)
	}
	return value.String()
}

func TestHumanQuotedValuesRoundTripAndWidths(t *testing.T) {
	values := []string{"", "  leading and trailing  ", "quotes \" and backslash \\", "café 😀\t\n\r",
		string([]byte{0xff, 0, 0x1b}), strings.Repeat(" \"\\é😀\t ", 100)}
	for _, length := range []int{77, 78, 79} {
		values = append(values, strings.Repeat("A", length-16))
	}
	for i, value := range values {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var response humanResponse
			if err := response.quoted("Value         ", value, ""); err != nil {
				t.Fatal(err)
			}
			assertHumanWire(t, string(response.data))
			if got := unquoteHumanPieces(t, string(response.data)); got != value {
				t.Fatalf("lost bytes: %q != %q", got, value)
			}
			preflight := humanResponse{countOnly: true}
			if err := preflight.quoted("Value         ", value, ""); err != nil ||
				preflight.size != len(response.data) || len(preflight.data) != 0 {
				t.Fatal("preflight differs from final CRLF response size")
			}
		})
	}
}

func TestHumanSettingsRestoredPathMinimum(t *testing.T) {
	for _, minimum := range []int{0, 15, 21, 30} {
		t.Run(strconv.Itoa(minimum), func(t *testing.T) {
			s := presetTestServer(t)
			cfg := pathreliability.DefaultConfig()
			cfg.MinObservationCount, cfg.BeaconMinObservationCount = 21, 11
			s.pathPredictor = pathreliability.NewPredictor(cfg, []string{"20m"})
			record := &filter.UserRecord{Filter: *testRuleFilter(t), Dialect: "go", PathMinObservationCount: minimum}
			if err := filter.SaveUserRecord("W1ABC-1", record); err != nil {
				t.Fatal(err)
			}
			c := configurationTestClient(s, "W1ABC-1")
			if _, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
			before := presetDiskBytes(t, path)
			if !s.handleHumanReadback(c, "SHOW SETTINGS") {
				t.Fatal("settings not handled")
			}
			message := <-c.controlChan
			t.Cleanup(message.configurationReservation.Release)
			got := string(message.raw)
			assertHumanWire(t, got)
			want := "Path samples  DEFAULT; stations 21, beacons 11\r\n"
			if minimum > 0 && minimum <= 21 {
				want = fmt.Sprintf("Path samples  %d configured; effective stations 21, beacons 11\r\n              Override inactive: not above station minimum 21\r\n", minimum)
			}
			if minimum == 30 {
				want = "Path samples  30 (user minimum); stations 30, beacons 30\r\n              Cluster minimums: stations 21, beacons 11\r\n"
			}
			if !strings.Contains(got, want) || !bytes.Equal(before, presetDiskBytes(t, path)) {
				t.Fatalf("restored settings misdescribed or rewritten:\n%s", got)
			}
			runtimeMinimum := 0
			if minimum > 21 {
				runtimeMinimum = minimum
			}
			if c.pathSnapshot().pathMinObservationCount != runtimeMinimum {
				t.Fatal("readback changed runtime preparation")
			}
		})
	}
}

func TestHumanFinalResponseExactAndPlusOne(t *testing.T) {
	s, c := readbackTestClient()
	// Settings remain lossless for restored values; use that retained surface
	// rather than manufacturing retired unbounded category collections.
	low, high := 0, 65537
	for low+1 < high {
		middle := (low + high) / 2
		c.configuredSettings.Grid = strings.Repeat("A", middle)
		_, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
		if err == nil {
			low = middle
		} else if errors.Is(err, errReadbackTooLarge) {
			high = middle
		} else {
			t.Fatal(err)
		}
	}
	c.configuredSettings.Grid = strings.Repeat("A", low)
	got, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.callsign += strings.Repeat("A", 65536-len(got))
	got, err = s.renderHumanReadback(c, "SETTINGS", "", time.Second)
	if err != nil || len(got) != 65536 {
		t.Fatalf("exact boundary: %d, %v", len(got), err)
	}
	assertHumanWire(t, got)
	c.callsign += "A"
	if got, err = s.renderHumanReadback(c, "SETTINGS", "", time.Second); got != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("limit+1 returned partial success")
	}
	if !s.handleHumanReadback(c, "SHOW SETTINGS") {
		t.Fatal("size-error command unhandled")
	}
	message := <-c.controlChan
	if string(message.raw) != "Readback failed: response exceeds 65,536 bytes.\r\n"+readbackFooterLiteral || message.readback.epoch == 0 {
		t.Fatal("size error differs or lacks pause completion")
	}
}

func TestHumanSettingsAvailabilityAndPauseDescriptions(t *testing.T) {
	s, c := readbackTestClient()
	s.dedupeMedEnabled, s.dedupeSlowEnabled = true, true
	c.setDedupePolicy(dedupePolicySlow)
	c.configuredSettings.DedupePolicy = "SLOW"
	c.gridCell, c.gridCoarseCell = 1, 2
	if !s.handleHumanReadback(c, "SHOW SETTINGS") {
		t.Fatal("unhandled")
	}
	got := string((<-c.controlChan).raw)
	assertHumanWire(t, got)
	if !strings.Contains(got, "Dedupe        SLOW\r\n") {
		t.Fatal("ordinary dedupe policy misdescribed")
	}
	s.dedupeMedEnabled, s.dedupeSlowEnabled = false, false
	c.recordProtected = true
	c.readPauseUntilUnixNano.Store(s.now().Add(time.Hour).UnixNano())
	if !s.handleHumanReadback(c, "SHOW SETTINGS") {
		t.Fatal("unhandled")
	}
	got = string((<-c.controlChan).raw)
	for _, want := range []string{"Dedupe        SLOW; secondary duplicate suppression disabled\r\n",
		"Persistence   Temporary defaults; changes will not be saved\r\n", "Existing pause: 3600s remaining\r\n"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	disabled := pathreliability.DefaultConfig()
	disabled.Enabled = false
	s.pathPredictor = pathreliability.NewPredictor(disabled, []string{"20m"})
	got, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
	if err != nil || !strings.Contains(got, "Path samples  DEFAULT; prediction disabled\r\n") {
		t.Fatal("disabled prediction misdescribed")
	}
}

func TestHumanSavedMixedCaseSettingsRemainExact(t *testing.T) {
	s := presetTestServer(t)
	cfg := filter.ConfigurationFromFilter(testRuleFilter(t), filter.SettingsConfiguration{Dialect: "go", NoiseClass: "MixedCase"})
	if err := filter.SaveConfiguration("W1ABC-1", cfg, nil, nil); err != nil {
		t.Fatal(err)
	}
	c := configurationTestClient(s, "W1ABC-1")
	if _, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := s.renderHumanReadback(c, "SETTINGS", "", time.Second)
	if err != nil || !strings.Contains(got, "Noise         MixedCase\r\n") || strings.Contains(got, "MIXEDCASE") {
		t.Fatalf("saved case lost: %v\n%s", err, got)
	}
	assertHumanWire(t, got)
	if _, err := os.Stat(filepath.Join(filter.UserDataDir, c.callsign+".yaml")); err != nil {
		t.Fatal(err)
	}
}
