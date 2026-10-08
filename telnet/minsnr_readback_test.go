package telnet

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func TestMinSNRHumanReadbacks(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.MinSNR = map[string]int{"CW": 0, "FT8": -10, "RETIRED": 3}
	for _, category := range []string{"", "FULL", "MINSNR"} {
		response, err := s.renderHumanReadback(c, "FILTER", category, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		assertHumanWire(t, response)
		for _, literal := range []string{"3 configured modes; 1 inactive", "CW >= 0 dB", "FT8 >= -10 dB", "RETIRED >= 3 dB (inactive)", "Human spots and spots without SNR are exempt"} {
			if !strings.Contains(response, literal) {
				t.Fatalf("%s omits %q: %s", category, literal, response)
			}
		}
		if strings.Index(response, "CW >=") > strings.Index(response, "FT8 >=") || strings.Index(response, "FT8 >=") > strings.Index(response, "RETIRED >=") {
			t.Fatal("minimums are not sorted")
		}
	}
}

func TestMinSNRMachineActivityStatus(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.filter.MinSNR = map[string]int{"CW": 0, "FT8": -10, "RETIRED": 3, "FT-8": 17}
	for _, resource := range []string{"FILTER", "CONFIG", "SETTINGS", "CAPABILITIES"} {
		response, err := s.renderYAMLReadbackVersion(c, resource, "snr-status", "session-0", 3)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Status struct {
				MinSNR *readbackMinSNRStatus `yaml:"min_snr"`
			} `yaml:"status"`
		}
		if err := yaml.Unmarshal([]byte(response), &envelope); err != nil {
			t.Fatal(err)
		}
		want := &readbackMinSNRStatus{ConfiguredCount: 4, InactiveCount: 2, InactiveModes: []string{"FT-8", "RETIRED"}}
		if !reflect.DeepEqual(envelope.Status.MinSNR, want) {
			t.Fatalf("%s activity status: %+v", resource, envelope.Status.MinSNR)
		}
		for _, version := range []int{1, 2} {
			legacy, err := s.renderYAMLReadbackVersion(c, resource, "snr-status", "session-0", version)
			if err != nil || strings.Contains(legacy, "min_snr") || strings.Contains(legacy, "inactive_modes") {
				t.Fatalf("schema %d %s status leaked: %v %s", version, resource, err, legacy)
			}
		}
	}
	// The map fits its raw bound, but a complete schema3 envelope repeats the
	// dormant key in status. Admission must include both copies before commit.
	c.filter.MinSNR = map[string]int{strings.Repeat("A", maxYAMLBytes/2): 0}
	if _, err := s.renderYAMLReadbackVersion(c, "FILTER", "", "session-0", 3); !errors.Is(err, errReadbackTooLarge) {
		t.Fatalf("GET admitted oversized schema3 activity: %v", err)
	}
	cfg := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
	if err := s.configurationReadbackFitsVersion(c, cfg, nil, 3); !errors.Is(err, errReadbackTooLarge) {
		t.Fatalf("candidate admitted oversized schema3 activity: %v", err)
	}
	if err := s.configurationReadbackFitsVersion(c, cfg, nil, 2); err != nil {
		t.Fatalf("hidden status changed schema2 admission: %v", err)
	}
}

func TestMinSNRReadbackBounds(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	// Stored mode names have a separate raw-byte bound; the final rendered
	// response still needs its own complete framing/continuation admission.
	c.filter.MinSNR = map[string]int{strings.Repeat("A", filter.MaxMinSNRKeyBytes): 0}
	if response, err := s.renderHumanReadback(c, "FILTER", "MINSNR", time.Second); err == nil || response != "" {
		t.Fatal("oversized rendered response was partially returned")
	}
	c.filter.MinSNR = map[string]int{strings.Repeat("A", 300): 0}
	response, err := s.renderHumanReadback(c, "FILTER", "MINSNR", time.Second)
	if err != nil || !strings.Contains(response, "(inactive)") || !strings.Contains(response, "A + joins quoted pieces") {
		t.Fatal(response, err)
	}
	assertHumanWire(t, response)
}
