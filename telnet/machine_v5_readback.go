// File role: Pure bounded v5 DTO rendering from a coherent prepared snapshot.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
)

type v5ReadbackRuleCounts struct {
	Nodes  int `yaml:"nodes"`
	Atoms  int `yaml:"atoms"`
	Values int `yaml:"values"`
}

type v5ReadbackRule struct {
	Slot          int               `yaml:"slot"`
	Action        filter.RuleAction `yaml:"action"`
	EffectiveExpr string            `yaml:"effective_expr"`
	Suspended     bool              `yaml:"suspended"`
}

type v5ReadbackRuleStatus struct {
	ConfiguredCount int                  `yaml:"configured_count"`
	EffectiveCount  int                  `yaml:"effective_count"`
	SuspendedCount  int                  `yaml:"suspended_count"`
	Full            v5ReadbackRuleCounts `yaml:"full"`
	Nearby          v5ReadbackRuleCounts `yaml:"nearby"`
	Retained        v5ReadbackRuleCounts `yaml:"retained"`
	Entries         []v5ReadbackRule     `yaml:"entries"`
}

type v5ReadbackStatus struct {
	configurationReadbackStatus `yaml:",inline"`
	Rules                       v5ReadbackRuleStatus `yaml:"rules"`
}

func v5RuleReadback(set *filter.PreparedRuleSet, nearby bool) v5ReadbackRuleStatus {
	counts := set.Counts()
	status := v5ReadbackRuleStatus{
		ConfiguredCount: counts.Rules,
		Full:            v5ReadbackRuleCounts(counts.Normal),
		Nearby:          v5ReadbackRuleCounts(counts.Nearby),
		Retained:        v5ReadbackRuleCounts(counts.Retained),
		Entries:         make([]v5ReadbackRule, 0, counts.Rules),
	}
	for _, row := range set.Readback(nearby) {
		status.Entries = append(status.Entries, v5ReadbackRule{
			Slot: row.Slot, Action: row.Action, EffectiveExpr: row.EffectiveExpr, Suspended: row.Suspended,
		})
		if row.Suspended {
			status.SuspendedCount++
		} else {
			status.EffectiveCount++
		}
	}
	return status
}

// The coordinator supplies coherent definitions/programs and captured metadata.
// Rendering only reads the immutable set; it never prepares, compiles, or derives
// preset association from mutable session state.
func renderV5YAMLReadback(cfg filter.V5Configuration, set *filter.PreparedRuleSet, status configurationReadbackStatus, resource, requestID, revision string) (string, error) {
	resource = strings.ToUpper(resource)
	var configuration any
	switch resource {
	case "FILTER":
		configuration = cfg.Filters
	case "SETTINGS":
		configuration = cfg.Settings
	case "CONFIG":
		configuration = cfg
	case "CAPABILITIES":
		configuration = v5MachineCapabilities()
	default:
		return "", fmt.Errorf("unknown YAML resource %q", resource)
	}
	status.Configured = cfg.Settings
	if len(resource) > maxYAMLBytes || !readbackMetadataFits(status, requestID, revision) ||
		len(cfg.Filters.Rules) > filter.MaxRuleSlots || set.Counts().Rules > filter.MaxRuleSlots {
		return "", errReadbackTooLarge
	}
	return encodeBoundedYAML(struct {
		SchemaVersion int              `yaml:"schema_version"`
		RequestID     string           `yaml:"request_id"`
		Resource      string           `yaml:"resource"`
		Revision      string           `yaml:"revision"`
		Configuration any              `yaml:"configuration"`
		Status        v5ReadbackStatus `yaml:"status"`
	}{
		filter.MVPConfigurationVersion, requestID, resource, revision, configuration,
		v5ReadbackStatus{status, v5RuleReadback(set, cfg.Filters.NearbyEnabled)},
	})
}

func renderV5YAMLCommandError(resource, requestID, revision, code, message string) string {
	type documentError struct {
		Code    string `yaml:"code"`
		Message string `yaml:"message"`
	}
	if len(resource)+len(requestID)+len(revision)+len(code)+len(message) <= maxYAMLBytes {
		response, err := encodeBoundedYAML(struct {
			SchemaVersion int           `yaml:"schema_version"`
			RequestID     string        `yaml:"request_id"`
			Resource      string        `yaml:"resource"`
			Revision      string        `yaml:"revision,omitempty"`
			Error         documentError `yaml:"error"`
		}{filter.MVPConfigurationVersion, requestID, resource, revision, documentError{code, message}})
		if err == nil {
			return response
		}
	}
	return "---\r\nschema_version: 5\r\nrequest_id: \"\"\r\nresource: \"\"\r\nerror:\r\n  code: response_too_large\r\n  message: Error details exceed the response limit.\r\n...\r\n"
}

func renderV5YAMLCommandSuccess(resource, requestID, revision, verb string, applied, persisted bool) (string, error) {
	if (verb != "PUT" && verb != "PATCH" && verb != "VALIDATE") || !machineResourceAllowed(verb, resource) {
		return "", schemaError("configuration", "unsupported command or resource")
	}
	if len(resource)+len(requestID)+len(revision)+len(verb) > maxYAMLBytes {
		return "", errReadbackTooLarge
	}
	type commandResult struct {
		Operation string `yaml:"operation"`
		Valid     bool   `yaml:"valid"`
		Applied   bool   `yaml:"applied"`
		Persisted bool   `yaml:"persisted"`
	}
	return encodeBoundedYAML(struct {
		SchemaVersion int           `yaml:"schema_version"`
		RequestID     string        `yaml:"request_id"`
		Resource      string        `yaml:"resource"`
		Revision      string        `yaml:"revision"`
		Result        commandResult `yaml:"result"`
	}{filter.MVPConfigurationVersion, requestID, resource, revision, commandResult{verb, true, applied, persisted}})
}
