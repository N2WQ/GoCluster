// File role: Pure bounded v6 DTO rendering from a coherent prepared snapshot.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
)

type v6ReadbackRuleCounts struct {
	Nodes  int `yaml:"nodes"`
	Atoms  int `yaml:"atoms"`
	Values int `yaml:"values"`
}

type v6ReadbackRuleStatus struct {
	ConfiguredCount int                     `yaml:"configured_count"`
	Program         v6ReadbackRuleCounts    `yaml:"program"`
	Entries         []filter.RuleDefinition `yaml:"entries"`
}

type v6ReadbackStatus struct {
	configurationReadbackStatus `yaml:",inline"`
	Rules                       v6ReadbackRuleStatus `yaml:"rules"`
}

func v6RuleReadback(set *filter.PreparedRuleSet) v6ReadbackRuleStatus {
	counts := set.Counts()
	entries := set.Definitions()
	if entries == nil {
		entries = []filter.RuleDefinition{}
	}
	return v6ReadbackRuleStatus{
		ConfiguredCount: counts.Rules,
		Program:         v6ReadbackRuleCounts(counts.Program),
		Entries:         entries,
	}
}

// The coordinator supplies coherent definitions/programs and captured metadata.
// Rendering only reads the immutable set; it never prepares, compiles, or derives
// preset association from mutable session state.
func renderV6YAMLReadback(cfg filter.V6Configuration, set *filter.PreparedRuleSet, status configurationReadbackStatus, resource, requestID, revision string) (string, error) {
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
		configuration = v6MachineCapabilities()
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
		Status        v6ReadbackStatus `yaml:"status"`
	}{
		filter.CurrentConfigurationVersion, requestID, resource, revision, configuration,
		v6ReadbackStatus{status, v6RuleReadback(set)},
	})
}

func renderV6YAMLCommandError(resource, requestID, revision, code, message string) string {
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
		}{filter.CurrentConfigurationVersion, requestID, resource, revision, documentError{code, message}})
		if err == nil {
			return response
		}
	}
	return "---\r\nschema_version: 6\r\nrequest_id: \"\"\r\nresource: \"\"\r\nerror:\r\n  code: response_too_large\r\n  message: Error details exceed the response limit.\r\n...\r\n"
}

func renderV6YAMLCommandSuccess(resource, requestID, revision, verb string, applied, persisted bool) (string, error) {
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
	}{filter.CurrentConfigurationVersion, requestID, resource, revision, commandResult{verb, true, applied, persisted}})
}
