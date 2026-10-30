// File role: Strict schema-seven envelopes and explicit resource ownership.
// Requests retain detached values/presence only; no YAML nodes escape decoding.
package telnet

import (
	"bytes"
	"errors"
	"io"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

const (
	v7BankGO = 1 << iota
	v7BankCC
)

type v7MachineRequest struct {
	SchemaVersion int
	RequestID     string
	IfRevision    string
	Configuration filter.V7Configuration
	resource      string
	verb          string
	dialect       string
	bankFields    uint8
	goPatch       v6MachineRequest
	ccPatch       v7MachineCCPatch
}

func decodeV7MachineRequest(body []byte, command machineCommand, resolve func(string) (int, error)) (v7MachineRequest, error) {
	r := v7MachineRequest{resource: command.Resource, verb: command.Verb}
	if len(body) > maxV7YAMLBytes {
		return r, schemaError("document", "exceeds 262144-byte limit")
	}
	if command.SchemaVersion != 7 || (command.Verb != "PUT" && command.Verb != "PATCH" && command.Verb != "VALIDATE") || !machineResourceAllowed(command.Verb, command.Resource) {
		return r, schemaError("document", "requires an explicit supported SCHEMA 7 upload header")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	var document, extra yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return r, schemaError("document", "invalid YAML syntax")
	}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return r, schemaError("document", "exactly one YAML document is required")
	}
	if err := validateMachineNodes(&document); err != nil {
		return r, err
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return r, schemaError("document", "expected a mapping")
	}
	fields, err := machineObject(document.Content[0], "document", []string{"schema_version", "request_id", "if_revision", "configuration"}, false)
	if err != nil {
		return r, err
	}
	if err := r.decodeEnvelope(fields); err != nil {
		return r, err
	}
	if fields["configuration"] == nil {
		return r, schemaError("configuration", "required field is missing")
	}
	err = r.decodeConfiguration(fields["configuration"], resolve)
	return r, err
}

func (r *v7MachineRequest) decodeEnvelope(fields map[string]*yaml.Node) error {
	version := fields["schema_version"]
	if version == nil {
		return schemaError("schema_version", "required field is missing")
	}
	var err error
	r.SchemaVersion, err = machineInteger(version, "schema_version")
	if err != nil {
		return err
	}
	if r.SchemaVersion != 7 {
		return schemaError("schema_version", "must match the explicit SCHEMA 7 header")
	}
	r.RequestID, err = requiredMachineString(fields, "request_id", "request_id")
	if err != nil {
		return err
	}
	if !validMachineRequestID(r.RequestID) {
		r.RequestID = ""
		return schemaError("request_id", "expected 1-32 ASCII letters, digits or hyphens")
	}
	if revision := fields["if_revision"]; revision != nil {
		r.IfRevision, err = machineString(revision, "if_revision")
		if err == nil && !validMachineRevision(r.IfRevision) {
			err = schemaError("if_revision", "expected 1-128 printable ASCII bytes")
		}
		return err
	}
	if r.verb != "VALIDATE" {
		return schemaError("if_revision", "required field is missing")
	}
	return nil
}

func (r *v7MachineRequest) decodeConfiguration(node *yaml.Node, resolve func(string) (int, error)) error {
	complete := r.verb != "PATCH"
	switch r.resource {
	case "FILTER":
		fields, err := machineObject(node, "configuration", []string{"dialect", "filters"}, true)
		if err != nil {
			return err
		}
		r.dialect, err = machineString(fields["dialect"], "configuration.dialect")
		if err != nil {
			return err
		}
		return r.decodeBank(r.dialect, fields["filters"], "configuration.filters", complete, resolve)
	case "SETTINGS":
		return r.decodeSettings(node, "configuration", complete)
	case "CONFIG":
		fields, err := machineObject(node, "configuration", []string{"filters", "settings"}, complete)
		if err != nil {
			return err
		}
		if banks := fields["filters"]; banks != nil {
			entries, err := machineObject(banks, "configuration.filters", []string{"go", "cc"}, complete)
			if err != nil {
				return err
			}
			for _, dialect := range []string{"go", "cc"} {
				if bank := entries[dialect]; bank != nil {
					if err := r.decodeBank(dialect, bank, "configuration.filters."+dialect, complete, resolve); err != nil {
						return err
					}
				}
			}
		}
		if settings := fields["settings"]; settings != nil {
			return r.decodeSettings(settings, "configuration.settings", complete)
		}
		return nil
	default:
		return schemaError("configuration", "unsupported resource")
	}
}

func (r *v7MachineRequest) decodeBank(dialect string, node *yaml.Node, path string, complete bool, resolve func(string) (int, error)) error {
	switch dialect {
	case "go":
		r.bankFields |= v7BankGO
		if err := r.goPatch.decodeFilters(node, path, complete, resolve); err != nil {
			return err
		}
		r.Configuration.Filters.GO = r.goPatch.Configuration.Filters
		return nil
	case "cc":
		r.bankFields |= v7BankCC
		if err := r.ccPatch.decode(node, path, complete); err != nil {
			return err
		}
		r.Configuration.Filters.CC = r.ccPatch.configuration
		return nil
	default:
		return schemaError("configuration.dialect", "expected explicit lowercase go or cc")
	}
}

func (r *v7MachineRequest) decodeSettings(node *yaml.Node, path string, complete bool) error {
	if err := r.goPatch.decodeSettings(node, path, complete); err != nil {
		return err
	}
	r.Configuration.Settings = r.goPatch.Configuration.Settings
	return nil
}
