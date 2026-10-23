// File role: Strict detached v5 machine proposals; no transaction or publication.
package telnet

import (
	"bytes"
	"errors"
	"io"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

var machineV5FilterFields = [...]string{
	"rules", "nearby_enabled", "include_beacons", "allow_wwv", "allow_wcy",
	"allow_announce", "allow_self", "allow_toxic",
}

// Presence is independent of zero values. Only a supplied rules list owns a
// prepared replacement; omitted rules borrow the caller's immutable snapshot.
type v5MachineRequest struct {
	SchemaVersion int
	RequestID     string
	IfRevision    string
	Configuration filter.V5Configuration
	resource      string
	filterFields  uint16
	settingFields uint8
	preparedRules *filter.PreparedRuleSet
}

func decodeV5MachineRequest(body []byte, command machineCommand, resolve func(string) (int, error)) (v5MachineRequest, error) {
	request := v5MachineRequest{resource: command.Resource}
	if len(body) > maxYAMLBytes {
		return request, schemaError("document", "exceeds 65536-byte limit")
	}
	if command.Verb != "PUT" && command.Verb != "PATCH" && command.Verb != "VALIDATE" {
		return request, schemaError("document", "command does not receive YAML")
	}
	if !machineResourceAllowed(command.Verb, command.Resource) {
		return request, schemaError("configuration", "unsupported resource")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	var document, extra yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return request, schemaError("document", "invalid YAML syntax")
	}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return request, schemaError("document", "exactly one YAML document is required")
	}
	if err := validateMachineNodes(&document); err != nil {
		return request, v5MachineNodeErrorPath(&document, "document", 0, err)
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return request, schemaError("document", "expected a mapping")
	}
	envelope, err := machineObject(document.Content[0], "document",
		[]string{"schema_version", "request_id", "if_revision", "configuration"}, false)
	if err != nil {
		return request, err
	}
	version := envelope["schema_version"]
	if version == nil {
		return request, schemaError("schema_version", "required field is missing")
	}
	if request.SchemaVersion, err = machineInteger(version, "schema_version", false); err != nil {
		return request, err
	}
	if request.SchemaVersion != filter.MVPConfigurationVersion {
		return request, schemaError("schema_version", "expected explicit schema version 5")
	}
	if request.RequestID, err = requiredMachineString(envelope, "request_id", "request_id"); err != nil {
		return request, err
	}
	if !validMachineRequestID(request.RequestID) {
		request.RequestID = ""
		return request, schemaError("request_id", "expected 1-32 ASCII letters, digits or hyphens")
	}
	if revision := envelope["if_revision"]; revision != nil {
		if request.IfRevision, err = machineString(revision, "if_revision"); err != nil {
			return request, err
		}
		if !validMachineRevision(request.IfRevision) {
			return request, schemaError("if_revision", "expected 1-128 printable ASCII bytes")
		}
	} else if command.Verb != "VALIDATE" {
		return request, schemaError("if_revision", "required field is missing")
	}
	configuration := envelope["configuration"]
	if configuration == nil {
		return request, schemaError("configuration", "required field is missing")
	}
	err = request.decodeConfiguration(configuration, command.Verb != "PATCH", resolve)
	return request, err
}

func (r *v5MachineRequest) decodeConfiguration(node *yaml.Node, complete bool, resolve func(string) (int, error)) error {
	switch r.resource {
	case "FILTER":
		return r.decodeFilters(node, "configuration", complete, resolve)
	case "SETTINGS":
		return r.decodeSettings(node, "configuration", complete)
	case "CONFIG":
		containers, err := machineObject(node, "configuration", []string{"filters", "settings"}, complete)
		if err != nil {
			return err
		}
		if filters := containers["filters"]; filters != nil {
			if err := r.decodeFilters(filters, "configuration.filters", complete, resolve); err != nil {
				return err
			}
		}
		if settings := containers["settings"]; settings != nil {
			return r.decodeSettings(settings, "configuration.settings", complete)
		}
		return nil
	default:
		return schemaError("configuration", "unsupported resource")
	}
}

func (r *v5MachineRequest) decodeFilters(node *yaml.Node, path string, complete bool, resolve func(string) (int, error)) error {
	fields, err := machineObject(node, path, machineV5FilterFields[:], complete)
	if err != nil {
		return err
	}
	f := &r.Configuration.Filters
	toggles := [6]*filter.DefaultBool{&f.IncludeBeacons, &f.AllowWWV, &f.AllowWCY, &f.AllowAnnounce, &f.AllowSelf, &f.AllowToxic}
	for i, name := range machineV5FilterFields {
		field := fields[name]
		if field == nil {
			continue
		}
		fieldPath := path + "." + name
		switch i {
		case 0:
			r.preparedRules, err = decodeV5MachineRules(field, fieldPath, resolve)
			if err == nil {
				*f = f.WithRuleSet(r.preparedRules)
			}
		case 1:
			f.NearbyEnabled, err = machineBoolean(field, fieldPath)
		default:
			*toggles[i-2], err = machineDefaultBoolean(field, fieldPath)
		}
		if err != nil {
			return err
		}
		r.filterFields |= 1 << uint(i)
	}
	return nil
}

func (r *v5MachineRequest) decodeSettings(node *yaml.Node, path string, complete bool) error {
	fields, err := machineObject(node, path, machineSettingFields[:], complete)
	if err != nil {
		return err
	}
	s := &r.Configuration.Settings
	stringsTargets := [4]*string{&s.Dialect, &s.Grid, &s.NoiseClass, &s.DedupePolicy}
	integerTargets := [2]*int{&s.PathMinObservationCount, &s.SolarSummaryMinutes}
	for i, name := range machineSettingFields {
		field := fields[name]
		if field == nil {
			continue
		}
		fieldPath := path + "." + name
		if i < 4 {
			*stringsTargets[i], err = machineString(field, fieldPath)
		} else {
			*integerTargets[i-4], err = machineInteger(field, fieldPath, false)
		}
		if err != nil {
			return err
		}
		r.settingFields |= 1 << uint(i)
	}
	return nil
}

// apply never compiles or merges list entries. Omitted collections are borrowed
// until the coordinator prepares and detaches the complete candidate. A supplied
// list reattaches private programs, not potentially edited public definitions.
func (r v5MachineRequest) apply(before filter.V5Configuration) (filter.V5Configuration, error) {
	if r.resource != "FILTER" && r.resource != "SETTINGS" && r.resource != "CONFIG" {
		return before, schemaError("configuration", "unsupported resource")
	}
	f, desired := &before.Filters, r.Configuration.Filters
	if r.filterFields&1 != 0 {
		*f = f.WithRuleSet(r.preparedRules)
	}
	if r.filterFields&(1<<1) != 0 {
		f.NearbyEnabled = desired.NearbyEnabled
	}
	toggles := [6]*filter.DefaultBool{&f.IncludeBeacons, &f.AllowWWV, &f.AllowWCY, &f.AllowAnnounce, &f.AllowSelf, &f.AllowToxic}
	proposed := [6]filter.DefaultBool{desired.IncludeBeacons, desired.AllowWWV, desired.AllowWCY, desired.AllowAnnounce, desired.AllowSelf, desired.AllowToxic}
	for i := range toggles {
		if r.filterFields&(1<<uint(i+2)) != 0 {
			*toggles[i] = proposed[i]
		}
	}
	s, wanted := &before.Settings, r.Configuration.Settings
	strings := [4]*string{&s.Dialect, &s.Grid, &s.NoiseClass, &s.DedupePolicy}
	values := [4]string{wanted.Dialect, wanted.Grid, wanted.NoiseClass, wanted.DedupePolicy}
	for i := range strings {
		if r.settingFields&(1<<uint(i)) != 0 {
			*strings[i] = values[i]
		}
	}
	if r.settingFields&(1<<4) != 0 {
		s.PathMinObservationCount = wanted.PathMinObservationCount
	}
	if r.settingFields&(1<<5) != 0 {
		s.SolarSummaryMinutes = wanted.SolarSummaryMinutes
	}
	return before, nil
}
