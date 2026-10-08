// File role: Validates machine YAML envelopes before detaching writable data.
// Supported schema versions select vocabulary; presence masks preserve PATCH
// omissions without retaining parser nodes in client state.
package telnet

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

// machineRequest contains only detached writable values and fixed presence
// masks. No Node or decoder survives decoding, so the preparation permit can be
// released before transaction ownership, disk I/O or publication is awaited.
type machineRequest struct {
	SchemaVersion int
	RequestID     string
	IfRevision    string
	Configuration filter.Configuration
	resource      string
	filterFields  uint32
	settingFields uint8
	ruleFields    [16]uint8
}

// machineSchemaError names a schema-controlled path, never an unbounded value
// from the request. This keeps malformed-document error responses bounded.
type machineSchemaError struct {
	path   string
	reason string
}

func (e *machineSchemaError) Error() string { return e.path + ": " + e.reason }

func schemaError(path, reason string) error {
	return &machineSchemaError{path: path, reason: reason}
}

func decodeMachineRequest(body []byte, command machineCommand) (machineRequest, error) {
	request := machineRequest{resource: command.Resource}
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
	// Retain an unambiguous supported envelope version for error replies even
	// when a later node is invalid. This does not admit any request values.
	if document.Kind == yaml.DocumentNode && len(document.Content) == 1 {
		root := document.Content[0]
		if root.Kind == yaml.MappingNode && len(root.Content)%2 == 0 {
			var version *yaml.Node
			duplicate := false
			for i := 0; i < len(root.Content); i += 2 {
				if root.Content[i].Value == "schema_version" {
					duplicate = version != nil
					if duplicate {
						break
					}
					version = root.Content[i+1]
				}
			}
			if version != nil && !duplicate {
				if value, err := machineInteger(version, "schema_version", false); err == nil && (value == 1 || value == 2 || value == 3) {
					request.SchemaVersion = value
				}
			}
		}
	}
	if err := validateMachineNodes(&document); err != nil {
		return request, err
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return request, schemaError("document", "expected a mapping")
	}
	envelope, err := machineObject(document.Content[0], "document", []string{"schema_version", "request_id", "if_revision", "configuration"}, false)
	if err != nil {
		return request, err
	}
	if request.RequestID, err = requiredMachineString(envelope, "request_id", "request_id"); err != nil {
		return request, err
	}
	if !validMachineRequestID(request.RequestID) {
		request.RequestID = ""
		return request, schemaError("request_id", "expected 1-32 ASCII letters, digits or hyphens")
	}
	version := envelope["schema_version"]
	if version == nil {
		return request, schemaError("schema_version", "required field is missing")
	}
	if request.SchemaVersion, err = machineInteger(version, "schema_version", false); err != nil {
		return request, err
	}
	if request.SchemaVersion != 1 && request.SchemaVersion != 2 && request.SchemaVersion != 3 {
		return request, schemaError("schema_version", "unsupported schema version")
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
	err = request.decodeConfiguration(configuration, command.Verb != "PATCH")
	return request, err
}

// validateMachineNodes makes one iterative pass without decoding values or
// expanding aliases. Duplicate checks use a hash table per mapping, avoiding
// the YAML typed decoder's quadratic duplicate-key comparisons.
func validateMachineNodes(document *yaml.Node) error {
	stack := []*yaml.Node{document}
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if node.Anchor != "" || node.Alias != nil || node.Kind == yaml.AliasNode {
			return schemaError("document", "anchors and aliases are not supported")
		}
		if node.Tag == "!!merge" {
			return schemaError("document", "merge keys are not supported")
		}
		if node.Tag == "!!null" {
			return schemaError("document", "null values are not supported")
		}
		if !ordinaryMachineTag(node.Tag) {
			return schemaError("document", "custom tags are not supported")
		}
		if node.Kind == yaml.MappingNode {
			if len(node.Content)%2 != 0 {
				return schemaError("document", "invalid mapping")
			}
			seen := make(map[string]struct{}, len(node.Content)/2)
			for i := 0; i < len(node.Content); i += 2 {
				key := node.Content[i]
				if key.Kind != yaml.ScalarNode {
					return schemaError("document", "mapping keys must be scalar values")
				}
				if _, duplicate := seen[key.Value]; duplicate {
					return schemaError("document", "duplicate mapping key")
				}
				seen[key.Value] = struct{}{}
			}
		}
		stack = append(stack, node.Content...)
	}
	return nil
}

func ordinaryMachineTag(tag string) bool {
	switch tag {
	case "", "!!map", "!!seq", "!!str", "!!bool", "!!int", "!!float", "!!timestamp", "!!binary":
		return true
	default:
		return false
	}
}

func machineObject(node *yaml.Node, path string, allowed []string, complete bool) (map[string]*yaml.Node, error) {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
		return nil, schemaError(path, "expected a mapping")
	}
	values := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return nil, schemaError(path, "field names must be strings")
		}
		known := false
		for _, field := range allowed {
			if key.Value == field {
				known = true
				break
			}
		}
		if !known {
			return nil, schemaError(path, "unknown or read-only field")
		}
		values[key.Value] = node.Content[i+1]
	}
	if complete {
		for _, field := range allowed {
			if values[field] == nil {
				return nil, schemaError(path+"."+field, "required field is missing")
			}
		}
	}
	return values, nil
}

func requiredMachineString(values map[string]*yaml.Node, field, path string) (string, error) {
	node := values[field]
	if node == nil {
		return "", schemaError(path, "required field is missing")
	}
	return machineString(node, path)
}

func machineString(node *yaml.Node, path string) (string, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return "", schemaError(path, "expected a string")
	}
	return node.Value, nil
}

func machineBoolean(node *yaml.Node, path string) (bool, error) {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!bool" {
		switch node.Value {
		case "true", "True", "TRUE":
			return true, nil
		case "false", "False", "FALSE":
			return false, nil
		}
	}
	return false, schemaError(path, "expected a boolean")
}

func machineInteger(node *yaml.Node, path string, allowString bool) (int, error) {
	if node.Kind != yaml.ScalarNode || (node.Tag != "!!int" && (!allowString || node.Tag != "!!str")) {
		return 0, schemaError(path, "expected an integer")
	}
	value, err := strconv.ParseInt(strings.ReplaceAll(node.Value, "_", ""), 0, strconv.IntSize)
	if err != nil {
		return 0, schemaError(path, "expected an integer within the supported range")
	}
	return int(value), nil
}

func validMachineRevision(revision string) bool {
	if len(revision) < 1 || len(revision) > 128 {
		return false
	}
	for i := range revision {
		if revision[i] < 0x20 || revision[i] > 0x7e {
			return false
		}
	}
	return true
}

func (r *machineRequest) decodeConfiguration(node *yaml.Node, complete bool) error {
	switch r.resource {
	case "FILTER":
		return r.decodeFilters(node, "configuration", complete)
	case "SETTINGS":
		return r.decodeSettings(node, "configuration", complete)
	case "CONFIG":
		containers, err := machineObject(node, "configuration", []string{"filters", "settings"}, complete)
		if err != nil {
			return err
		}
		if filters := containers["filters"]; filters != nil {
			if err := r.decodeFilters(filters, "configuration.filters", complete); err != nil {
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
