// File role: Validates machine YAML envelopes before detaching writable data.
// Supported schema versions select vocabulary; presence masks preserve PATCH
// omissions without retaining parser nodes in client state.
package telnet

import (
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

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
