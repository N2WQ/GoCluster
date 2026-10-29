package telnet

import (
	"errors"
	"strconv"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

// Only on a failed shared validation, narrow its diagnostic to known schema
// fields. The fixed depth/row limits prevent malformed nesting or attacker-owned
// field names from creating unbounded paths. Admission remains entirely owned
// by validateMachineNodes, including aliases, nulls, tags and duplicate keys.
func v6MachineNodeErrorPath(node *yaml.Node, path string, depth int, cause error) error {
	if depth < 7 {
		switch node.Kind {
		case yaml.DocumentNode:
			if len(node.Content) == 1 {
				return v6MachineNodeErrorPath(node.Content[0], path, depth+1, cause)
			}
		case yaml.MappingNode:
			for i := 0; i+1 < len(node.Content); i += 2 {
				key, child := node.Content[i], node.Content[i+1]
				if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || !v6MachineKnownField(key.Value) {
					continue
				}
				if err := validateMachineNodes(child); err != nil {
					childPath := key.Value
					if path != "document" {
						childPath = path + "." + key.Value
					}
					return v6MachineNodeErrorPath(child, childPath, depth+1, err)
				}
			}
		case yaml.SequenceNode:
			for i, child := range node.Content[:min(len(node.Content), filter.MaxRuleSlots)] {
				if err := validateMachineNodes(child); err != nil {
					return v6MachineNodeErrorPath(child, path+"["+strconv.Itoa(i)+"]", depth+1, err)
				}
			}
		}
	}
	var problem *machineSchemaError
	if errors.As(cause, &problem) {
		return schemaError(path, problem.reason)
	}
	return cause
}

func v6MachineKnownField(name string) bool {
	switch name {
	case "schema_version", "request_id", "if_revision", "configuration", "filters", "settings", "slot", "action", "expr":
		return true
	}
	for _, field := range machineV6FilterFields {
		if name == field {
			return true
		}
	}
	for _, field := range machineSettingFields {
		if name == field {
			return true
		}
	}
	return false
}
