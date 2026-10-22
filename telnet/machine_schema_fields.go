// File role: Shared strict settings vocabulary and bool-or-DEFAULT scalars.
package telnet

import (
	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

var machineSettingFields = [...]string{
	"dialect", "grid", "noise_class", "dedupe_policy", "path_min_observation_count", "solar_summary_minutes",
}

func machineDefaultBoolean(node *yaml.Node, path string) (filter.DefaultBool, error) {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!str" && node.Value == "DEFAULT" {
		return filter.DefaultBoolDefault, nil
	}
	value, err := machineBoolean(node, path)
	if err != nil {
		return 0, schemaError(path, "expected true, false or DEFAULT")
	}
	if value {
		return filter.DefaultBoolTrue, nil
	}
	return filter.DefaultBoolFalse, nil
}
