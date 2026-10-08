// File role: Enforces MINSNR disk-version and allocation bounds on YAML nodes
// before yaml.v3 constructs a typed threshold map. Legacy merges retain their
// decoder contract, including unused anchors and quoted merge-looking fields.
package filter

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Only effective legacy merge sources are followed; unrelated anchor fields
// cannot add a new failure to historical migration. Each node is visited once
// so recursive alias graphs terminate before the decoder reports the cycle.
func validateStoredMinSNRFields(node *yaml.Node, version int) error {
	pending := []*yaml.Node{node}
	seen := make(map[*yaml.Node]bool)
	for len(pending) != 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current == nil || seen[current] {
			continue
		}
		seen[current] = true
		switch current.Kind {
		case yaml.AliasNode:
			pending = append(pending, current.Alias)
		case yaml.SequenceNode:
			pending = append(pending, current.Content...)
		case yaml.MappingNode:
			if err := validateStoredMinSNRMapping(current, version, &pending); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateStoredMinSNRMapping(node *yaml.Node, version int, pending *[]*yaml.Node) error {
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		// An alias of a << scalar is an ordinary field to yaml.v3, not a merge.
		if key.Kind == yaml.ScalarNode && key.Value == "<<" && key.Tag == "!!merge" {
			*pending = append(*pending, node.Content[i+1])
			continue
		}
		if key.Kind == yaml.AliasNode && key.Alias != nil {
			key = key.Alias
		}
		if key.Value != "min_snr" {
			continue
		}
		if version < CurrentConfigurationVersion {
			return fmt.Errorf("min_snr requires configuration version %d", CurrentConfigurationVersion)
		}
		if err := validateStoredMinSNRMap(node.Content[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func validateStoredMinSNRMap(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	if node.Kind != yaml.MappingNode || len(node.Content)/2 > MaxMinSNREntries {
		return fmt.Errorf("invalid bounded min_snr map: maximum %d entries", MaxMinSNREntries)
	}
	remaining := MaxMinSNRKeyBytes
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind == yaml.AliasNode && key.Alias != nil {
			key = key.Alias
		}
		if len(key.Value) > remaining {
			return fmt.Errorf("min_snr exceeds %d mode-key bytes", MaxMinSNRKeyBytes)
		}
		remaining -= len(key.Value)
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || !ValidMinSNRModeKey(key.Value) {
			return fmt.Errorf("min_snr contains an invalid mode key")
		}
	}
	return nil
}
