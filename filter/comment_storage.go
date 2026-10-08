// File role: Checks comment field versions and list bounds before typed decoding.
package filter

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

// Follow only effective merge sources, preserving the legacy decoder contract.
func validateStoredCommentFields(node *yaml.Node, version int) error {
	pending := []*yaml.Node{node}
	seen := make(map[*yaml.Node]bool)
	for len(pending) > 0 {
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
			for i := 0; i < len(current.Content); i += 2 {
				key := current.Content[i]
				if key.Kind == yaml.ScalarNode && key.Value == "<<" && key.Tag == "!!merge" {
					pending = append(pending, current.Content[i+1])
					continue
				}
				if key.Kind == yaml.AliasNode && key.Alias != nil {
					key = key.Alias
				}
				if key.Value != "comments" && key.Value != "block_comments" {
					continue
				}
				if version < commentConfigurationVersion {
					return fmt.Errorf("%s requires configuration version %d", key.Value, commentConfigurationVersion)
				}
				if err := validateStoredCommentList(current.Content[i+1]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateStoredCommentList(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	if node.Kind != yaml.SequenceNode || len(node.Content) > MaxCommentPhrases {
		return fmt.Errorf("comment rules must be a sequence of at most %d phrases", MaxCommentPhrases)
	}
	for _, phrase := range node.Content {
		if phrase.Kind == yaml.AliasNode && phrase.Alias != nil {
			phrase = phrase.Alias
		}
		if phrase.Kind != yaml.ScalarNode || phrase.Tag != "!!str" || !ValidCommentPhrase(phrase.Value) {
			return fmt.Errorf("comment rules contain an invalid phrase")
		}
	}
	return nil
}
