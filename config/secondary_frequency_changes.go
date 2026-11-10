package config

import "fmt"

// validateRawSecondaryFrequencyChanges prevents yaml.v3's float-to-integer
// truncation from silently changing whole-Hz tolerances or the change limit.
// Required presence/null diagnostics remain with the shared key walker.
func validateRawSecondaryFrequencyChanges(raw map[string]any) []error {
	var errs []error
	for _, key := range []string{
		"cw_rtty_tolerance_hz", "ft_tolerance_hz", "other_skimmer_tolerance_hz",
		"human_tolerance_hz", "max_changes",
	} {
		value, present := yamlValueAt(raw, "dedup", "secondary_frequency_changes", key)
		if !present || value == nil {
			continue
		}
		var positive bool
		switch n := value.(type) {
		case int:
			positive = n > 0
		case int64:
			positive = n > 0
		}
		if !positive {
			errs = append(errs, fmt.Errorf("invalid YAML setting %q: requires a positive integer", "dedup.secondary_frequency_changes."+key))
		}
	}
	return errs
}
