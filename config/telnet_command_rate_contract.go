package config

import "fmt"

// maxTelnetCommandRateValue bounds both integer settings so the session's
// nanocommand credit fits safely in int64.
const maxTelnetCommandRateValue = 1_000_000_000

// validateRawTelnetCommandRate rejects float scalars before typed YAML decoding,
// which would otherwise truncate them. The presence walker reports absent or
// null values separately.
func validateRawTelnetCommandRate(raw map[string]any) []error {
	var errs []error
	for _, setting := range []struct {
		key string
		min int
	}{
		{key: "command_rate_per_second", min: 0},
		{key: "command_burst", min: 1},
	} {
		value, present := yamlValueAt(raw, "telnet", setting.key)
		if !present || value == nil {
			continue
		}
		n, ok := value.(int)
		if !ok || n < setting.min || n > maxTelnetCommandRateValue {
			errs = append(errs, fmt.Errorf("invalid YAML setting %q: requires an integer between %d and %d", "telnet."+setting.key, setting.min, maxTelnetCommandRateValue))
		}
	}
	return errs
}
