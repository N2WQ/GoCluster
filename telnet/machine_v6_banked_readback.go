// File role: Honest legacy views over a both-bank profile. CC settings and
// capabilities contain no fabricated Boolean rules, inactive GO state, or v6
// conversion of native preset baselines. Every view identifies its backup limit.
package telnet

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"dxcluster/filter"
)

const legacyV6BankWarning = "Schema 6 is not a complete backup. Native CC filters and both-bank backup/restore require GET/PUT YAML CONFIG SCHEMA 7."

func (s *Server) handleBankedV6Readback(c *Client, cmd machineCommand) machineDispatch {
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, renderV6YAMLCommandError(cmd.Resource, cmd.RequestID, "", v7MachineErrorCode(err), err.Error()), false)}
	}
	response, err := s.prepareBankedV6Readback(c, cmd)
	if err != nil {
		reservation.Release()
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, renderV6YAMLCommandError(cmd.Resource, cmd.RequestID, "", machineErrorCode(err), err.Error()), false)}
	}
	if response.refused {
		reservation.Release()
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, response.frame, false)}
	}
	raw := []byte(response.frame)
	response.frame = ""
	err = c.enqueueConfigurationResponse(reservation, raw, false)
	return machineDispatch{handled: true, terminal: err != nil}
}

type bankedV6Readback struct {
	frame   string
	refused bool
}

func (s *Server) prepareBankedV6Readback(c *Client, cmd machineCommand) (bankedV6Readback, error) {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return bankedV6Readback{}, err
	}
	defer release()
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		return bankedV6Readback{}, err
	}
	if c.dialect == DialectCC && (cmd.Resource == "FILTER" || cmd.Resource == "CONFIG") {
		return bankedV6Readback{renderV6YAMLCommandError(cmd.Resource, cmd.RequestID, revision, "schema7_required", legacyV6BankWarning), true}, nil
	}
	id := cmd.RequestID
	if id == "" {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return bankedV6Readback{}, fmt.Errorf("could not assign request identifier")
		}
		id = hex.EncodeToString(nonce[:])
	}
	status := s.captureReadbackStatus(c, nil)
	cfg, err := c.captureV7Configuration()
	if err != nil {
		return bankedV6Readback{}, err
	}
	status.Configured = cfg.Settings
	status.Preset = c.v7PresetStatuses(cfg)[string(c.dialect)]
	if c.dialect == DialectCC {
		frame, err := renderCCBankedV6Readback(cfg.Settings, status, cmd.Resource, id, revision)
		return bankedV6Readback{frame: frame}, err
	}
	legacy := filter.V6Configuration{Filters: cfg.Filters.GO, Settings: cfg.Settings}
	canonical, runtime, err := filter.PrepareV6Configuration(legacy)
	if err != nil {
		return bankedV6Readback{}, err
	}
	frame, err := renderV6YAMLReadback(canonical, runtime.PreparedRules(), status, cmd.Resource, id, revision, legacyV6BankWarning)
	return bankedV6Readback{frame: frame}, err
}

func renderCCBankedV6Readback(settings filter.SettingsConfiguration, status configurationReadbackStatus, resource, id, revision string) (string, error) {
	var configuration any
	switch resource {
	case "SETTINGS":
		configuration = settings
	case "CAPABILITIES":
		configuration = struct {
			SupportedSchemaVersions []int    `yaml:"supported_schema_versions"`
			Resources               []string `yaml:"resources"`
			SettingsFields          []string `yaml:"settings_fields"`
			ActiveDialect           string   `yaml:"active_dialect"`
			NativeSchemaVersion     int      `yaml:"native_schema_version"`
			ResponseBytes           int      `yaml:"response_bytes"`
			UploadBytes             int      `yaml:"upload_bytes"`
		}{[]int{6, 7}, []string{"SETTINGS", "CAPABILITIES"}, machineSettingFields[:], "cc", 7, maxYAMLBytes, maxYAMLBytes}
	default:
		return "", fmt.Errorf("native CC filter inspection requires schema 7")
	}
	if !readbackMetadataFits(status, id, revision) {
		return "", errReadbackTooLarge
	}
	return encodeBoundedYAML(struct {
		SchemaVersion        int                         `yaml:"schema_version"`
		RequestID            string                      `yaml:"request_id"`
		Resource             string                      `yaml:"resource"`
		Revision             string                      `yaml:"revision"`
		Configuration        any                         `yaml:"configuration"`
		Status               configurationReadbackStatus `yaml:"status"`
		CompatibilityWarning string                      `yaml:"compatibility_warning"`
	}{6, id, resource, revision, configuration, status, legacyV6BankWarning})
}
