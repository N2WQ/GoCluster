// File role: Schema-six compatibility over authoritative version-seven banks.
// Legacy vocabulary never gains inactive-bank authority. The complete v7 record
// is admitted and committed before selected live state is published.
package telnet

import (
	"errors"
	"fmt"
	"math"
	"time"

	"dxcluster/filter"
)

func (s *Server) handleBankedV6MachineCommand(c *Client, line string) machineDispatch {
	cmd, handled, err := parseV6MachineHeader(line)
	if !handled {
		return machineDispatch{}
	}
	if err != nil {
		terminal := cmd.Verb != "GET"
		queued := s.queueMachineReply(c, renderV6YAMLCommandError("", "", "", "invalid_header", err.Error()), terminal)
		return machineDispatch{handled: true, terminal: terminal || !queued, closeQueued: terminal && queued}
	}
	if cmd.Verb == "GET" {
		return s.handleBankedV6Readback(c, cmd)
	}
	request, terminal, err := s.receiveBankedV6MachineRequest(c, cmd)
	if err != nil {
		if errors.Is(err, errYAMLUploadDeadline) || errors.Is(err, errYAMLUploadFraming) || errors.Is(err, errYAMLUploadTooLarge) {
			return machineDispatch{handled: true, terminal: true}
		}
		queued := s.queueMachineReply(c, renderV6YAMLCommandError(cmd.Resource, request.RequestID, "", v7MachineErrorCode(err), err.Error()), terminal)
		return machineDispatch{handled: true, terminal: terminal || !queued, closeQueued: terminal && queued}
	}
	response := s.applyBankedV6MachineRequest(c, cmd, request)
	return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, response, false)}
}

func (s *Server) receiveBankedV6MachineRequest(c *Client, cmd machineCommand) (v6MachineRequest, bool, error) {
	releaseInput, err := acquireConfigurationInput(c)
	if err != nil {
		return v6MachineRequest{}, true, err
	}
	defer releaseInput()
	body, err := c.receiveYAMLBody(time.Now().Add(yamlUploadTimeout))
	if err != nil {
		return v6MachineRequest{}, true, err
	}
	releaseDecode, err := s.acquireYAMLPreparation(c)
	if err != nil {
		return v6MachineRequest{}, false, err
	}
	request, err := decodeV6MachineRequest(body, cmd, newRuleDXCCResolver(s.ctyLookup))
	releaseDecode()
	return request, false, err
}

func (s *Server) applyBankedV6MachineRequest(c *Client, cmd machineCommand, request v6MachineRequest) string {
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, "", v7MachineErrorCode(err), err.Error())
	}
	defer reservation.Release()
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, "", machineErrorCode(err), err.Error())
	}
	defer release()
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, "", "revision_unavailable", err.Error())
	}
	fail := func(err error) string {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, machineErrorCode(err), err.Error())
	}
	if c.dialect == DialectCC && cmd.Resource != "SETTINGS" {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, "schema7_required", legacyV6BankWarning)
	}
	validate := cmd.Verb == "VALIDATE"
	if !validate && c.recordProtected {
		return fail(errProtectedRecord)
	}
	if !validate && request.IfRevision != revision {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, "revision_conflict", "Configuration changed or the session was replaced; GET again before retrying.")
	}
	before, err := c.captureV7Configuration()
	if err != nil {
		return fail(err)
	}
	legacy, err := request.apply(filter.V6Configuration{Filters: before.Filters.GO, Settings: before.Settings})
	if err != nil {
		return fail(err)
	}
	if cmd.Resource == "CONFIG" && s.effectiveV7Dialect(legacy.Settings) != DialectGo {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, "schema7_required", "Schema 6 CONFIG requires the resulting GO dialect; use schema 7 for a complete both-bank restore.")
	}
	next := before
	next.Filters.GO, next.Settings = legacy.Filters, legacy.Settings
	next, goRuntime, _, err := filter.PrepareV7Configuration(next)
	if err != nil {
		return fail(err)
	}
	ruleEdit := before
	ruleEdit.Filters.GO = ruleEdit.Filters.GO.WithRuleSet(goRuntime.PreparedRules())
	if err := s.validateMachineSettingsWithGridRecovery(next.Settings, request.filterFields&1 != 0 && ruleEdit.Equal(next)); err != nil {
		return fail(err)
	}
	prepared, err := s.prepareV7ConfigurationUpdate(c, before, next, time.Now().UTC())
	if err != nil {
		return fail(err)
	}
	refs, err := c.v7PresetReferences.Clone()
	if err != nil {
		return fail(err)
	}
	if err := s.v7ConfigurationReadbackFits(c, next, prepared, refs); err != nil {
		return fail(err)
	}
	if prepared.dialect == DialectGo {
		if err := s.v6ConfigurationReadbackFits(c, filter.V6Configuration{Filters: next.Filters.GO, Settings: next.Settings}, prepared, nil); err != nil {
			return fail(err)
		}
	}
	if err := c.admitTemporaryV7Configuration(next); err != nil {
		return fail(err)
	}
	var payload *filter.PreparedUserRecord
	if !c.recordProtected {
		payload, err = s.prepareV7ConfigurationRecord(c, next, refs)
		if err != nil {
			return fail(err)
		}
	}
	changed := !before.Equal(next)
	resultRevision := revision
	if !validate && changed {
		if c.configurationRevision == math.MaxUint64 {
			return fail(fmt.Errorf("configuration revision exhausted; reconnect before retrying"))
		}
		resultRevision = fmt.Sprintf("%s-%d", c.configurationEpoch, c.configurationRevision+1)
	}
	response, err := renderV6YAMLCommandSuccess(cmd.Resource, request.RequestID, resultRevision, cmd.Verb, !validate, !validate)
	if err != nil {
		return fail(err)
	}
	if validate {
		return response
	}
	if err := s.commitConfigurationRecord(payload); err != nil {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, "persistence_failed", "Could not save configuration; live and saved configuration are unchanged.")
	}
	if changed {
		s.publishV7Configuration(c, next, prepared, refs, time.Now().UTC())
	}
	c.configurationUnsaved = false
	return response
}
