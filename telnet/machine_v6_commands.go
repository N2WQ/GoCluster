// File role: Active v6 machine transactions.
// Existing transport, full-SSID lease, commit seam and pure state transfer own
// their original boundaries.
package telnet

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"dxcluster/filter"
)

func (s *Server) handleV6MachineCommand(c *Client, line string) machineDispatch {
	banked := false
	if c != nil {
		c.filterMu.RLock()
		banked = c.filterBanks != nil
		c.filterMu.RUnlock()
	}
	if banked {
		return s.handleBankedV6MachineCommand(c, line)
	}
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
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, s.prepareV6MachineReadback(c, cmd), false)}
	}
	body, err := c.receiveYAMLBody(time.Now().Add(yamlUploadTimeout))
	if err != nil {
		return machineDispatch{handled: true, terminal: true}
	}
	request, err := s.prepareV6MachineRequest(c, body, cmd)
	if err != nil {
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c,
			renderV6YAMLCommandError(cmd.Resource, request.RequestID, "", "invalid_document", err.Error()), false)}
	}
	// apply releases the lease before queueing can invoke connection cleanup.
	response := s.applyV6MachineRequest(c, cmd, request)
	return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, response, false)}
}

func (s *Server) prepareV6MachineRequest(c *Client, body []byte, cmd machineCommand) (v6MachineRequest, error) {
	release, err := s.acquireYAMLPreparation(c)
	if err != nil {
		return v6MachineRequest{}, err
	}
	request, err := decodeV6MachineRequest(body, cmd, newRuleDXCCResolver(s.ctyLookup))
	release()
	return request, err
}

func (s *Server) prepareV6MachineReadback(c *Client, cmd machineCommand) string {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, cmd.RequestID, "", "session_unavailable", err.Error())
	}
	defer release()
	revision, err := c.v6ConfigurationRevisionToken()
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, cmd.RequestID, "", "revision_unavailable", err.Error())
	}
	id := cmd.RequestID
	if id == "" {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return renderV6YAMLCommandError(cmd.Resource, "", revision, "identifier_unavailable", "Could not assign a request identifier.")
		}
		id = hex.EncodeToString(nonce[:])
	}
	// Capture status before borrowing path/filter state. The lease orders all
	// preference mutations, while ordinary runtime observations remain current.
	status := s.captureReadbackStatus(c, nil)
	cfg := c.captureV6Configuration()
	ref, err := filter.V6PresetReferenceFromRuntime(c.presetReference)
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, id, revision, machineErrorCode(err), err.Error())
	}
	status = attachV6ReadbackConfiguration(status, cfg, ref)
	canonical, runtime, err := filter.PrepareV6Configuration(cfg)
	if err == nil {
		var response string
		response, err = renderV6YAMLReadback(canonical, runtime.PreparedRules(), status, cmd.Resource, id, revision)
		if err == nil {
			return response
		}
	}
	return renderV6YAMLCommandError(cmd.Resource, id, revision, machineErrorCode(err), err.Error())
}

func (s *Server) applyV6MachineRequest(c *Client, cmd machineCommand, request v6MachineRequest) string {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, "", "session_unavailable", err.Error())
	}
	defer release()
	revision, err := c.v6ConfigurationRevisionToken()
	if err != nil {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, "", "revision_unavailable", err.Error())
	}
	fail := func(err error) string {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, machineErrorCode(err), err.Error())
	}
	validate := cmd.Verb == "VALIDATE"
	if !validate && c.recordProtected {
		return fail(errProtectedRecord)
	}
	if !validate && request.IfRevision != revision {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, "revision_conflict", "Configuration changed or the session was replaced; GET again before retrying.")
	}
	before, next, runtime, err := c.prepareV6MachineCandidate(request)
	if err != nil {
		return fail(err)
	}
	// Effective settings use the same compact runtime captures as
	// human publication; immutable rule programs are shared across preparation.
	_, prior, err := filter.PrepareV6Configuration(before)
	if err != nil {
		return fail(err)
	}
	now := time.Now().UTC()
	prepared, _ := s.prepareConfigurationUpdate(c,
		filter.ConfigurationFromFilter(&prior, before.Settings),
		filter.ConfigurationFromFilter(&runtime, next.Settings), now)
	// Only a supplied rules list may differ. This narrow recovery waives the
	// unchanged GRID's mapping availability, never its syntax or other settings.
	ruleEdit := before
	ruleEdit.Filters = ruleEdit.Filters.WithRuleSet(runtime.PreparedRules())
	ruleOnlyRecovery := request.filterFields&1 != 0 && ruleEdit.Equal(next)
	if err := s.validateMachineSettingsWithGridRecovery(next.Settings, ruleOnlyRecovery); err != nil {
		return fail(err)
	}
	ref, err := filter.V6PresetReferenceFromRuntime(c.presetReference)
	if err != nil {
		return fail(err)
	}
	if err := s.v6ConfigurationReadbackFits(c, next, prepared, ref); err != nil {
		return fail(err)
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
	payload, err := s.prepareV6ConfigurationRecord(c, next, ref)
	if err != nil {
		return fail(err)
	}
	if err := s.commitConfigurationRecord(payload); err != nil {
		return renderV6YAMLCommandError(cmd.Resource, request.RequestID, revision, "persistence_failed", "Could not save configuration; live and saved configuration are unchanged.")
	}
	// Even identical requests repair disk. Publication has no fallible work,
	// retains the stable Filter pointer and preserves an unchanged solar clock.
	if changed {
		s.publishConfigurationState(c, next.Settings, prepared, c.presetReference, time.Now().UTC(), false)
		c.refreshV6ConfigurationRevision()
	}
	return response
}
