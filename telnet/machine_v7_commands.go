// File role: Schema-seven transport and atomic full-profile transactions.
// Input budget survives body/AST ownership. Complete readback/admission reserves
// output before capture; publication follows the existing durable commit seam.
package telnet

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"dxcluster/filter"
)

func v7MachineActive() bool { return filter.CurrentConfigurationVersion >= 7 }

func (s *Server) handleV7MachineCommand(c *Client, line string) machineDispatch {
	cmd, handled, err := parseV7MachineHeader(line)
	if !handled {
		return machineDispatch{}
	}
	if cmd.SchemaVersion != 7 {
		return s.handleV6MachineCommand(c, line)
	}
	if err != nil {
		terminal := cmd.Verb != "GET"
		queued := s.queueMachineReply(c, renderV7YAMLCommandError("", "", "", "invalid_header", err.Error()), terminal)
		return machineDispatch{handled: true, terminal: terminal || !queued, closeQueued: terminal && queued}
	}
	if cmd.Verb == "GET" {
		return s.handleV7MachineReadback(c, cmd)
	}
	request, terminal, err := s.receiveV7MachineRequest(c, cmd)
	if err != nil {
		if errors.Is(err, errYAMLUploadDeadline) || errors.Is(err, errYAMLUploadFraming) || errors.Is(err, errYAMLUploadTooLarge) {
			return machineDispatch{handled: true, terminal: true}
		}
		queued := s.queueMachineReply(c, renderV7YAMLCommandError(cmd.Resource, request.RequestID, "", v7MachineErrorCode(err), err.Error()), terminal)
		return machineDispatch{handled: true, terminal: terminal || !queued, closeQueued: terminal && queued}
	}
	response := s.applyV7MachineRequest(c, cmd, request)
	return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, response, false)}
}

func (s *Server) receiveV7MachineRequest(c *Client, cmd machineCommand) (v7MachineRequest, bool, error) {
	releaseInput, err := acquireConfigurationInput(c)
	if err != nil {
		return v7MachineRequest{}, true, err // Unread body cannot become commands.
	}
	defer releaseInput()
	body, err := c.receiveYAMLBodyLimit(time.Now().Add(yamlUploadTimeout), maxV7YAMLBytes)
	if err != nil {
		return v7MachineRequest{}, true, err
	}
	releaseDecode, err := s.acquireYAMLPreparation(c)
	if err != nil {
		return v7MachineRequest{}, false, err
	}
	request, err := decodeV7MachineRequest(body, cmd, newRuleDXCCResolver(s.ctyLookup))
	releaseDecode()
	return request, false, err
}

func (s *Server) handleV7MachineReadback(c *Client, cmd machineCommand) machineDispatch {
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		response := renderV7YAMLCommandError(cmd.Resource, cmd.RequestID, "", v7MachineErrorCode(err), err.Error())
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, response, false)}
	}
	response, err := s.prepareV7MachineReadback(c, cmd)
	if err != nil {
		reservation.Release()
		response = renderV7YAMLCommandError(cmd.Resource, cmd.RequestID, "", v7MachineErrorCode(err), err.Error())
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, response, false)}
	}
	// Enqueue consumes the lease on every outcome. On success only writer may
	// release it after dropping the frame/batch following flush or final discard.
	raw := []byte(response)
	err = c.enqueueConfigurationResponse(reservation, raw, false)
	return machineDispatch{handled: true, terminal: err != nil}
}

func (s *Server) prepareV7MachineReadback(c *Client, cmd machineCommand) (string, error) {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return "", err
	}
	defer release()
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		return "", err
	}
	id := cmd.RequestID
	if id == "" {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return "", fmt.Errorf("could not assign request identifier")
		}
		id = hex.EncodeToString(nonce[:])
	}
	status := s.captureReadbackStatus(c, nil)
	cfg, err := c.captureV7Configuration()
	if err != nil {
		return "", err
	}
	canonical, goRuntime, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		return "", err
	}
	return renderV7YAMLReadback(canonical, goRuntime.PreparedRules(), status, c.v7PresetStatuses(canonical), cmd.Resource, id, revision)
}

func v7MachineErrorCode(err error) string {
	if errors.Is(err, errConfigurationTransferBusy) {
		return "configuration_busy"
	}
	var document *machineSchemaError
	if errors.As(err, &document) {
		return "invalid_document"
	}
	return machineErrorCode(err)
}

func (s *Server) applyV7MachineRequest(c *Client, cmd machineCommand, request v7MachineRequest) string {
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return renderV7YAMLCommandError(cmd.Resource, request.RequestID, "", v7MachineErrorCode(err), err.Error())
	}
	defer reservation.Release()
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return renderV7YAMLCommandError(cmd.Resource, request.RequestID, "", v7MachineErrorCode(err), err.Error())
	}
	defer release()
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		return renderV7YAMLCommandError(cmd.Resource, request.RequestID, "", "revision_unavailable", err.Error())
	}
	fail := func(err error) string {
		return renderV7YAMLCommandError(cmd.Resource, request.RequestID, revision, v7MachineErrorCode(err), err.Error())
	}
	validate := cmd.Verb == "VALIDATE"
	if !validate && c.recordProtected {
		return fail(errProtectedRecord)
	}
	if !validate && request.IfRevision != revision {
		return renderV7YAMLCommandError(cmd.Resource, request.RequestID, revision, "revision_conflict", "Configuration changed or the session was replaced; GET again before retrying.")
	}
	before, err := c.captureV7Configuration()
	if err != nil {
		return fail(err)
	}
	next, err := request.apply(before, string(c.dialect))
	if err != nil {
		return fail(err)
	}
	next, goRuntime, _, err := filter.PrepareV7Configuration(next)
	if err != nil {
		return fail(err)
	}
	ruleEdit := before
	ruleEdit.Filters.GO = ruleEdit.Filters.GO.WithRuleSet(goRuntime.PreparedRules())
	ruleOnlyRecovery := request.goPatch.filterFields&1 != 0 && ruleEdit.Equal(next)
	if err := s.validateMachineSettingsWithGridRecovery(next.Settings, ruleOnlyRecovery); err != nil {
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
	if err := c.admitTemporaryV7Configuration(next); err != nil {
		return fail(err)
	}
	// VALIDATE admits the prospective physical record too, while never calling
	// the commit seam or publishing any state, revision, reference or clock.
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
	response, err := renderV7YAMLCommandSuccess(cmd.Resource, request.RequestID, resultRevision, cmd.Verb, !validate, !validate)
	if err != nil {
		return fail(err)
	}
	if validate {
		return response
	}
	if err := s.commitConfigurationRecord(payload); err != nil {
		return renderV7YAMLCommandError(cmd.Resource, request.RequestID, revision, "persistence_failed", "Could not save configuration; live and saved configuration are unchanged.")
	}
	if changed {
		s.publishV7Configuration(c, next, prepared, refs, time.Now().UTC())
	}
	c.configurationUnsaved = false
	return response
}
