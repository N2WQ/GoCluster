// File role: Dispatches framed machine read/write commands and atomic proposals.
// A complete reply is one control message. Upload failures never return payload
// tails to ordinary command dispatch; successful YAML commands do not touch pause.
package telnet

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

// machineDispatch reports whether a terminal reply owns the writer's close.
// The command loop then exits without interrupting that queued final frame.
type machineDispatch struct {
	handled     bool
	terminal    bool
	closeQueued bool
}

func (s *Server) queueMachineReply(c *Client, response string, terminal bool) bool {
	if terminal {
		c.preserveRecordOnExit.Store(true)
	}
	err := c.enqueueControl(controlMessage{raw: []byte(response), closeAfter: terminal})
	return err == nil
}

func (s *Server) machineHeaderFailure(c *Client, verb string, err error) machineDispatch {
	terminal := verb != "GET"
	response := renderYAMLCommandError("", "", "", "invalid_header", err.Error())
	queued := s.queueMachineReply(c, response, terminal)
	return machineDispatch{handled: true, terminal: terminal || !queued, closeQueued: terminal && queued}
}

func (s *Server) handleMachineCommand(c *Client, line string) machineDispatch {
	command, handled, err := parseMachineHeader(line)
	if !handled {
		return machineDispatch{}
	}
	if err != nil {
		return s.machineHeaderFailure(c, command.Verb, err)
	}
	if command.Verb == "GET" {
		return machineDispatch{handled: true, terminal: !s.getMachineConfiguration(c, command)}
	}
	// The absolute upload interval starts when the valid header is accepted;
	// reception checks this deadline itself even if the watchdog runs late.
	body, err := c.receiveYAMLBody(time.Now().Add(yamlUploadTimeout))
	if err != nil {
		return machineDispatch{handled: true, terminal: true}
	}
	request, err := s.prepareMachineRequest(c, body, command)
	if err != nil {
		return machineDispatch{handled: true, terminal: !s.queueMachineReply(c,
			renderYAMLCommandError(command.Resource, request.RequestID, "", "invalid_document", err.Error()), false)}
	}
	response := s.applyMachineRequest(c, command, request)
	return machineDispatch{handled: true, terminal: !s.queueMachineReply(c, response, false)}
}

func (s *Server) acquireYAMLPreparation(c *Client) (func(), error) {
	s.yamlPreparationOnce.Do(func() { s.yamlPreparation = make(chan struct{}, 4) })
	select {
	case s.yamlPreparation <- struct{}{}:
	case <-c.done:
		return nil, errClientClosed
	case <-s.shutdown:
		return nil, errClientClosed
	}
	release := func() { <-s.yamlPreparation }
	select {
	case <-c.done:
		release()
		return nil, errClientClosed
	case <-s.shutdown:
		release()
		return nil, errClientClosed
	default:
	}
	return release, nil
}

func (s *Server) prepareMachineRequest(c *Client, body []byte, command machineCommand) (machineRequest, error) {
	release, err := s.acquireYAMLPreparation(c)
	if err != nil {
		return machineRequest{}, err
	}
	// The decoder returns only detached values and presence masks, never Nodes.
	// Release the permit before any transaction wait or disk I/O.
	request, err := decodeMachineRequest(body, command)
	release()
	return request, err
}

func (s *Server) getMachineConfiguration(c *Client, command machineCommand) bool {
	response := s.prepareMachineReadback(c, command)
	return s.queueMachineReply(c, response, false)
}

// prepareMachineReadback releases transaction ownership before queueing can
// trigger connection cleanup or optional close reporting.
func (s *Server) prepareMachineReadback(c *Client, command machineCommand) string {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return renderYAMLCommandError(command.Resource, command.RequestID, "", "session_unavailable", err.Error())
	}
	defer release()
	revision, err := c.configurationRevisionToken()
	if err != nil {
		return renderYAMLCommandError(command.Resource, command.RequestID, "", "revision_unavailable", err.Error())
	}
	requestID := command.RequestID
	if requestID == "" {
		var identifier [16]byte
		if _, err := rand.Read(identifier[:]); err != nil {
			return renderYAMLCommandError(command.Resource, "", revision, "identifier_unavailable", "Could not assign a request identifier.")
		}
		requestID = hex.EncodeToString(identifier[:])
	}
	response, err := s.renderYAMLReadback(c, command.Resource, requestID, revision)
	if err != nil {
		response = renderYAMLCommandError(command.Resource, requestID, revision, machineErrorCode(err), err.Error())
	}
	return response
}

// prepareMachineCandidate borrows the old configuration only under transaction
// ownership: all writers are fenced by that lease and broadcast readers never
// mutate it. The resulting configuration is preflighted before its bounded clone.
// This permits a full PUT or reducing PATCH to repair an oversized human record.
func (c *Client) prepareMachineCandidate(request machineRequest) (before, next filter.Configuration, err error) {
	c.initializeConfiguredSettings()
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	before = filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
	next, err = request.apply(before)
	if err != nil {
		return before, next, err
	}
	if !next.MinimumSizeFits(maxYAMLBytes) {
		return before, next, errReadbackTooLarge
	}
	next = next.Clone()
	return before, next, nil
}

func (s *Server) applyMachineRequest(c *Client, command machineCommand, request machineRequest) string {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return renderYAMLCommandError(command.Resource, request.RequestID, "", "session_unavailable", err.Error())
	}
	defer release()
	revision, err := c.configurationRevisionToken()
	if err != nil {
		return renderYAMLCommandError(command.Resource, request.RequestID, "", "revision_unavailable", err.Error())
	}
	fail := func(err error) string {
		return renderYAMLCommandError(command.Resource, request.RequestID, revision, machineErrorCode(err), err.Error())
	}
	validateOnly := command.Verb == "VALIDATE"
	if !validateOnly && c.recordProtected {
		return fail(errProtectedRecord)
	}
	if !validateOnly && request.IfRevision != revision {
		return renderYAMLCommandError(command.Resource, request.RequestID, revision, "revision_conflict", "Configuration changed or the session was replaced; GET again before retrying.")
	}
	before, next, err := c.prepareMachineCandidate(request)
	if err != nil {
		return fail(err)
	}
	if err := s.validateMachineConfiguration(next); err != nil {
		return fail(err)
	}
	prepared, _ := s.prepareConfigurationUpdate(c, before, next, time.Now().UTC())
	if next.Filters.NearbyEnabled && (prepared.filter.NearbyUserFine == pathreliability.InvalidCell || prepared.filter.NearbyUserCoarse == pathreliability.InvalidCell) {
		return fail(fmt.Errorf("NEARBY requires a usable GRID and available H3 tables"))
	}
	if err := s.configurationReadbackFits(c, next, prepared); err != nil {
		return fail(err)
	}
	resultRevision := revision
	if !validateOnly && next.Fingerprint() != c.configurationDigest {
		if c.configurationRevision == math.MaxUint64 {
			return fail(fmt.Errorf("configuration revision exhausted; reconnect before retrying"))
		}
		resultRevision = fmt.Sprintf("%s-%d", c.configurationEpoch, c.configurationRevision+1)
	}
	response, err := renderYAMLCommandSuccess(command.Resource, request.RequestID, resultRevision, command.Verb, !validateOnly, !validateOnly)
	if err != nil {
		return fail(err)
	}
	if validateOnly {
		return response
	}
	// Semantic equality ignores callsign order. Order-only updates still publish
	// their supplied lists; identical writes repair disk without rebuilding runtime.
	unchanged := before.Equal(next) &&
		slices.Equal(before.Filters.DXCallsigns, next.Filters.DXCallsigns) &&
		slices.Equal(before.Filters.BlockDXCallsigns, next.Filters.BlockDXCallsigns) &&
		slices.Equal(before.Filters.DECallsigns, next.Filters.DECallsigns) &&
		slices.Equal(before.Filters.BlockDECallsigns, next.Filters.BlockDECallsigns)
	// Even an unchanged PUT must establish durable consistency. No fallible work
	// remains after this commit; a lost acknowledgement is recovered by GET.
	if err := s.persistConfiguration(c, next, c.presetReference); err != nil {
		return renderYAMLCommandError(command.Resource, request.RequestID, revision, "persistence_failed", "Could not save configuration; live and saved configuration are unchanged.")
	}
	if !unchanged {
		s.publishConfiguration(c, next, prepared, c.presetReference, time.Now().UTC())
	}
	return response
}

func machineErrorCode(err error) string {
	switch {
	case errors.Is(err, errReadbackTooLarge):
		return "response_too_large"
	case errors.Is(err, errProtectedRecord):
		return "record_protected"
	case errors.Is(err, errClientClosed), errors.Is(err, errRetiredConfiguration):
		return "session_unavailable"
	default:
		return "invalid_configuration"
	}
}
