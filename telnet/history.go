// History owns one small cursor per connection. Configuration stripes serialize
// coherent capture and publication with settings transactions; historyMu orders
// publication against close and competing search generations. Neither is held
// while Pebble scans. Only accepted control messages advance continuation.
package telnet

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"strings"
	"time"

	"dxcluster/archive"
	"dxcluster/commands"
	"dxcluster/spot"
)

const historyRestart = "History settings or search changed. Start a fresh SHOW DX search.\n"
const historyFailure = "History search failed or was canceled. Retry the search or continuation.\n"

type historySearch struct {
	query      commands.HistoryQuery
	before     []byte
	token      string
	unreadable bool
	hadMatches bool
}

type historyRequest struct {
	search     historySearch
	filter     historyFilterSnapshot
	generation uint64
	older      bool
}

// refreshHistoryRevision runs inside the configuration transaction. A change
// followed by restoration still invalidates the cursor because each publication
// advances this generation. Propagation observations never enter the digest.
func (c *Client) refreshHistoryRevision() {
	digest := c.historyFilterDigest()
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	if c.historyDigestSet && digest != c.historyDigest {
		c.historyGeneration++
		c.historySearch = nil
	}
	c.historyDigest, c.historyDigestSet = digest, true
}

func (c *Client) invalidateHistory() {
	c.historyMu.Lock()
	c.historyClosed = true
	c.historyGeneration++
	c.historySearch = nil
	c.historyMu.Unlock()
}

func historyCommandCandidate(line string) bool {
	parts := strings.Fields(strings.ToUpper(line))
	if len(parts) == 0 {
		return false
	}
	if parts[0] == "SHOW/DX" || parts[0] == "SH/DX" {
		return true
	}
	return len(parts) >= 2 && (parts[0] == "SHOW" || parts[0] == "SH") && (parts[1] == "DX" || parts[1] == "MYDX")
}

// beginHistory validates before replacing state. A valid fresh query replaces
// the old search immediately, even if scanning subsequently fails. The detached
// filter belongs only to this request, while the saved position remains bounded.
func (s *Server) beginHistory(c *Client, line string) (historyRequest, string, error) {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return historyRequest{}, "", err
	}
	defer release()
	command, _, errText := s.processor.ParseHistoryCommand(line, string(c.dialect))
	if errText != "" {
		return historyRequest{}, errText, nil
	}
	c.refreshConfigurationRevision()
	snapshot := c.captureHistoryFilter()
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	if c.historyClosed {
		return historyRequest{}, "", errClientClosed
	}
	request := historyRequest{filter: snapshot, older: command.Token != ""}
	if request.older {
		if c.historySearch == nil || c.historySearch.token != command.Token {
			return historyRequest{}, "Invalid history continuation. Start a fresh SHOW DX search.\n", nil
		}
		request.search = *c.historySearch
		request.search.before = append([]byte(nil), c.historySearch.before...)
	} else {
		c.historyGeneration++
		c.historySearch = nil
		request.search.query = command.Query
	}
	request.generation = c.historyGeneration
	return request, "", nil
}

func newHistoryToken() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "H1" + strings.ToUpper(hex.EncodeToString(random[:])), nil
}

// handleHistoryCommand returns handled and whether the command loop may continue.
// It intercepts history before the generic processor's live-filter predicate.
func (s *Server) handleHistoryCommand(c *Client, line string) (bool, bool) {
	if !historyCommandCandidate(line) {
		return false, true
	}
	request, errText, err := s.beginHistory(c, line)
	if err != nil {
		return true, false
	}
	if errText != "" {
		return true, s.sendCommandResponse(c, errText, "history validation")
	}
	page, readErr := s.processor.ReadHistoryPage(request.search.query, request.search.before,
		func(sp *spot.Spot) bool { return request.filter.matches(s, sp) }, s.now(), c.done)
	var next *historySearch
	response := historyFailure
	if readErr == nil {
		state := request.search
		state.unreadable = state.unreadable || page.Unreadable > 0
		state.hadMatches = state.hadMatches || len(page.Spots) > 0
		state.before = append([]byte(nil), page.Before...)
		state.token = ""
		if page.End != archive.HistoryExhausted {
			state.token, readErr = newHistoryToken()
			if readErr == nil {
				next = &state
			}
		}
		if readErr == nil {
			response = commands.RenderHistoryPage(page, state.token, request.older, state.unreadable, request.search.hadMatches)
		}
	}
	if readErr != nil {
		log.Printf("History search for %s failed: %v", c.identity(), readErr)
	}
	return true, s.publishHistory(c, request, next, response, readErr == nil)
}

// publishHistory linearizes settings validation, queue acceptance and advancement.
// A full queue retains the existing disconnect policy outside all guards, since
// close takes historyMu. No network write or archive work runs in this section.
func (s *Server) publishHistory(c *Client, request historyRequest, next *historySearch, response string, success bool) bool {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return false
	}
	c.refreshConfigurationRevision()
	c.historyMu.Lock()
	if c.historyClosed {
		c.historyMu.Unlock()
		release()
		return false
	}
	valid := c.historyGeneration == request.generation
	if request.older {
		valid = valid && c.historySearch != nil && c.historySearch.token == request.search.token
	}
	if !valid {
		response, success = historyRestart, false
	}
	response = s.maybeApplyAutoReadPause(c, response)
	select {
	case c.controlChan <- controlMessage{line: response}:
		if success {
			c.historySearch = next
		}
		c.historyMu.Unlock()
		release()
		return true
	default:
		c.historyMu.Unlock()
		release()
		return c.controlQueueFull() == nil
	}
}
