// File role: One small DX-history cursor per connection. Configuration stripes serialize
// coherent capture and publication with settings transactions; historyMu orders
// publication against close and competing search generations. Neither is held
// while Pebble scans. Only accepted control messages advance continuation.
package telnet

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
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
	search       historySearch
	filter       historyFilterSnapshot
	generation   uint64
	older        bool
	cwSpeed      bool
	diag         diagMode
	presentation *historyPagePresentation // Request-local only; never retained by cursor or queue.
	jsonPage     *jsonHistoryDelivery
}

type historyPagePresentation struct {
	page       archive.HistoryPage
	token      string
	unreadable bool
	hadMatches bool
}

// observeHistoryDigest runs inside the configuration transaction. A change
// followed by restoration still invalidates the cursor because each publication
// advances this generation. Propagation observations never enter the digest.
func (c *Client) observeHistoryDigest(digest [32]byte) {
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
	cwSpeed := c.cwSpeedEnabled()
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	if c.historyClosed {
		return historyRequest{}, "", errClientClosed
	}
	request := historyRequest{filter: snapshot, older: command.Next, cwSpeed: cwSpeed, diag: c.getDiagMode()}
	if request.older {
		if command.Token == "" && c.historySearch == nil {
			return historyRequest{}, "No archive search to continue. Start a fresh SHOW DX search.\n", nil
		}
		if c.historySearch == nil || (command.Token != "" && c.historySearch.token != command.Token) {
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
	json := c.jsonRequested.Load()
	if json {
		// The reader waits for he before accepting NEXT. This lock is private
		// to competing history requests; neither writer nor configuration
		// invalidation needs it.
		c.jsonHistoryMu.Lock()
		defer c.jsonHistoryMu.Unlock()
	}
	request, errText, err := s.beginHistory(c, line)
	if err != nil {
		if json {
			s.commandSessionClosed(c)
		}
		return true, false
	}
	if errText != "" {
		return true, s.sendCommandResponse(c, errText, "history validation")
	}
	var collect func(*spot.Spot) error
	if json {
		request.jsonPage = &jsonHistoryDelivery{
			generation: request.generation, oldToken: request.search.token,
			older: request.older, unreadable: request.search.unreadable,
			hadMatches: request.search.hadMatches, done: make(chan struct{}),
		}
		collect = func(sp *spot.Spot) error { return request.jsonPage.rows.collect(s, sp, request.cwSpeed) }
	}
	page, readErr := s.processor.CollectHistoryPage(request.search.query, request.search.before,
		func(sp *spot.Spot) bool { return request.filter.matches(s, sp) }, s.now(), c.done, collect)
	request.filter = historyFilterSnapshot{}
	var next *historySearch
	response := historyFailure
	if readErr == nil {
		state := request.search
		state.unreadable = state.unreadable || page.Unreadable > 0
		state.hadMatches = state.hadMatches || page.Selected > 0 || len(page.Spots) > 0
		state.before = append([]byte(nil), page.Before...)
		state.token = ""
		if page.End != archive.HistoryExhausted {
			state.token, readErr = newHistoryToken()
			if readErr == nil {
				next = &state
			}
		}
		if readErr == nil {
			if json {
				p := request.jsonPage
				p.next, p.unreadable, p.hadMatches = next, state.unreadable, state.hadMatches
				switch page.End {
				case archive.HistoryCountReached:
					p.end = "count"
				case archive.HistoryBudgetReached:
					p.end = "budget"
				default:
					p.end = "exhausted"
				}
			} else {
				request.presentation = &historyPagePresentation{
					page: page, token: state.token, unreadable: state.unreadable,
					hadMatches: request.search.hadMatches,
				}
			}
		}
	}
	if readErr != nil {
		busy := errors.Is(readErr, archive.ErrHistoryBusy)
		if json {
			request.jsonPage.empty("failed")
			if busy {
				request.jsonPage.end = "busy"
			}
		}
		if !busy {
			log.Printf("History search for %s failed: %v", c.identity(), readErr)
		}
	}
	return true, s.publishHistory(c, request, next, response, readErr == nil)
}

// publishHistory retains the configuration lease through format preparation and
// atomic queue/cursor publication. Cleanup precedes lease release and reporting;
// no network write or archive work runs under the publication guards.
func (s *Server) publishHistory(c *Client, request historyRequest, next *historySearch, response string, success bool) bool {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		if request.jsonPage != nil {
			request.jsonPage.discard()
			s.commandSessionClosed(c)
		}
		return false
	}
	notes, refused, err := s.publishHistoryOwned(c, request, next, response, success)
	release()
	c.reportVE7CC(notes, refused)
	if p := request.jsonPage; p != nil {
		if err != nil {
			p.discard()
			if s.commandSessionClosed(c) {
				return false
			}
		} else {
			// The writer publishes the cursor before signaling the reader.
			// A retired/canceled reader leaves the owned page to writer cleanup.
			select {
			case <-p.done:
				return !s.commandSessionClosed(c) && p.flushed
			case <-c.done:
				return false
			case <-c.retired:
				s.waitForRetiredWriter(c)
				return false
			case <-s.shutdown:
				return false
			}
		}
	}
	return s.finishVE7CCSend(c, err, "history publication")
}
