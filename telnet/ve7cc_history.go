// File role: Bounded native history preparation and close-fenced publication.
// The producer retains the configuration lease through preparation and enqueue.
// Only an accepted immutable frame transfers its existing output owner to writer.
package telnet

import (
	"bytes"
	"errors"

	"dxcluster/commands"
	"dxcluster/spot"
)

func (c *Client) historyRequestValidLocked(request historyRequest) bool {
	if c.historyClosed || c.historyGeneration != request.generation {
		return false
	}
	return !request.older || (c.historySearch != nil && c.historySearch.token == request.search.token)
}

// No reservation crosses the unlocked archive scan. Unqueued backing and its
// charge are abandoned before returning to the caller that releases the lease.
func (s *Server) publishHistoryOwned(c *Client, request historyRequest, next *historySearch, response string, success bool) (notes ve7ccFormatNotes, refused bool, err error) {
	var frame []byte
	var reservation *configurationOutputReservation
	defer func() {
		frame = nil
		reservation.Release()
	}()
	c.refreshConfigurationRevision()
	c.historyMu.Lock()
	closed, valid := c.historyClosed, c.historyRequestValidLocked(request)
	c.historyMu.Unlock()
	if closed {
		return notes, false, errClientClosed
	}
	if !valid {
		response, success = historyRestart, false
	}
	native := c.ve7ccEnabled
	if native && success {
		reservation, err = c.reserveConfigurationOutput()
		if errors.Is(err, errConfigurationTransferBusy) {
			response, success = ve7ccHistoryBusy, false
		} else if err != nil {
			return notes, false, err
		} else {
			frame, notes, err = s.prepareVE7CCHistory(request.presentation, request.older)
			if err != nil {
				if !errors.Is(err, errVE7CCInvalidSpot) && !errors.Is(err, errVE7CCRecordTooLarge) &&
					!errors.Is(err, commands.ErrHistoryResponseTooLarge) {
					return notes, true, err
				}
				frame = nil
				response, success, refused = ve7ccHistoryFailure, false, true
			}
		}
	} else if success && request.presentation != nil {
		page := request.presentation
		response = commands.RenderHistoryPageWithFormatter(page.page, page.token, request.older, page.unreadable, page.hadMatches, func(sp *spot.Spot) string {
			return sp.FormatDXClusterCW(request.cwSpeed && request.diag == diagModeOff)
		})
	}

	// Close takes this same order. Defers unlock both guards before releasing
	// an untransferred reservation, whose Release acquires bulletinResponseMu.
	c.bulletinResponseMu.Lock()
	defer c.bulletinResponseMu.Unlock()
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	if c.historyClosed || configurationTransferClosed(c) {
		return notes, refused, errClientClosed
	}
	if !c.historyRequestValidLocked(request) {
		frame = nil
		response, success = historyRestart, false
	}
	var message controlMessage
	if native && success {
		// All body lines already have CRLF. Reserved footer headroom was
		// admitted before any pause mutation, so this cannot grow the buffer.
		footer := s.autoReadPauseFooter(c, bytes.Count(frame, []byte{'\n'}))
		frame = appendWriterNormalized(frame, footer)
		message = controlMessage{raw: frame, configurationReservation: reservation}
	} else {
		message.line = s.maybeApplyAutoReadPause(c, response)
	}
	select {
	case c.controlChan <- message:
		if success {
			c.historySearch = next
		}
		if message.configurationReservation != nil {
			reservation = nil
		}
		frame, message.raw, message.configurationReservation = nil, nil, nil
		return notes, refused, nil
	default:
		return notes, refused, errControlQueueFull
	}
}

func (s *Server) prepareVE7CCHistory(page *historyPagePresentation, older bool) ([]byte, ve7ccFormatNotes, error) {
	var notes ve7ccFormatNotes
	if page == nil {
		return nil, notes, errVE7CCInvalidSpot
	}
	budget := maxVE7CCRecordBytes
	if s.autoReadPauseEnabled() {
		budget -= dedupeReadPauseFooterBytes
	}
	context := s.ve7ccContext(true)
	// One fixed backing holds records, normalized statuses and the possible
	// footer. No page-sized string conversion or second producer frame exists.
	frame, err := commands.AppendHistoryPage(make([]byte, 0, maxVE7CCRecordBytes),
		page.page, page.token, older, page.unreadable, page.hadMatches, budget,
		func(dst []byte, sp *spot.Spot, remaining int) ([]byte, error) {
			encoded, recordNotes, recordErr := appendVE7CCRecord(dst, sp, context, remaining)
			notes.sanitized = notes.sanitized || recordNotes.sanitized
			notes.unavailable = notes.unavailable || recordNotes.unavailable
			notes.conflict = notes.conflict || recordNotes.conflict
			return encoded, recordErr
		})
	if err != nil {
		return nil, notes, err
	}
	return frame, notes, nil
}
