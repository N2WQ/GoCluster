// File role: Bounded bulletin history rendering and composed control-queue
// reservation cleanup shared with native configuration output.
package telnet

import (
	"log"

	"dxcluster/bulletinhistory"
)

const (
	bulletinResponseBudget = 4 << 20
	// Covers render growth, optional footer, queued text and the normalized
	// writer-batch copy for the longest supported bulletin history.
	bulletinResponseReservation = 256 << 10
	bulletinResponseMaxBytes    = 60 << 10
)

func (s *Server) reserveBulletinResponse() bool {
	for {
		used := s.bulletinResponseBytes.Load()
		if used > bulletinResponseBudget-bulletinResponseReservation {
			return false
		}
		if s.bulletinResponseBytes.CompareAndSwap(used, used+bulletinResponseReservation) {
			return true
		}
	}
}

// BulletinHistoryStats reports bounded history occupancy and storage health.
func (s *Server) BulletinHistoryStats() bulletinhistory.Stats {
	if s == nil || s.bulletinHistory == nil {
		return bulletinhistory.Stats{}
	}
	return s.bulletinHistory.Stats()
}

func (s *Server) sendBulletinHistoryResponse(client *Client, kind string, full bool, errText string) bool {
	if errText != "" {
		return s.sendCommandResponse(client, errText, "bulletin usage")
	}
	if s.bulletinHistory == nil {
		return s.sendCommandResponse(client, "Bulletin history unavailable.\n", "bulletin history")
	}
	if !s.reserveBulletinResponse() {
		return s.sendCommandResponse(client, "Bulletin history is busy; try again.\n", "bulletin history busy")
	}
	var message string
	if full {
		message = s.bulletinHistory.Render(kind, true, s.now())
	} else {
		message = s.bulletinHistory.RenderRecent(kind, s.now())
	}
	message = s.maybeApplyAutoReadPause(client, message)
	if len(message) > bulletinResponseMaxBytes {
		s.bulletinResponseBytes.Add(-bulletinResponseReservation)
		log.Printf("Bulletin history response exceeded bound: %d bytes", len(message))
		return s.sendClientMessage(client, "Bulletin history is unavailable: response too large.\n", "bulletin history")
	}
	// The close lock ensures the writer's final queue drain cannot race an
	// enqueue that has already reserved process-wide response memory.
	client.bulletinResponseMu.Lock()
	select {
	case <-client.done:
		client.bulletinResponseMu.Unlock()
		s.bulletinResponseBytes.Add(-bulletinResponseReservation)
		return false
	default:
	}
	select {
	case client.controlChan <- controlMessage{line: message, bulletinReservation: bulletinResponseReservation}:
		client.bulletinResponseMu.Unlock()
		return true
	default:
		client.bulletinResponseMu.Unlock()
		s.bulletinResponseBytes.Add(-bulletinResponseReservation)
		if err := client.controlQueueFull(); !isExpectedClientSendErr(err) {
			log.Printf("Failed to enqueue bulletin history: %v", err)
		}
		return false
	}
}

func (c *Client) releaseBulletinReservations(amount int64) {
	if amount > 0 && c.server != nil {
		c.server.bulletinResponseBytes.Add(-amount)
	}
}

// drainBulletinReservations is the single final control-queue drain. It releases
// both bulletin and configuration owners after the writer stops, so neither
// consumer can discard the other owner's reservation.
func (c *Client) drainBulletinReservations() {
	for {
		select {
		case msg := <-c.controlChan:
			msg.raw, msg.line = nil, ""
			c.releaseBulletinReservations(msg.bulletinReservation)
			msg.configurationReservation.Release()
		default:
			return
		}
	}
}
