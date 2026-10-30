// File role: At-least-once upgrade notices, owned by the existing connection.
// Writer receipts report complete flushes, not operator reads. Waiting owns no
// configuration/preparation/writer lock; acknowledgement changes metadata only.
package telnet

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"dxcluster/filter"
)

type v7ResetNoticeEvents struct {
	profileGeneration int
	libraryGeneration int
}

func (events v7ResetNoticeEvents) empty() bool {
	return events.profileGeneration == 0 && events.libraryGeneration == 0
}

// One receipt belongs to one connection's startup notice. It never borrows a
// PAUSE epoch, and a writer cannot wait for a reader that has already departed.
type v7ResetNoticeReceipt struct {
	events    v7ResetNoticeEvents
	delivered chan struct{}
	signaled  atomic.Bool
}

func newV7ResetNoticeReceipt(events v7ResetNoticeEvents) *v7ResetNoticeReceipt {
	return &v7ResetNoticeReceipt{events: events, delivered: make(chan struct{}, 1)}
}

func (receipt *v7ResetNoticeReceipt) signal() {
	if receipt == nil || !receipt.signaled.CompareAndSwap(false, true) {
		return
	}
	select {
	case receipt.delivered <- struct{}{}:
	default:
	}
}

func pendingV7ResetNotice(c *Client) v7ResetNoticeEvents {
	var events v7ResetNoticeEvents
	if c.v7Notice.ProfileResetGeneration > c.v7Notice.ProfileAcknowledgedGeneration {
		events.profileGeneration = c.v7Notice.ProfileResetGeneration
	}
	if c.v7LibraryResetGeneration > c.v7Notice.LibraryAcknowledgedGeneration {
		events.libraryGeneration = c.v7LibraryResetGeneration
	}
	return events
}

// prepareV7ResetNotice releases its detached full-bank state and reservation
// before the sender can wait. Only the bounded resulting text escapes.
func (s *Server) prepareV7ResetNotice(c *Client) (string, v7ResetNoticeEvents, bool, error) {
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return "", v7ResetNoticeEvents{}, false, err
	}
	defer release()
	events := pendingV7ResetNotice(c)
	if events.empty() {
		return "", events, false, nil
	}
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return "", events, true, err
	}
	defer reservation.Release()
	configuration, err := c.captureV7Configuration()
	if err != nil {
		return "", events, true, err
	}
	message, err := renderV7ResetNotice(configuration, c.dialect, events, c.configurationUnsaved)
	return message, events, true, err
}

func renderV7ResetNotice(configuration filter.V7Configuration, dialect DialectName, events v7ResetNoticeEvents, unsaved bool) (string, error) {
	return renderNativeCCBody(maxYAMLBytes, func(h *humanResponse) error {
		if err := h.line("Upgrade reset notice"); err != nil {
			return err
		}
		if events.profileGeneration != 0 {
			if err := h.prose("", "", "An earlier upgrade reset your GO and CC filter banks and their six delivery switches to the operator's starting defaults."); err != nil {
				return err
			}
		}
		if events.libraryGeneration != 0 {
			if err := h.prose("", "", "The shared library's old named presets were cleared during that upgrade. New presets saved afterward were kept."); err != nil {
				return err
			}
		}
		if err := h.prose("", "", "This notice reports completed work. Your current selections remain active; no reset is repeated."); err != nil {
			return err
		}
		if err := h.row("Active bank:", strings.ToUpper(string(dialect))); err != nil {
			return err
		}
		if dialect == DialectCC {
			if err := h.row("Modes:", compactV7NoticeSelection(configuration.Filters.CC.Modes)); err != nil {
				return err
			}
			if err := h.row("Sources:", compactV7NoticeSelection(configuration.Filters.CC.Sources)); err != nil {
				return err
			}
			if err := h.line("SHOW/FILTER displays all current native CC restrictions."); err != nil {
				return err
			}
		} else {
			if err := h.prose("", "", "Your GO Boolean selection is active. SHOW FILTER displays its exact rules; this notice does not flatten them into mode or source lists."); err != nil {
				return err
			}
		}
		if unsaved {
			if err := h.prose("", "", "Your active selections include unsaved changes. They may be lost when you reconnect."); err != nil {
				return err
			}
		}
		return h.prose("", "", "If acknowledgement cannot be saved, this notice may appear at your next login.")
	})
}

func compactV7NoticeSelection(values []string) string {
	if len(values) == 0 {
		return "(none)"
	}
	if len(values) <= 8 {
		length := 0
		for _, value := range values {
			length += len(value) + 2
		}
		if length <= 54 {
			return strings.Join(values, ", ")
		}
	}
	return fmt.Sprintf("%d selected; use SHOW/FILTER for the full list", len(values))
}

func (c *Client) enqueueV7ResetNotice(message string, receipt *v7ResetNoticeReceipt) error {
	if len(message) > maxYAMLBytes {
		return errReadbackTooLarge
	}
	c.bulletinResponseMu.Lock()
	if configurationTransferClosed(c) {
		c.bulletinResponseMu.Unlock()
		return errClientClosed
	}
	select {
	case c.controlChan <- controlMessage{raw: []byte(message), resetNotice: receipt}:
		c.bulletinResponseMu.Unlock()
		return nil
	default:
		c.bulletinResponseMu.Unlock()
		return c.controlQueueFull()
	}
}

// sendV7ResetNotice runs before the command loop, in that connection's goroutine.
func (s *Server) sendV7ResetNotice(c *Client) bool {
	if s == nil || c == nil {
		return false
	}
	message, events, pending, err := s.prepareV7ResetNotice(c)
	if err != nil {
		if errors.Is(err, errRetiredConfiguration) {
			s.waitForRetiredWriter(c)
			return false
		}
		if errors.Is(err, errClientClosed) {
			return false
		}
		if !pending {
			return false
		}
		return c.enqueueV7ResetNotice("An upgrade notice is still pending. Use SHOW FILTER to inspect your\r\ncurrent selections. The notice will be offered again at a later login.\r\n", nil) == nil
	}
	if !pending {
		return true
	}
	receipt := newV7ResetNoticeReceipt(events)
	if err := c.enqueueV7ResetNotice(message, receipt); err != nil {
		return false
	}
	// Preparation helpers have returned: no large snapshot/lease is held here.
	select {
	case <-receipt.delivered:
		err := s.acknowledgeV7ResetNotice(c, receipt.events)
		if errors.Is(err, errRetiredConfiguration) {
			s.waitForRetiredWriter(c)
			return false
		}
		if errors.Is(err, errClientClosed) {
			return false
		}
		if err != nil {
			log.Printf("Upgrade notice acknowledgement for %s remains pending: %v", c.identity(), err)
			return c.enqueueV7ResetNotice("The upgrade notice acknowledgement could not be saved.\r\nThe notice may appear again at your next login.\r\n", nil) == nil
		}
		return true
	case <-c.retired:
		s.waitForRetiredWriter(c)
		return false
	case <-c.done:
		return false
	case <-s.shutdown:
		return false
	}
}

// acknowledgeV7ResetNotice reloads strict current storage under current-owner
// authority. It never calls a migration/factory or repairs unsaved preferences.
func (s *Server) acknowledgeV7ResetNotice(c *Client, events v7ResetNoticeEvents) error {
	if c == nil || s == nil {
		return errClientClosed
	}
	if events.empty() {
		return nil
	}
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		return err
	}
	defer release()
	if c.recordProtected {
		return errProtectedRecord
	}
	reservation, err := c.reserveConfigurationPreparation()
	if err != nil {
		return err
	}
	defer reservation.Release()
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		return err
	}
	if events.profileGeneration != 0 && record.Notice.ProfileResetGeneration != events.profileGeneration {
		return errors.New("profile reset event changed before acknowledgement")
	}
	if events.libraryGeneration != 0 {
		library, err := filter.LoadV7PresetCollection(c.callsign)
		if err != nil {
			return err
		}
		if library.ResetGeneration != events.libraryGeneration {
			return errors.New("library reset event changed before acknowledgement")
		}
	}
	before := record.Notice
	if events.profileGeneration != 0 {
		record.Notice.ProfileAcknowledgedGeneration = events.profileGeneration
	}
	if events.libraryGeneration != 0 {
		record.Notice.LibraryAcknowledgedGeneration = events.libraryGeneration
	}
	if record.Notice == before {
		c.v7Notice = record.Notice
		return nil
	}
	payload, err := filter.PrepareV7UserRecord(c.callsign, record)
	if err != nil {
		return err
	}
	if err := s.commitConfigurationRecord(payload); err != nil {
		return err
	}
	c.v7Notice = record.Notice
	return nil
}
