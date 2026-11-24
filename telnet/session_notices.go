// File role: Bounded advisory notices owned by a connection's existing writer.
package telnet

import (
	"fmt"
	"strconv"
	"sync"

	"dxcluster/spot"
)

const presenceNoticeLimit = 16

type noticeType uint8

const (
	presenceNoticeKind noticeType = iota
	profileNoticeKind
	saveNoticeKind
	presetNoticeKind
	goNoticeKind
	noticeTypes
)

var noticeCodes = [noticeTypes]string{
	"presence-changed", "config-changed", "save-status-changed", "presets-changed", "go-selected",
}

type sessionNotice struct {
	version uint64
	text    string
}

// Normal controls keep their queue and close policy. Advisory notices own a
// bounded separate queue for presence events and one latest slot per view.
type sessionNotices struct {
	mu        sync.Mutex
	wake      chan struct{} // Immutable after connection construction.
	ready     bool
	latest    [noticeTypes]sessionNotice
	events    []sessionNotice
	collapsed uint64
}

func (n *sessionNotices) signalLocked() {
	if n.ready {
		select {
		case n.wake <- struct{}{}:
		default:
		}
	}
}

func (c *Client) enableSessionNotices() {
	n := &c.notices
	n.mu.Lock()
	n.ready = true
	n.signalLocked()
	n.mu.Unlock()
}

func (c *Client) offerNotice(kind noticeType, version uint64, text string, event bool) {
	n := &c.notices
	if n.wake == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if configurationTransferClosed(c) {
		return
	}
	notice := sessionNotice{version: version, text: text}
	if event {
		switch {
		case n.collapsed != 0:
			if n.collapsed < ^uint64(0) {
				n.collapsed++
			}
		case len(n.events) == presenceNoticeLimit || len(c.controlChan) == cap(c.controlChan):
			n.collapsed = uint64(len(n.events)) + 1
			n.events = nil
		default:
			if n.events == nil {
				n.events = make([]sessionNotice, 0, presenceNoticeLimit)
			}
			n.events = append(n.events, notice)
			n.signalLocked()
			return
		}
	}
	if version < n.latest[kind].version {
		return
	}
	n.latest[kind] = notice
	n.signalLocked()
}

func (c *Client) nextSessionNotice() controlMessage {
	n := &c.notices
	n.mu.Lock()
	defer n.mu.Unlock()
	if configurationTransferClosed(c) {
		return controlMessage{}
	}
	select {
	case <-c.retired:
		return controlMessage{}
	default:
	}
	if len(n.events) != 0 {
		notice := n.events[0]
		copy(n.events, n.events[1:])
		n.events[len(n.events)-1] = sessionNotice{}
		n.events = n.events[:len(n.events)-1]
		n.signalLocked()
		return notice.control(presenceNoticeKind)
	}
	for kind, notice := range n.latest {
		if notice.text == "" {
			continue
		}
		// Keep the delivered version so an older publisher on another SSID
		// cannot replace the latest preset notification after it was drained.
		n.latest[kind].text = ""
		if noticeType(kind) == presenceNoticeKind && n.collapsed != 0 {
			notice.text = fmt.Sprintf("Web console changed connection %d times; %s", n.collapsed, notice.text)
			n.collapsed = 0
		}
		n.signalLocked()
		return notice.control(noticeType(kind))
	}
	return controlMessage{}
}

func (n sessionNotice) control(kind noticeType) controlMessage {
	return controlMessage{kind: controlAdvisory, line: n.text + "\n",
		noticeCode: noticeCodes[kind], noticeVersion: strconv.FormatUint(n.version, 10)}
}

func (c *Client) clearSessionNotices() {
	c.notices.mu.Lock()
	c.notices.events = nil
	clear(c.notices.latest[:])
	c.notices.collapsed = 0
	c.notices.mu.Unlock()
}

// The SSID lease owns presence versions and state transitions. Text includes
// only the address captured for the current browser and is aimed at Telnet.
func (s *Server) publishPresenceOwned(c *Client, departing bool) {
	s.clientsMutex.RLock()
	other := s.otherTransportSlotLocked(c)
	s.clientsMutex.RUnlock()
	c.presenceVersion++
	version := c.presenceVersion
	if c.web != nil {
		if other == nil {
			return
		}
		if departing {
			// The logger may still have the original GO notice queued. Its
			// instruction described a browser that is no longer connected.
			other.notices.mu.Lock()
			other.notices.latest[goNoticeKind].text = ""
			other.notices.mu.Unlock()
			other.offerNotice(presenceNoticeKind, version, "Web console disconnected.", true)
			return
		}
		other.offerNotice(presenceNoticeKind, version, c.browserPresenceLine(), true)
		c.offerNotice(presenceNoticeKind, version, "Logger connected.", false)
		return
	}
	if other == nil {
		return
	}
	if departing {
		other.offerNotice(presenceNoticeKind, version, "Logger disconnected.", false)
		return
	}
	c.offerNotice(presenceNoticeKind, version, other.browserPresenceLine(), true)
	other.offerNotice(presenceNoticeKind, version, "Logger connected.", false)
}

func (c *Client) browserPresenceLine() string {
	country := c.browserCountry
	if country == "" {
		country = "unknown"
	}
	return fmt.Sprintf("Web console connected from %s (%s).", spotterIP(c.address), country)
}

type profileChangeBaseline struct {
	revision uint64
	unsaved  bool
	goDirty  bool
}

func (c *Client) profileChangeBaseline() profileChangeBaseline {
	return profileChangeBaseline{c.configurationRevision, c.hasUnsavedChanges(), c.goSavePending}
}

// This runs before releasing the SSID lease. No producer calls the writer or
// waits for a wakeup. Only the current slots receive changes to their profile.
func (s *Server) publishProfileChangesOwned(c *Client, before profileChangeBaseline) {
	changed := c.configurationRevision != before.revision
	unsaved := c.hasUnsavedChanges()
	saveChanged := unsaved != before.unsaved || c.goSavePending != before.goDirty
	if !changed && !saveChanged {
		return
	}
	s.clientsMutex.RLock()
	other := s.otherTransportSlotLocked(c)
	s.clientsMutex.RUnlock()
	if saveChanged {
		c.saveStatusVersion++
	}
	if changed && other != nil {
		other.observeHistoryDigest(c.historyFilterDigest())
		if other.getSolarSummaryMinutes() != c.configuredSettings.SolarSummaryMinutes {
			other.setSolarSummaryMinutes(c.configuredSettings.SolarSummaryMinutes, s.now())
		}
	}
	for _, recipient := range [2]*Client{c, other} {
		if recipient == nil {
			continue
		}
		if changed && (recipient != c || c.jsonRequested.Load()) {
			message := "Live profile changed."
			if c.web != nil && recipient != c {
				message = "Live profile changed from the web console."
			}
			recipient.offerNotice(profileNoticeKind, c.configurationRevision, message, false)
		}
		if saveChanged && (recipient != c || c.web != nil || c.jsonRequested.Load()) {
			message := "Live profile is saved."
			if c.goSavePending {
				message = "GO is active but unsaved."
			} else if unsaved {
				message = "Live profile has unsaved changes."
			}
			recipient.offerNotice(saveNoticeKind, c.saveStatusVersion, message, false)
		}
	}
}

func (s *Server) publishGoSelectionOwned(c *Client) {
	s.clientsMutex.RLock()
	logger := s.otherTransportSlotLocked(c)
	s.clientsMutex.RUnlock()
	if logger != nil {
		logger.offerNotice(goNoticeKind, c.configurationRevision, browserRequiresGo, false)
	}
}

func (s *Server) publishPresetChange(c *Client) {
	if s == nil {
		return
	}
	version := s.presetNoticeVersion.Add(1)
	base := spot.NormalizeOwnCallsign(c.callsign)
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	for client := range s.activeConnectionsLocked {
		if (client != c || c.jsonRequested.Load()) && spot.NormalizeOwnCallsign(client.callsign) == base {
			client.offerNotice(presetNoticeKind, version, "Preset library changed.", false)
		}
	}
}
