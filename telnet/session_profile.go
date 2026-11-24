// File role: Shared SSID preferences and transport-scoped registry ownership.
package telnet

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

// sessionProfile is independent of sockets and survives either connection.
// The full SSID lease owns mutations and durable metadata. Fan-out readers use
// pathMu then filterMu, and the dedupe policy has its existing atomic reader.
type sessionProfile struct {
	recentIPs                []string               // Most-recent-first IP history for this callsign
	dialect                  DialectName            // Active command dialect for filter commands
	grid                     string                 // User grid (4+ chars) for path reliability
	gridDerived              bool                   // True when grid was derived from CTY prefix info
	gridCell                 pathreliability.CellID // Cached cell for path reliability
	gridCoarseCell           pathreliability.CellID // Cached coarse (res-1) cell for nearby filtering
	noiseClass               string                 // Noise class token (e.g., QUIET, URBAN)
	pathMinObservationCount  int                    // Optional stricter per-user path sample floor
	dedupePolicy             atomic.Uint32          // Secondary dedupe policy (fast/med/slow)
	filterMu                 sync.RWMutex
	filter                   *filter.Filter // Personal spot filter (band, mode, callsign)
	pathMu                   sync.RWMutex
	configuredSettings       filter.SettingsConfiguration
	configurationInitialized bool
	presetReference          *filter.PresetReference
	filterBanks              *filter.V7FilterBanks // Immutable published definitions; active edits merge by program kind.
	v7PresetReferences       filter.V7PresetReferences
	v7Notice                 filter.V7NoticeState
	v7LibraryResetGeneration int
	configurationUnsaved     bool // A complete profile save clears unsaved preference changes.
	initialRecordMissing     bool // Starting defaults were admitted, but their first write failed.
	shared                   bool // This live profile has served both transport slots.
	recordProtected          bool
	lastLoginUTC             time.Time
	configurationEpoch       string
	configurationRevision    uint64
	configurationDigest      [32]byte
	configurationDigestSet   bool
	goSavePending            bool // The automatic browser GO selection is active but unsaved.
	saveStatusVersion        uint64
	presenceVersion          uint64
}

// Each map entry belongs to a current registered slot. A companion is present
// only while the same SSID has a Telnet slot, so its size is bounded by
// registered physical connections and it is deleted on either departure.
func (s *Server) sameTransportSlotLocked(c *Client) *Client {
	primary := s.clients[c.callsign]
	if primary == nil || (primary.web == nil) == (c.web == nil) {
		return primary
	}
	if c.web != nil {
		return s.browserCompanions[c.callsign]
	}
	return nil
}

func (s *Server) otherTransportSlotLocked(c *Client) *Client {
	primary := s.clients[c.callsign]
	if primary == nil || (primary.web == nil) != (c.web == nil) {
		return primary
	}
	if c.web == nil {
		return s.browserCompanions[c.callsign]
	}
	return nil
}

func (s *Server) installConnectionLocked(c *Client) {
	primary := s.clients[c.callsign]
	if primary != nil && (primary.web == nil) != (c.web == nil) {
		if s.browserCompanions == nil {
			s.browserCompanions = make(map[string]*Client)
		}
		if c.web != nil {
			s.browserCompanions[c.callsign] = c
			return
		}
		s.browserCompanions[c.callsign] = primary
	}
	s.clients[c.callsign] = c
}

func (s *Server) removeConnectionLocked(c *Client) {
	if s.clients[c.callsign] == c {
		if browser := s.browserCompanions[c.callsign]; browser != nil {
			s.clients[c.callsign] = browser
		} else {
			delete(s.clients, c.callsign)
		}
	}
	delete(s.browserCompanions, c.callsign)
}

func (s *Server) activeConnectionCountLocked() int {
	return len(s.clients) + len(s.browserCompanions)
}

// Callers hold clientsMutex while iterating. Peer and operator views instead
// walk the primary map, so a pair is exactly one SSID in those views.
func (s *Server) activeConnectionsLocked(yield func(*Client) bool) {
	for _, c := range s.clients {
		if !yield(c) {
			return
		}
	}
	for _, c := range s.browserCompanions {
		if !yield(c) {
			return
		}
	}
}

func (c *Client) awaitConfigurationWriter() {
	if configurationTransferClosed(c) && c.writerStopped != nil {
		select {
		case <-c.writerStopped:
		case <-c.retired:
		}
	}
}

func (c *Client) hasUnsavedChanges() bool {
	return c.configurationUnsaved || c.goSavePending || c.initialRecordMissing
}

func (c *Client) markProfileSaved() {
	c.configurationUnsaved = false
	c.initialRecordMissing = false
	c.goSavePending = false
}

const browserRequiresGo = "GO is active while a web console is connected. Use DIALECT CC after it disconnects."

var errBrowserRequiresGo = errors.New("GO is active while a web console is connected; use DIALECT CC after it disconnects")

func (s *Server) browserRequiresGo(c *Client) bool {
	s.clientsMutex.RLock()
	browser := s.clients[c.callsign]
	required := browser != nil && (browser.web != nil || s.browserCompanions[c.callsign] != nil)
	s.clientsMutex.RUnlock()
	return required
}

type browserGoSelection struct {
	prepared *Client
	owner    *configurationOutputReservation
}

// Validate the live GO view before the slot acquires authority. A rejected
// login cannot change the current dialect, even if validation ran beforehand.
func (s *Server) prepareBrowserGoOwned(c *Client, now time.Time) (selection browserGoSelection, err error) {
	if c.web == nil || c.filterBanks == nil || c.dialect == DialectGo {
		return browserGoSelection{}, nil
	}
	reservation, err := c.reserveConfigurationPreparation()
	if err != nil {
		return browserGoSelection{}, err
	}
	defer func() {
		if err != nil {
			reservation.Release()
		}
	}()
	before, err := c.captureV7Configuration()
	if err != nil {
		return browserGoSelection{}, err
	}
	next := before
	next.Settings.Dialect = string(DialectGo)
	prepared, err := s.prepareV7ConfigurationUpdate(c, before, next, now)
	if err != nil {
		return browserGoSelection{}, err
	}
	if err := s.v7ConfigurationReadbackFits(c, next, prepared, c.v7PresetReferences); err != nil {
		return browserGoSelection{}, err
	}
	return browserGoSelection{prepared, reservation}, c.admitTemporaryV7Configuration(next)
}

// The slot is registered before browser GO selection can change shared state.
// Separate GO persistence from login metadata and from other unsaved edits.
func (s *Server) selectBrowserGoOwned(c *Client, selection browserGoSelection, now time.Time) {
	prepared := selection.prepared
	if prepared != nil {
		s.publishConfigurationState(c, prepared.configuredSettings, prepared, nil, now, false)
		c.refreshV7ConfigurationRevision()
		selection.owner.Release()
	}
	if prepared == nil && !c.goSavePending && !c.initialRecordMissing {
		return
	}
	c.goSavePending = s.saveBrowserGoOwned(c) != nil
	if prepared != nil {
		s.publishGoSelectionOwned(c)
	}
}

func (s *Server) saveBrowserGoOwned(c *Client) error {
	if c.recordProtected {
		return errProtectedRecord
	}
	reservation, err := c.reserveConfigurationPreparation()
	if err != nil {
		return err
	}
	defer reservation.Release()
	record, err := filter.LoadV7UserRecord(c.callsign)
	if c.initialRecordMissing && errors.Is(err, os.ErrNotExist) {
		// Recreate only starting defaults. Other active, unsaved edits remain live.
		record, err = s.newV7LoginRecord(DialectGo)
		if err == nil {
			record.LastLoginUTC = c.lastLoginUTC
			record.RecentIPs = boundedSessionIPs(c.recentIPs)
		}
	}
	if err != nil {
		return err
	}
	record.Configuration.Settings.Dialect = string(DialectGo)
	payload, err := filter.PrepareV7UserRecord(c.callsign, record)
	if err != nil {
		return err
	}
	if err := s.commitConfigurationRecord(payload); err != nil {
		return err
	}
	c.initialRecordMissing = false
	return nil
}

func (s *Server) connectionSnapshot() []*Client {
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	clients := make([]*Client, 0, s.activeConnectionCountLocked())
	for client := range s.activeConnectionsLocked {
		clients = append(clients, client)
	}
	return clients
}
