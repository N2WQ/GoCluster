// File role: Process-wide, nonblocking reservations for configuration transport.
// Input owners retain their charge through reception and AST decoding until all
// request bytes/nodes are detached. Output owners reserve before rendering, then
// transfer exactly one charge per client to the writer through its final flush.
package telnet

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	configurationInputBudget            = 8 << 20
	configurationOutputBudget           = 8 << 20
	configurationInputReservation       = 256 << 10
	configurationOutputReservationBytes = 1 << 20
)

var (
	configurationInputBytes         atomic.Int64
	configurationOutputBytes        atomic.Int64
	errConfigurationTransferBusy    = errors.New("configuration transfer is busy; try again")
	errConfigurationPreparationOnly = errors.New("configuration preparation owner cannot queue a response")
)

// The counters are process-wide, including when more than one Server exists.
// Stats expose live reservations; callers must not reset another owner's charge.
func configurationTransferBudgetStats() (inputBytes, outputBytes int64) {
	return configurationInputBytes.Load(), configurationOutputBytes.Load()
}

// ConfigurationTransferStats reports live process-wide byte reservations,
// including other Server instances. These charges are not exact heap usage.
func (s *Server) ConfigurationTransferStats() (inputBytes, outputBytes int64) {
	return configurationTransferBudgetStats()
}

func acquireConfigurationInput(c *Client) (func(), error) {
	if configurationTransferClosed(c) {
		return nil, errClientClosed
	}
	for {
		used := configurationInputBytes.Load()
		if used > configurationInputBudget-configurationInputReservation {
			return nil, errConfigurationTransferBusy
		}
		if configurationInputBytes.CompareAndSwap(used, used+configurationInputReservation) {
			break
		}
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { configurationInputBytes.Add(-configurationInputReservation) })
	}
	if configurationTransferClosed(c) {
		release()
		return nil, errClientClosed
	}
	return release, nil
}

// The existing close fence also orders configuration reservation/enqueue against
// writer teardown. A successful enqueue transfers raw and this lease to writer;
// the caller must discard its references and must not release the lease early.
type configurationOutputReservation struct {
	client          *Client
	preparationOnly bool
	released        atomic.Bool
}

func (c *Client) reserveConfigurationOutput() (*configurationOutputReservation, error) {
	return c.reserveConfigurationOwner(false)
}

// reserveConfigurationPreparation is only for nonqueue work already protected
// by the caller's full-callsign configuration lease. Final teardown owns that
// lease after socket/shutdown closure; it must wait for its writer to relinquish
// any old output owner before entering here. This grants no enqueue authority.
func (c *Client) reserveConfigurationPreparation() (*configurationOutputReservation, error) {
	return c.reserveConfigurationOwner(true)
}

func (c *Client) reserveConfigurationOwner(preparationOnly bool) (*configurationOutputReservation, error) {
	if c == nil {
		return nil, errClientClosed
	}
	c.bulletinResponseMu.Lock()
	defer c.bulletinResponseMu.Unlock()
	if !preparationOnly && configurationTransferClosed(c) {
		return nil, errClientClosed
	}
	if c.configurationOutputHeld {
		return nil, errConfigurationTransferBusy
	}
	for {
		used := configurationOutputBytes.Load()
		if used > configurationOutputBudget-configurationOutputReservationBytes {
			return nil, errConfigurationTransferBusy
		}
		if configurationOutputBytes.CompareAndSwap(used, used+configurationOutputReservationBytes) {
			break
		}
	}
	c.configurationOutputHeld = true
	return &configurationOutputReservation{client: c, preparationOnly: preparationOnly}, nil
}

func (r *configurationOutputReservation) Release() {
	if r == nil || !r.released.CompareAndSwap(false, true) {
		return
	}
	r.client.bulletinResponseMu.Lock()
	r.client.configurationOutputHeld = false
	configurationOutputBytes.Add(-configurationOutputReservationBytes)
	r.client.bulletinResponseMu.Unlock()
}

func (c *Client) enqueueConfigurationResponse(reservation *configurationOutputReservation, raw []byte, terminal bool) error {
	if reservation == nil || reservation.client != c {
		if reservation != nil {
			reservation.Release()
		}
		return errClientClosed
	}
	if reservation.preparationOnly {
		reservation.Release()
		return errConfigurationPreparationOnly
	}
	if len(raw) > maxV7YAMLBytes {
		reservation.Release()
		return errReadbackTooLarge
	}
	if terminal {
		c.preserveRecordOnExit.Store(true)
	}
	c.bulletinResponseMu.Lock()
	if configurationTransferClosed(c) || reservation.released.Load() {
		c.bulletinResponseMu.Unlock()
		reservation.Release()
		return errClientClosed
	}
	select {
	case c.controlChan <- controlMessage{raw: raw, closeAfter: terminal, configurationReservation: reservation}:
		c.bulletinResponseMu.Unlock()
		return nil
	default:
		c.bulletinResponseMu.Unlock()
		reservation.Release()
		return c.controlQueueFull()
	}
}

func configurationTransferClosed(c *Client) bool {
	if c == nil {
		return true
	}
	select {
	case <-c.done:
		return true
	default:
	}
	if c.server != nil {
		select {
		case <-c.server.shutdown:
			return true
		default:
		}
	}
	return false
}
