// File role: Native-aware human rendering and reservation transfer. The command
// owns preparation under its configuration lease; the existing writer owns the
// reservation after enqueue, through successful flush, discard or disconnect.
package telnet

import (
	"errors"
	"fmt"
	"time"
)

func humanOwnedReadbackError(dialect DialectName, resource string, err error, duration time.Duration) string {
	if dialect == DialectCC && resource != "SETTINGS" && errors.Is(err, errReadbackTooLarge) {
		err = fmt.Errorf("response exceeds 262,144 bytes")
	}
	return humanReadbackError(err, duration)
}

func (s *Server) renderOwnedHumanReadback(c *Client, resource, category string, duration time.Duration) (string, error) {
	if resource == "SETTINGS" && (c.filterBanks != nil || c.dialect == DialectCC) {
		if category != "" {
			return "", fmt.Errorf("usage: SHOW SETTINGS")
		}
		return s.renderNativeHumanSettings(c, duration)
	}
	if c.dialect == DialectCC {
		return s.nativeCCReadback(c, resource, category, duration)
	}
	return s.renderHumanReadback(c, resource, category, duration)
}

func (s *Server) renderNativeHumanSettings(c *Client, duration time.Duration) (string, error) {
	c.initializeConfiguredSettings()
	status, runtime := s.captureReadbackStatus(c, nil), s.captureHumanRuntime(c)
	status.Configured = c.configuredSettings
	if c.filterBanks != nil {
		cfg, err := c.captureV7Configuration()
		if err != nil {
			return "", err
		}
		status.Preset = c.v7PresetStatuses(cfg)[string(c.dialect)]
	}
	return renderNativeCCBody(maxYAMLBytes, func(h *humanResponse) error {
		if err := writeHumanHeader(h, c, status); err != nil {
			return err
		}
		if err := writeHumanSettings(h, status.Configured, status, runtime, duration); err != nil {
			return err
		}
		return writeHumanFooter(h, duration)
	})
}

// A nil reservation is the unchanged legacy path. Native/both-bank responses
// retain their charge while queued and while the writer holds their batch.
// Queue-full and close failures release before invoking the existing policy.
func (c *Client) enqueueHumanReadback(reservation *configurationOutputReservation, raw []byte, completion readbackCompletion) error {
	if reservation == nil {
		return c.enqueueControl(controlMessage{raw: raw, readback: completion})
	}
	if reservation.preparationOnly {
		reservation.Release()
		return errConfigurationPreparationOnly
	}
	if reservation.client != c {
		reservation.Release()
		return errClientClosed
	}
	if len(raw) > maxV7YAMLBytes {
		reservation.Release()
		return errReadbackTooLarge
	}
	c.bulletinResponseMu.Lock()
	if configurationTransferClosed(c) || reservation.released.Load() {
		c.bulletinResponseMu.Unlock()
		reservation.Release()
		return errClientClosed
	}
	select {
	case c.controlChan <- controlMessage{raw: raw, readback: completion, configurationReservation: reservation}:
		c.bulletinResponseMu.Unlock()
		return nil
	default:
		c.bulletinResponseMu.Unlock()
		reservation.Release()
		return c.controlQueueFull()
	}
}
