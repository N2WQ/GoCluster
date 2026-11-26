// File role: User preference and per-client display for RBN CW speed.
package telnet

import (
	"strings"
	"sync"

	"dxcluster/spot"
)

func (c *Client) cwSpeedEnabled() bool {
	if c == nil {
		return false
	}
	c.filterMu.RLock()
	enabled := c.configuredSettings.CWSpeed
	c.filterMu.RUnlock()
	return enabled
}

func (s *Server) handleCWSpeedCommand(c *Client, line string) (string, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SET") || !strings.EqualFold(fields[1], "CWSPEED") {
		return "", false
	}
	return s.runPreferenceCommand(c, line, func(client *Client, input string) (string, bool) {
		return s.runHumanPreferenceOwned(client, input, s.stageCWSpeedCommand)
	})
}

func (s *Server) stageCWSpeedCommand(c *Client, line string) humanCommandOutcome {
	fields := strings.Fields(line)
	if len(fields) != 3 || (!strings.EqualFold(fields[2], "ON") && !strings.EqualFold(fields[2], "OFF")) {
		return humanHandled("Usage: SET CWSPEED <ON|OFF>\n")
	}
	c.configuredSettings.CWSpeed = strings.EqualFold(fields[2], "ON")
	state := strings.ToUpper(fields[2])
	return humanMutation("CW speed: "+state+"\n", "CW speed: "+state+" (warning: failed to persist: %v)\n")
}

// Only envelopes from the same immutable live publication share these two
// undecorated lines. Writers select their current preference after diagnostics
// and wire-mode gates. The ring, history and session never own this cache.
type cwBroadcastLines struct {
	off, on cwBroadcastLine
}

type cwBroadcastLine struct {
	once sync.Once
	line string
}

// Includes the writer's LF. Larger configured layouts use the same formatter
// without retaining their output in recipient queues.
const maxCWBroadcastLineBytes = 256

func injectCWLineGlyph(line, glyph string) string {
	// Only body columns count toward the layout. A canonical line can have
	// been primed at a previous width before the current glyph lookup.
	return injectGlyphsInBody(line, glyph, len(line)-1)
}

func canShareCWLines(sp *spot.Spot) bool {
	return sp.CWDisplayAvailable && spot.CurrentDXClusterLayout().LineLength < maxCWBroadcastLineBytes
}

func formatCWSpotForClient(c *Client, sp *spot.Spot) string {
	if sp == nil {
		return ""
	}
	if !sp.CWDisplayAvailable {
		return sp.FormatDXCluster()
	}
	return sp.FormatDXClusterCW(c.cwSpeedEnabled())
}

func (env *spotEnvelope) cwLineForClient(c *Client) string {
	if env.publication == nil || env.publication.cw == nil {
		return formatCWSpotForClient(c, env.spot) + "\n"
	}
	sp := env.spot
	enabled := c.cwSpeedEnabled()
	lines := env.publication.cw
	line := &lines.off
	if enabled {
		line = &lines.on
	}
	width := spot.CurrentDXClusterLayout().LineLength
	if width < maxCWBroadcastLineBytes {
		var built string
		line.once.Do(func() {
			built = sp.FormatDXClusterCW(enabled) + "\n"
			if len(built) <= maxCWBroadcastLineBytes {
				line.line = built
			}
		})
		if len(line.line) == width+1 {
			return line.line
		}
		if built != "" {
			return built
		}
	}
	return sp.FormatDXClusterCW(enabled) + "\n"
}
