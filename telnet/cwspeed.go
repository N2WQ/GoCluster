// File role: User preference and per-client display for RBN CW speed.
package telnet

import (
	"strings"

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

func formatCWSpotForClient(c *Client, sp *spot.Spot) string {
	if sp == nil {
		return ""
	}
	if !sp.CWDisplayAvailable {
		return sp.FormatDXCluster()
	}
	return sp.FormatDXClusterCW(c.cwSpeedEnabled())
}
