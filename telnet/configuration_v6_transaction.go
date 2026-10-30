// File role: Exact Boolean configuration captures and revision ownership.
// All entry points require the existing full-SSID configuration lease.
package telnet

import (
	"fmt"

	"dxcluster/filter"
)

func (c *Client) captureV6Configuration() filter.V6Configuration {
	c.initializeConfiguredSettings()
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	return filter.V6ConfigurationFromFilter(c.filter, c.configuredSettings)
}

func (c *Client) refreshV6ConfigurationRevision() {
	if c.filterBanks != nil {
		c.refreshV7ConfigurationRevision()
		return
	}
	c.observeConfigurationDigest(c.captureV6Configuration().Fingerprint())
	c.observeHistoryDigest(c.v6HistoryFilterDigest())
}

func (c *Client) v6ConfigurationRevisionToken() (string, error) {
	if err := c.ensureConfigurationEpoch(); err != nil {
		return "", err
	}
	c.refreshV6ConfigurationRevision()
	return fmt.Sprintf("%s-%d", c.configurationEpoch, c.configurationRevision), nil
}

func (c *Client) prepareV6MachineCandidate(request v6MachineRequest) (before, next filter.V6Configuration, runtime filter.Filter, err error) {
	before = c.captureV6Configuration()
	next, err = request.apply(before)
	if err != nil {
		return
	}
	// Shared preparation attests definitions and supplies detached canonical
	// rules and fresh control pointers; no mutable collection escapes live state.
	next, runtime, err = filter.PrepareV6Configuration(next)
	return
}
