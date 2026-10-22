// File role: Exact Boolean configuration captures and revision ownership.
// All entry points require the existing full-SSID configuration lease.
package telnet

import (
	"fmt"

	"dxcluster/filter"
)

func (c *Client) captureV5Configuration() filter.V5Configuration {
	c.initializeConfiguredSettings()
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	return filter.V5ConfigurationFromFilter(c.filter, c.configuredSettings)
}

func (c *Client) refreshV5ConfigurationRevision() {
	c.observeConfigurationDigest(c.captureV5Configuration().Fingerprint())
	c.observeHistoryDigest(c.v5HistoryFilterDigest())
}

func (c *Client) v5ConfigurationRevisionToken() (string, error) {
	if err := c.ensureConfigurationEpoch(); err != nil {
		return "", err
	}
	c.refreshV5ConfigurationRevision()
	return fmt.Sprintf("%s-%d", c.configurationEpoch, c.configurationRevision), nil
}

func (c *Client) prepareV5MachineCandidate(request v5MachineRequest) (before, next filter.V5Configuration, runtime filter.Filter, err error) {
	before = c.captureV5Configuration()
	next, err = request.apply(before)
	if err != nil {
		return
	}
	// Shared preparation attests definitions and supplies detached canonical
	// rules and fresh control pointers; no mutable collection escapes live state.
	next, runtime, err = filter.PrepareV5Configuration(next)
	return
}
