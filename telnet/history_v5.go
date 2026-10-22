// File role: Matching-only history invalidation for Boolean configurations.
package telnet

func (c *Client) v5HistoryFilterDigest() [32]byte {
	c.pathMu.RLock()
	c.filterMu.RLock()
	defer c.filterMu.RUnlock()
	defer c.pathMu.RUnlock()
	return c.historyFilterDigestLocked()
}
