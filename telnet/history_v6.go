// File role: Matching-only history invalidation for Boolean configurations.
package telnet

func (c *Client) v6HistoryFilterDigest() [32]byte {
	return c.historyFilterDigest()
}
