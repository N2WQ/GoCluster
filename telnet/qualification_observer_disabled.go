//go:build !qualification

package telnet

// The ordinary build carries no observer state, timestamp, or callback work.
// This empty method is inlined away at the successful enqueue boundary.
func (*Client) observeQualificationEnqueue(*spotEnvelope) {}
