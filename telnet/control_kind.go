// File role: Identifies queued control messages for the connection writer's
// format and terminal-delivery decisions.
package telnet

// Producers select the meaning of queued bytes. The connection writer selects
// their framing at the message's FIFO position, including across a format switch.
type controlKind uint8

const (
	controlText controlKind = iota
	controlTerminal
	controlSessionReplaced
	controlYAML
	controlYAMLTerminal
	controlKeepalive
	controlBulletin
	controlReset
	controlReadback
	controlRefused
	controlAdvisory
	controlJSONOff
)
