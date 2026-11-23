package telnet

// Producers select the meaning of queued bytes. The connection writer selects
// their framing at the message's FIFO position, including across a format switch.
type controlKind uint8

const (
	controlText controlKind = iota
	controlTerminal
	controlYAML
	controlYAMLTerminal
	controlKeepalive
	controlBulletin
	controlReset
	controlReadback
	controlRefused
	controlJSONOff
)
