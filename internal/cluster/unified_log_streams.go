package cluster

// Stream identities are process-wide and stable across destination recovery.
// Their values index the common logger's fixed loss ledger.
const (
	streamSystemFile uint8 = iota
	streamSystemConsole
	streamPropagation
	streamDroppedBadCall
	streamDroppedUnlicensed
	streamDroppedHarmonic
	streamLoginAttempts
	streamReputationDrops
	streamTelnetConnections
	streamIngestConnections
	streamPeerConnections
	streamPeerOverlong
	streamSurfaceDropped
	streamSurfaceCall
	streamSurfaceUnlicensed
	streamSurfaceHarmonic
	streamSurfaceReputation
	streamSurfaceSystem
	streamSystemEarly
)

func logStreamName(stream uint8) string {
	switch stream {
	case streamSystemFile:
		return "system"
	case streamPropagation:
		return "propagation"
	case streamDroppedBadCall:
		return "dropped bad calls"
	case streamDroppedUnlicensed:
		return "dropped unlicensed calls"
	case streamDroppedHarmonic:
		return "dropped harmonics"
	case streamLoginAttempts:
		return "login attempts"
	case streamReputationDrops:
		return "reputation drops"
	case streamTelnetConnections:
		return "telnet connections"
	case streamIngestConnections:
		return "ingest connections"
	case streamPeerConnections:
		return "peer connections"
	case streamPeerOverlong:
		return "peer overlong"
	default:
		return "unknown"
	}
}
