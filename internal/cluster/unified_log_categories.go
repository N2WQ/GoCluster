package cluster

import "dxcluster/config"

// Category adapters perform the existing producer formatting and route into
// the common queue. Deduplication is intentionally deferred to each stream's
// reserved processor, after admission, so rejected records cannot suppress
// future evidence.
func newUnifiedDroppedCallLogger(cfg config.DroppedCallLoggingConfig, emitter streamEmitter) *droppedCallLogger {
	if !cfg.Enabled {
		return nil
	}
	l := &droppedCallLogger{}
	if cfg.BadDEDX {
		l.badDEDX = &unifiedLogLineSink{emitter: emitter, stream: streamDroppedBadCall}
	}
	if cfg.NoLicense {
		l.noLicense = &unifiedLogLineSink{emitter: emitter, stream: streamDroppedUnlicensed}
	}
	if cfg.Harmonics {
		l.harmonics = &unifiedLogLineSink{emitter: emitter, stream: streamDroppedHarmonic}
	}
	return l
}

func newUnifiedEventFileLogger(cfg config.LoggingConfig, emitter streamEmitter) *eventFileLogger {
	makeSink := func(enabled bool, stream uint8) *eventLogSink {
		if !enabled {
			return nil
		}
		return &eventLogSink{sink: &unifiedLogLineSink{emitter: emitter, stream: stream}}
	}
	return &eventFileLogger{
		loginAttempts:     makeSink(cfg.LoginAttempts.Enabled, streamLoginAttempts),
		reputationDrops:   makeSink(cfg.ReputationDrops.Enabled, streamReputationDrops),
		telnetConnections: makeSink(cfg.TelnetConnections.Enabled, streamTelnetConnections),
		ingestConnections: makeSink(cfg.IngestConnections.Enabled, streamIngestConnections),
	}
}
