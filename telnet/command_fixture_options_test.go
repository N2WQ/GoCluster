package telnet

import (
	"time"

	"dxcluster/config"
	"dxcluster/cty"
)

// commandFixtureOptions consumes the loaded telnet contracts. Deployment feeds,
// reputation, solar fetching and path prediction are absent in checkpoint 00;
// their later checkpoints use separate owners rather than claiming this bridge
// represents the complete production runtime.
func commandFixtureOptions(cfg *config.Config, db *cty.CTYDatabase) ServerOptions {
	c := cfg.Telnet
	return ServerOptions{
		ClusterCall: cfg.Server.NodeID, WelcomeMessage: c.WelcomeMessage,
		LoginGreeting: c.LoginGreeting, LoginPrompt: c.LoginPrompt,
		LoginEmptyMessage: c.LoginEmptyMessage, LoginInvalidMessage: c.LoginInvalidMessage,
		DuplicateLoginMsg: c.DuplicateLoginMsg, InputTooLongMessage: c.InputTooLongMessage,
		InputInvalidCharMessage: c.InputInvalidCharMessage, NearbyLoginWarning: c.NearbyLoginWarning,
		DialectWelcomeMessage: c.DialectWelcomeMessage, DialectSourceDefault: c.DialectSourceDefaultLabel,
		DialectSourcePersisted: c.DialectSourcePersistedLabel, PathStatusMessage: c.PathStatusMessage,
		MaxConnections: c.MaxConnections, MaxPreloginSessions: c.MaxPreloginSessions,
		PreloginTimeout:          time.Duration(c.PreloginTimeoutSeconds) * time.Second,
		PreloginConcurrencyPerIP: c.PreloginConcurrencyPerIP,
		AcceptRatePerIP:          c.AcceptRatePerIP, AcceptBurstPerIP: c.AcceptBurstPerIP,
		AcceptRatePerSubnet: c.AcceptRatePerSubnet, AcceptBurstPerSubnet: c.AcceptBurstPerSubnet,
		AcceptRateGlobal: c.AcceptRateGlobal, AcceptBurstGlobal: c.AcceptBurstGlobal,
		Transport: c.Transport, EchoMode: c.EchoMode, HandshakeMode: string(c.SkipHandshake),
		LoginLineLimit: c.LoginLineLimit, CommandLineLimit: c.CommandLineLimit,
		LoginTimeout:     time.Duration(c.LoginTimeoutSeconds) * time.Second,
		ReadIdleTimeout:  time.Duration(c.ReadIdleTimeoutSeconds) * time.Second,
		KeepaliveSeconds: c.KeepaliveSeconds, BroadcastWorkers: c.BroadcastWorkers,
		BroadcastQueue: c.BroadcastQueue, WorkerQueue: c.WorkerQueue,
		ClientBuffer: c.ClientBuffer, ControlQueue: c.ControlQueueSize,
		AutoReadPauseMinRows:      c.AutoReadPauseMinRows,
		AutoReadPauseDuration:     time.Duration(c.AutoReadPauseSeconds) * time.Second,
		BroadcastBatchIntervalSet: true,
		BroadcastBatchInterval:    time.Duration(c.BroadcastBatchIntervalMS) * time.Millisecond,
		WriterBatchMaxBytes:       c.WriterBatchMaxBytes,
		WriterBatchWait:           time.Duration(c.WriterBatchWaitMS) * time.Millisecond,
		RejectWorkers:             c.RejectWorkers, RejectQueueSize: c.RejectQueueSize,
		RejectWriteDeadline: time.Duration(c.RejectWriteDeadlineMS) * time.Millisecond,
		CTYLookup:           func() *cty.CTYDatabase { return db },
		DefaultDedupePolicy: cfg.Dedup.DefaultPolicy,
		NoiseModel:          cfg.PathReliability.NoiseModel(),
		DedupeFastEnabled:   true, DedupeMedEnabled: true, DedupeSlowEnabled: true,
	}
}
