package telnet

import (
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/pathreliability"
)

// commandFixtureOptions consumes the loaded telnet contracts. Deployment feeds,
// reputation and solar fetching are absent in checkpoint 00; later checkpoints
// add their own controls. The loaded, empty predictor supports PATHSAMPLES from
// checkpoint 12 without ingestion/display or a production model-quality claim.
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
		PathPredictor:       pathreliability.NewPredictor(cfg.PathReliability, cfg.PathReliability.AllowedBands),
		DedupeFastEnabled:   cfg.Dedup.SecondaryFastWindowSeconds > 0,
		DedupeMedEnabled:    cfg.Dedup.SecondaryMedWindowSeconds > 0,
		DedupeSlowEnabled:   cfg.Dedup.SecondarySlowWindowSeconds > 0,
	}
}
