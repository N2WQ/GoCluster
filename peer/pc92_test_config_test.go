package peer

import "dxcluster/config"

// completeProtocolTestConfig supplies explicit wire-contract fixture values.
// Tests of invalid construction must call NewManager directly, without this
// helper. It deliberately leaves timers, queues, persistence and ACLs unchanged.
func completeProtocolTestConfig(cfg config.PeeringConfig, localCall string) config.PeeringConfig {
	if cfg.LocalCallsign == "" {
		cfg.LocalCallsign = localCall
	}
	if cfg.NodeVersion == "" {
		cfg.NodeVersion = "5457"
	}
	if cfg.LegacyVersion == "" {
		cfg.LegacyVersion = "5457"
	}
	if cfg.PC92Bitmap == 0 {
		cfg.PC92Bitmap = 5
	}
	if cfg.HopCount == 0 {
		cfg.HopCount = 99
	}
	if cfg.MaxLineLength == 0 {
		cfg.MaxLineLength = 65536
	}
	if cfg.PC92MaxBytes == 0 {
		cfg.PC92MaxBytes = 65536
	}
	return cfg
}
