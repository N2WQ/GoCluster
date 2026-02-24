package main

import "time"

const (
	shadowResolverQueueSize           = 8192
	shadowResolverMaxActiveKeys       = 6000
	shadowResolverMaxCandidatesPerKey = 16
	shadowResolverMaxReportersPerCand = 64
	shadowResolverInactiveTTL         = 10 * time.Minute
	shadowResolverEvalMinInterval     = 500 * time.Millisecond
	shadowResolverSweepInterval       = 1 * time.Second
	shadowResolverHysteresisWindows   = 2

	sampleInterval = 60 * time.Second
)
