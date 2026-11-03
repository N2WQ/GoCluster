package cluster

import (
	"context"
	"testing"
	"time"
)

func TestStatsDisplayStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		displayStatsWithFCC(ctx, time.Hour,
			nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, "", nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, "", "", dashboardIngestSourceConfig{})
	}()
	select {
	case <-done:
		t.Fatal("stats display stopped before cancellation")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stats display did not stop after cancellation")
	}
}
