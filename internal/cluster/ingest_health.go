// File role: Adapts ingest-client health snapshots into transition logs and
// file-only connection events for the live cluster runtime.
// Crawler notes: Start here for source-specific health identities, idle rules,
// and per-human-upstream connection status; construction lives in
// main_runtime.go and dashboard rendering lives in bootstrap.go.
// Related docs: docs/domain-contract.md, rbn/README.md.
// Related tests: internal/cluster/ingest_health_test.go,
// internal/cluster/main_stats_test.go.
package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"dxcluster/dxsummit"
	"dxcluster/pskreporter"
	"dxcluster/rbn"
	"dxcluster/strutil"
)

const (
	ingestHealthInterval  = 30 * time.Second
	ingestIdleThreshold   = 2 * time.Minute
	ingestHealthLogPrefix = "Ingest Health: "
)

type ingestHealthSnapshot struct {
	Connected            bool
	LastMessageAt        time.Time
	LastSpotAt           time.Time
	LastParseErrAt       time.Time
	PayloadQueueLen      int
	PayloadQueueCap      int
	MQTTQueueLen         int
	MQTTQueueCap         int
	SpotQueueLen         int
	SpotQueueCap         int
	PathOnlyQueueLen     int
	PathOnlyQueueCap     int
	PayloadDrops         uint64
	PayloadTooLarge      uint64
	SpotDrops            uint64
	PathOnlyDrops        uint64
	ParseErrors          uint64
	MQTTDropsQoS0        uint64
	MQTTQoS12Timeouts    uint64
	MQTTQoS12Disconnects uint64
}

type ingestHealthSource struct {
	id       string
	name     string
	endpoint string
	snapshot func() ingestHealthSnapshot
}

type ingestHealthState struct {
	connected   bool
	idle        bool
	initialized bool
}

// Purpose: Periodically log ingest health transitions with low noise.
// Key aspects: Reports only on connected/idle state changes and optionally mirrors transitions to file-only event logs.
// Upstream: main startup after ingest clients are created.
// Downstream: log.Printf and optional event reporter.
func startIngestHealthMonitorWithReporter(ctx context.Context, sources []ingestHealthSource, reporter func(source, action, endpoint, reason, state string)) {
	if len(sources) == 0 {
		return
	}
	ticker := time.NewTicker(ingestHealthInterval)
	go func() {
		defer ticker.Stop()
		states := make(map[string]ingestHealthState, len(sources))
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now().UTC()
				for _, source := range sources {
					if source.snapshot == nil {
						continue
					}
					snap := source.snapshot()
					idle := ingestIsIdle(snap, now)
					stateKey := ingestHealthSourceStateKey(source)
					state := states[stateKey]
					if !state.initialized || state.connected != snap.Connected || state.idle != idle {
						log.Printf("%s%s", ingestHealthLogPrefix, formatIngestHealthLine(source.name, snap, idle, now))
						if reporter != nil {
							action := "connected"
							reason := "none"
							if !snap.Connected {
								action = "disconnected"
								reason = "not_connected"
							}
							reporter(source.name, action, source.endpoint, reason, ingestStateLabel(idle))
						}
						states[stateKey] = ingestHealthState{
							connected:   snap.Connected,
							idle:        idle,
							initialized: true,
						}
					}
				}
			}
		}
	}()
}

func ingestHealthSourceStateKey(source ingestHealthSource) string {
	if source.id != "" {
		return source.id
	}
	return source.name
}

func ingestStateLabel(idle bool) string {
	if idle {
		return "idle"
	}
	return "active"
}

func ingestIsIdle(snap ingestHealthSnapshot, now time.Time) bool {
	last := snap.LastSpotAt
	if last.IsZero() {
		last = snap.LastMessageAt
	}
	if last.IsZero() {
		return true
	}
	return now.Sub(last) > ingestIdleThreshold
}

func formatIngestHealthLine(name string, snap ingestHealthSnapshot, idle bool, now time.Time) string {
	status := "connected"
	if !snap.Connected {
		status = "disconnected"
	}
	state := "active"
	if idle {
		state = "idle"
	}
	var b strings.Builder
	b.WriteString(name)
	b.WriteString(" ")
	b.WriteString(status)
	b.WriteString(" ")
	b.WriteString(state)
	if !snap.LastMessageAt.IsZero() {
		b.WriteString(" last_msg=")
		b.WriteString(strutil.FormatAge(now, snap.LastMessageAt))
	}
	if !snap.LastSpotAt.IsZero() {
		b.WriteString(" last_spot=")
		b.WriteString(strutil.FormatAge(now, snap.LastSpotAt))
	}
	if snap.PayloadQueueCap > 0 {
		b.WriteString(" payload_q=")
		fmt.Fprintf(&b, "%d/%d", snap.PayloadQueueLen, snap.PayloadQueueCap)
	}
	if snap.MQTTQueueCap > 0 {
		b.WriteString(" mqtt_q=")
		fmt.Fprintf(&b, "%d/%d", snap.MQTTQueueLen, snap.MQTTQueueCap)
	}
	if snap.SpotQueueCap > 0 {
		b.WriteString(" spot_q=")
		fmt.Fprintf(&b, "%d/%d", snap.SpotQueueLen, snap.SpotQueueCap)
	}
	if snap.PathOnlyQueueCap > 0 {
		b.WriteString(" path_only_q=")
		fmt.Fprintf(&b, "%d/%d", snap.PathOnlyQueueLen, snap.PathOnlyQueueCap)
	}
	var dropParts []string
	if snap.PayloadDrops > 0 {
		dropParts = append(dropParts, fmt.Sprintf("payload=%d", snap.PayloadDrops))
	}
	if snap.PayloadTooLarge > 0 {
		dropParts = append(dropParts, fmt.Sprintf("payload_oversize=%d", snap.PayloadTooLarge))
	}
	if snap.SpotDrops > 0 {
		dropParts = append(dropParts, fmt.Sprintf("spot=%d", snap.SpotDrops))
	}
	if snap.PathOnlyDrops > 0 {
		dropParts = append(dropParts, fmt.Sprintf("path_only=%d", snap.PathOnlyDrops))
	}
	if snap.ParseErrors > 0 {
		dropParts = append(dropParts, fmt.Sprintf("parse=%d", snap.ParseErrors))
	}
	if snap.MQTTDropsQoS0 > 0 {
		dropParts = append(dropParts, fmt.Sprintf("mqtt_qos0=%d", snap.MQTTDropsQoS0))
	}
	if snap.MQTTQoS12Timeouts > 0 {
		dropParts = append(dropParts, fmt.Sprintf("mqtt_qos12_timeout=%d", snap.MQTTQoS12Timeouts))
	}
	if snap.MQTTQoS12Disconnects > 0 {
		dropParts = append(dropParts, fmt.Sprintf("mqtt_qos12_disconnect=%d", snap.MQTTQoS12Disconnects))
	}
	if len(dropParts) > 0 {
		b.WriteString(" drops=")
		b.WriteString(strings.Join(dropParts, ","))
	}
	if !snap.LastParseErrAt.IsZero() {
		b.WriteString(" last_parse_err=")
		b.WriteString(strutil.FormatAge(now, snap.LastParseErrAt))
	}
	return b.String()
}

func rbnHealthSourceWithID(id, name string, client *rbn.Client) ingestHealthSource {
	return ingestHealthSource{
		id:       id,
		name:     name,
		endpoint: client.Endpoint(),
		snapshot: func() ingestHealthSnapshot {
			if client == nil {
				return ingestHealthSnapshot{}
			}
			snap := client.HealthSnapshot()
			return ingestHealthSnapshot{
				Connected:     snap.Connected,
				LastMessageAt: snap.LastLineAt,
				LastSpotAt:    snap.LastSpotAt,
				SpotQueueLen:  snap.SpotQueueLen,
				SpotQueueCap:  snap.SpotQueueCap,
				SpotDrops:     snap.SpotDrops,
			}
		},
	}
}

func pskReporterHealthSource(name string, client *pskreporter.Client) ingestHealthSource {
	return ingestHealthSource{
		id:       "pskreporter",
		name:     name,
		endpoint: client.Endpoint(),
		snapshot: func() ingestHealthSnapshot {
			if client == nil {
				return ingestHealthSnapshot{}
			}
			snap := client.HealthSnapshot()
			return ingestHealthSnapshot{
				Connected:            snap.Connected,
				LastMessageAt:        snap.LastPayloadAt,
				LastSpotAt:           snap.LastSpotAt,
				LastParseErrAt:       snap.LastParseErrAt,
				PayloadQueueLen:      snap.ProcessingQueueLen,
				PayloadQueueCap:      snap.ProcessingQueueCap,
				MQTTQueueLen:         snap.MQTTInboundQueueLen,
				MQTTQueueCap:         snap.MQTTInboundQueueCap,
				SpotQueueLen:         snap.SpotQueueLen,
				SpotQueueCap:         snap.SpotQueueCap,
				PathOnlyQueueLen:     snap.PathOnlyQueueLen,
				PathOnlyQueueCap:     snap.PathOnlyQueueCap,
				PayloadDrops:         snap.PayloadDrops,
				PayloadTooLarge:      snap.PayloadTooLarge,
				SpotDrops:            snap.SpotDrops,
				PathOnlyDrops:        snap.PathOnlyDrops,
				ParseErrors:          snap.ParseErrors,
				MQTTDropsQoS0:        snap.MQTTInboundDropsQoS0,
				MQTTQoS12Timeouts:    snap.MQTTInboundQoS12Timeouts,
				MQTTQoS12Disconnects: snap.MQTTInboundQoS12Disconnects,
			}
		},
	}
}

func dxsummitHealthSource(name string, client *dxsummit.Client) ingestHealthSource {
	return ingestHealthSource{
		id:       "dxsummit",
		name:     name,
		endpoint: client.Endpoint(),
		snapshot: func() ingestHealthSnapshot {
			if client == nil {
				return ingestHealthSnapshot{}
			}
			snap := client.HealthSnapshot()
			return ingestHealthSnapshot{
				Connected:       snap.Connected,
				LastMessageAt:   snap.LastMessageAt,
				LastSpotAt:      snap.LastSpotAt,
				LastParseErrAt:  snap.LastParseErrAt,
				SpotQueueLen:    snap.SpotQueueLen,
				SpotQueueCap:    snap.SpotQueueCap,
				SpotDrops:       snap.SpotDrops,
				ParseErrors:     snap.ParseErrors,
				PayloadTooLarge: snap.ResponseTooLarge,
			}
		},
	}
}

func ingestSourceName(raw string, fallback string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
