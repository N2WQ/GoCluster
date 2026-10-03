package cluster

import (
	"strings"

	"dxcluster/reputation"
	"dxcluster/telnet"
)

func (r *clusterRuntime) logLoginAttemptEvent(ev telnet.LoginAttemptEvent) {
	if r == nil || r.eventFileLogger == nil {
		return
	}
	detail := strings.TrimSpace(ev.Detail)
	if detail == "" {
		detail = "none"
	}
	r.eventFileLogger.LogLoginAttempt(
		eventLogField{key: "event", value: "login_attempt"},
		eventLogField{key: "action", value: ev.Action},
		eventLogField{key: "reason", value: ev.Reason},
		eventLogField{key: "call", value: eventCall(ev.Call)},
		eventLogField{key: "ip", value: eventLogIP(ev.Address)},
		eventLogField{key: "detail", value: detail},
	)
}

func (r *clusterRuntime) logTelnetConnectionEvent(ev telnet.ConnectionEvent) {
	if r == nil || r.eventFileLogger == nil {
		return
	}
	r.eventFileLogger.LogTelnetConnection(
		eventLogField{key: "event", value: "telnet_connection"},
		eventLogField{key: "action", value: ev.Action},
		eventLogField{key: "reason", value: ev.Reason},
		eventLogField{key: "call", value: eventCall(ev.Call)},
		eventLogField{key: "ip", value: eventLogIP(ev.Address)},
	)
}

func (r *clusterRuntime) logReputationDropEvent(ev reputation.DropEvent) {
	if r == nil || r.eventFileLogger == nil {
		return
	}
	country := ev.CountryCode
	if strings.TrimSpace(country) == "" {
		country = ev.CountryName
	}
	r.eventFileLogger.LogReputationDrop(
		eventLogField{key: "event", value: "reputation_drop"},
		eventLogField{key: "call", value: eventCall(ev.Call)},
		eventLogField{key: "band", value: ev.Band},
		eventLogField{key: "reason", value: string(ev.Reason)},
		eventLogField{key: "ip", value: ev.Prefix},
		eventLogField{key: "asn", value: ev.ASN},
		eventLogField{key: "country", value: country},
		eventLogField{key: "source", value: ev.Source},
		eventLogField{key: "flags", value: formatPenaltyFlags(ev.Flags)},
	)
}

func (r *clusterRuntime) logIngestConnectionEvent(source, action, endpoint, reason, state string) {
	if r == nil || r.eventFileLogger == nil {
		return
	}
	r.eventFileLogger.LogIngestConnection(
		eventLogField{key: "event", value: "ingest_connection"},
		eventLogField{key: "source", value: source},
		eventLogField{key: "action", value: action},
		eventLogField{key: "endpoint", value: endpoint},
		eventLogField{key: "state", value: state},
		eventLogField{key: "reason", value: reason},
	)
}

func eventCall(call string) string {
	call = strings.TrimSpace(call)
	if call == "" {
		return "unknown"
	}
	return call
}
