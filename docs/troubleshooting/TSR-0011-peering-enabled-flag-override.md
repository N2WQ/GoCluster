# TSR-0011 - Peering Enabled Flag Overridden by Host/Port Auto-Enable

Status: Resolved
Date Opened: 2026-03-03
Date Resolved: 2026-03-03
Owner: Codex
Technical Area: config, peer
Trigger Source: Chat request
Led To ADR(s): ADR-0047
Tags: peering, config, outbound sessions

## RCA Summary

- What happened: Peers configured with `enabled: false` were still dialed and
  produced repeated DNS/reconnect noise.
- Why: Config normalization treated host+port as an implicit enable signal and
  overwrote the disabled state before the peer manager checked it.
- What fixed it: ADR-0047 made outbound peer activation explicit; host and port
  no longer imply `enabled: true`.
- How we know: Source inspection showed the peer manager respected `Enabled`
  and config normalization flipped it; targeted config tests and full checks
  validated the new contract.
- Operator/support answer: For unwanted outbound peer dialing, check the
  effective `peering.peers[].enabled` value first; host/port alone should not
  start dialing.

## Triggering Request
- Request date: 2026-03-03
- Request summary: Disabled placeholder peers were still being dialed repeatedly.
- Request reference (chat/issue/link): Chat request in this implementation thread.

## Symptoms and Impact
- What failed or looked wrong? Logs showed repeated `Peering: dialing ...` and DNS failures for peers configured with `enabled: false`.
- User/operator impact: Placeholder peers produced avoidable reconnect noise and DNS lookup churn.
- Scope and affected components: `config.Load` normalization for `peering.peers[]`.

## Timeline
1. 2026-03-03 15:xx - Operator reported disabled peers were dialed.
2. 2026-03-03 15:xx - Root cause identified in config normalization loop that force-enabled peers with host+port.
3. 2026-03-03 16:xx - Fix implemented and validated with targeted and full repo checks.

## Hypotheses and Tests
1. Hypothesis A - Outbound manager ignores `enabled`.
   - Evidence/commands: inspected `peer/manager.go` startup loop; it filters on `if !peerCfg.Enabled { continue }`.
   - Outcome: Rejected
2. Hypothesis B - Config normalization flips disabled peers to enabled.
   - Evidence/commands: inspected `config/config.go` loop at peering normalization; found `host+port` caused `Enabled=true`.
   - Outcome: Supported

## Findings
- Root cause (or best current explanation): `config.Load` overrode explicit/implicit disabled state for peers when host and port were present.
- Contributing factors: convenience auto-enable logic lacked a way to preserve explicit disable semantics.
- Why this did or did not require a durable decision: This changes operator-facing config contract for peering activation and impacts runtime dialing behavior.

## Decision Linkage
- ADR created/updated: ADR-0047
- Decision delta summary: Outbound peer activation is explicit opt-in; omitted `enabled` defaults to disabled.
- Contract/behavior changes (or `No contract changes`): `peering.peers[].enabled` is now authoritative; host/port no longer imply enablement.

## Verification and Monitoring
- Validation steps run:
  - `go test ./config`
  - `go test ./config -run TestPeeringPeerEnabled -v`
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
- Signals to monitor (metrics/logs): Absence of `Peering: dialing` for peers with `enabled: false` or omitted `enabled`; stable reconnect logs only for explicitly enabled peers.
- Rollback triggers: Operators require legacy implicit-enable behavior for existing configs.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0047
- Related docs: `data/config/peering.yaml`, `README.md`
