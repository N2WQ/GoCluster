# TSR-0018 - Peer Bulletin Duplicate Fanout

Status: Resolved
Date Opened: 2026-04-19
Date Resolved: 2026-04-19
Owner: Codex
Technical Area: peer, telnet fan-out, config
Trigger Source: Chat request
Led To ADR(s): ADR-0063
Tags: peering, bulletins, dedupe, bounded-state

## RCA Summary

- What happened: Telnet users saw repeated WWV/WCY and `TO ALL` announcement
  bulletins when peers delivered the same control-plane message through
  multiple paths.
- Why: Bulletins bypassed the spot dedupe pipeline, peer-level keys were raw
  and hop-sensitive, and telnet bulletin fan-out had no duplicate suppression.
- What fixed it: ADR-0063 added configurable bounded telnet bulletin dedupe and
  changed peer WWV/WCY/PC93 keys to canonical payload fields.
- How we know: Source inspection separated bulletin fan-out from spot dedupe,
  and config, peer, and telnet tests validated suppression and keying behavior.
- Operator/support answer: For repeated bulletins, check the telnet bulletin
  dedupe counters and canonical peer keys before investigating spot dedupe.

## Triggering Request
- Request date: 2026-04-19
- Request summary: Review why announcements, WWV, and WCY messages repeat to telnet users as they arrive from peers, then implement an effective dedupe solution.
- Request reference: Chat request in this implementation thread.

## Symptoms and Impact
- What failed or looked wrong? Telnet users saw repeated WWV/WCY and `TO ALL` announcement lines when the same bulletin arrived through multiple peer paths.
- User/operator impact: Bulletin noise consumed control-queue capacity and made the live feed harder to scan.
- Scope and affected components: `peer.Manager.HandleFrame`, peer bulletin dedupe keys, telnet bulletin fan-out, runtime YAML config.

## Hypotheses Tested
1. Hypothesis A - Spot dedupe should already suppress the lines.
   - Evidence: `PC23`/`PC73` and `PC93` bypass the spot pipeline and call telnet bulletin callbacks directly.
   - Outcome: Rejected.
2. Hypothesis B - Peer dedupe keys are too raw/hop-sensitive.
   - Evidence: `wwvKey` and `pc93Key` used `Frame.Raw`, so hop-only route variants were distinct.
   - Outcome: Supported.
3. Hypothesis C - Telnet needs a separate bulletin dedupe guard.
   - Evidence: `telnet.Server.broadcastBulletin` filtered clients and enqueued control messages without any duplicate suppression.
   - Outcome: Supported.

## Findings
- Root cause: Bulletins are control-plane telnet output and do not use the shared spot dedupe pipeline; peer-level raw-frame keys also allowed hop variants through.
- Contributing factors: Multiple peers can deliver the same bulletin, and all-source relay passthrough shares the same telnet bulletin path.
- Why an ADR was required: The fix adds YAML configuration, retained server-lifetime state, and user-visible duplicate suppression semantics.

## Fix or Mitigation
- Added configurable telnet bulletin dedupe for WWV, WCY, and `TO ALL` announcements.
- Changed peer WWV/WCY and PC93 keys to use canonical payload fields rather than raw hop-bearing wire text.
- Added bounded retained-state tests, observability, and documentation.

## Verification and Monitoring
- Validation steps:
  - `go test ./config -run "Test.*BulletinDedupe" -count=1`
  - `go test ./peer -run "Test(WWVKey|PC93Key|HandleFrameWWV|HandleFramePC93Announcement).*" -count=1`
  - `go test ./telnet -run "TestBulletinDedupe" -count=1`
- Signals to monitor:
  - Telnet bulletin dedupe accepted/suppressed/evicted counters.
  - Control queue drops and sender failures during bulletin fan-in.
  - Peer reconnect churn if upstream duplicates coincide with broader overload.

## References
- Related ADR(s): ADR-0050, ADR-0053, ADR-0054, ADR-0063
- Related docs: `README.md`, `telnet/README.md`, `peer/README.md`, `data/config/runtime.yaml`
