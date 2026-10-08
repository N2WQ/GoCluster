# TSR-0045 - CCCluster Outbound Handshake

Status: Monitoring
Date Opened: 2026-10-08
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: peer/session, peer/pc92_controller
Trigger Source: Operator report
Led To ADR(s): ADR-0260
Tags: peer, ccluster, handshake, PC20, PC51, deadlines

## RCA Summary

- What happened: A reachable CCCluster node did not become an established
  outbound GoCluster peer.
- Why: Outbound startup ignored CCCluster's PC20 configuration request and did
  not answer startup PC51 requests.
- What fixed it: Answer startup pings and, for configured ccluster peers after
  local initialization, answer PC20 with fresh configuration then PC22.
- How we know: Current-source diagnosis, pinned DXSpider handler comparison,
  bounded wire simulation, production-session regression tests and a live test
  of the corrected manager; deployment remains separate.
- Operator/support answer: Check login refusal, configured peer family, startup
  markers and ping replies. Reachability alone does not prove establishment.

## Triggering Request

- Request date: 2026-10-08.
- Request summary: Diagnose live CCCluster peering, compare DXSpider and fix the
  handshake.
- Request reference: Owner troubleshooting chat and Approved v1.

## Symptoms and Impact

- TCP/login could succeed while no spots arrived and the connection later closed.
- First login was rejected with "You are already connected on another stream";
  that duplicate-session refusal is separate from the handshake defects.
- Scope: outbound ccluster completion and handshake PC51 service. No runtime
  configuration changes or deployment are included in this correction.

## Timeline

1. 2026-10-08 - First authorized login encountered duplicate-stream refusal.
2. 2026-10-08 - A freed session exposed PC20 and startup PC51 behavior.
3. 2026-10-08 - Source comparison and corrected wire simulation supported the fix.
4. 2026-10-08 - Owner authorized implementation with Approved v1.
5. 2026-10-08 - Corrected production manager established a live session for
   90 seconds, ingested seven spots and released session/staging on shutdown.

## Hypotheses and Tests

1. Duplicate-session rejection explains every failure.
   - Evidence: First login was rejected before protocol startup; a freed stream
     subsequently logged in and stalled during startup.
   - Outcome: Rejected as the sole explanation.
2. CCCluster must send PC22 before GoCluster can proceed.
   - Evidence: Remote PC20 arrived without PC22 during the stalled exchange.
     Pinned DXSpider answers PC20 with configuration and PC22, then enters normal
     operation.
   - Outcome: Rejected for configured outbound ccluster sessions.
3. Startup ping replies and the configuration response permit live peering.
   - Evidence: Corrected wire simulation sent initial A/K and PC20, answered
     pings, answered remote PC20 with fresh A/K and PC22, then sent membership
     C/A. Remote PC22 and eight spots arrived over 105.1 seconds. A modeled
     production-style exchange without ping replies received two pings, no spots
     and closed after 66.83 seconds.
   - Outcome: Supported for this bounded simulation. The comparison does not
     isolate each correction's independent effect or prove deployed behavior.
4. An extra diagnostic PC18 is needed.
   - Evidence: An earlier nonproduction PC18 injection occurred after remote
     PC20 and a live spot had arrived, and elicited another remote PC18.
   - Outcome: Inconclusive variation; unnecessary for the successful baseline.
     The correction adds no outbound diagnostic banner.

## Findings

- Root cause: Completion differed by family; ping handling started too late.
- Contributing factors: The PC reader discards non-PC server text, so generic
  handshake/session errors can hide duplicate-stream rejection. Logging changes
  remain outside this correction.
- Durable decision: Preserve strict DXSpider sequencing, fixed deadlines,
  authority and C/A recovery while answering CC-specific startup requests.

## Decision Linkage

- ADR created: [ADR-0260](../decisions/ADR-0260-cccluster-outbound-handshake.md).
- Decision delta: After initialization, ccluster PC20 receives fresh negotiated
  configuration and PC22; startup pings receive normal replies.
- Contract changes: Separate bounded A/K response progress. Inbound family paths,
  authority winner, queue refusal and fixed phase deadlines remain unchanged.

## Verification and Monitoring

- Validation steps run: Preimplementation simulation described above; final
  `go test ./peer -run '^TestCC' -count=1` passed (7.351s), including modern and
  legacy response ordering, same-spot admission before/after completion, fixed
  deadlines, duplicate markers, queue refusal, partial timestamp retry and
  fresh candidate ownership. The pinned DXSpider receiver wrapper passed
  (64.458s), including actual inbound/outbound modern/legacy Go sessions.
- Production live evidence: `go test ./peer -run '^TestCCLiveProductionSession$'
  -count=1 -v -timeout=150s` with explicit authorized endpoint
  `dxc.n2wq.com:7300`, local `N2WQ-2`, remote `N2WQ-1`, using the
  `GOCLUSTER_CC_LIVE_ENDPOINT`, `GOCLUSTER_CC_LIVE_LOCAL` and
  `GOCLUSTER_CC_LIVE_REMOTE` environment variables. It passed in 91.39 seconds:
  connected, established for 90 seconds, seven spots ingested, and active
  session/candidates/staged records and bytes released on manager shutdown.
  Ordinary test runs skip this opt-in network test. This is corrected manager
  evidence, not a production deployment or sustained-load qualification.
- Protocol fuzz: `go test ./peer -run '^$' -fuzz '^FuzzParseFrameHopSuffix$'
  -fuzztime=30s -parallel=2` passed with 158,225 executions.
- Final baseline: `go test ./... -count=1`, `go vet ./...`,
  `staticcheck ./...` and `golangci-lint run ./... --config=.golangci.yaml`
  passed; lint reported zero issues. `go test -race ./...` also passed,
  including the peer package (107.998s). The separate final peer package run
  passed (82.848s). No deployed result is claimed.
- Earlier validation failure: The first full test run overlapped race and
  analyzer processes. Two existing logging archive tests could not find their
  expected file, and the existing outbound backoff test observed 397.8405ms
  against its 400ms requirement. No code was changed to mask these failures.
  The three tests passed three isolated repetitions using an overlay of the
  unchanged HEAD production files; the two logging tests also passed 50
  repetitions. The complete final test rerun without overlapping validation
  processes passed. Load/timing sensitivity is an inference, not a proven
  cause. Local command logs remain in `.tmp/cc-handshake-v1/`.
- Signals to monitor: Login refusal, peer family, configuration/PC20/PC22,
  ping destinations, establishment, recovery C/A, spots and disconnect reason.
  Queue admission is not confirmation of remote processing.
- Rollback triggers: DXSpider sequencing regressions, duplicate registration,
  configuration loops, extended deadlines or missing mandatory C/A recovery.

## References

- Related ADRs: [ADR-0260](../decisions/ADR-0260-cccluster-outbound-handshake.md),
  [ADR-0230](../decisions/ADR-0230-pc18-pc92-authority-and-bounds.md),
  [ADR-0231](../decisions/ADR-0231-pc92-audit-corrections.md).
- Related docs: [Peer behavior](../../peer/README.md#control-plane),
  [domain contract](../domain-contract.md#pc18pc92-authority-and-recovery).
- Reference: [DXSpider DXProtHandle.pm](https://github.com/EA3CV/dxspider/blob/3e9b3621d94dd45c68702e4a0f896aac33f2a91d/perl/DXProtHandle.pm),
  PC20 and PC51 handlers. Short simulations do not qualify full interoperability
  or sustained resource bounds.
