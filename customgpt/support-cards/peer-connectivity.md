# Support Card: Peer Connectivity

## Match

Use for failed peering, CCCluster startup stalls, repeated peer disconnects or
questions about PC20, PC22 and startup pings.

## First Safe Check

Separate remote login rejection from an incomplete protocol handshake. Request
redacted peer family, deployed version, connection state and bounded wire/log
evidence. Keep private hosts, passwords and topology redacted.

## Must Include

- Duplicate-stream refusal happens before peering; freeing a stream does not
  prove the subsequent handshake succeeded.
- Outbound ccluster answers PC20 after initialization with fresh configuration
  (PC9x A/K or legacy PC19), then PC22 and completes establishment. Outbound
  DXSpider still requires remote PC22. Inbound family paths remain unchanged.
- Handshake PC51 replies follow existing destination rules and priority output.
  Pings do not establish authority or extend fixed deadlines.
- Bannerless startup eligibility remains; local initialization is not proof of
  remote identity. Startup A/K cannot replace mandatory established C/A recovery.
- Clock/publication/admission/control-queue gates can close otherwise valid
  sessions. Reconnection alone cannot cure a still-failing gate.
- A wire simulation, a production-session test and a deployed cluster result
  support different claims. Short successful sessions do not prove complete
  CCCluster interoperability or sustained-resource qualification.

## Must Avoid

- Do not add diagnostic PC18 banners or suggest extending timeouts as the fix.
- Do not attribute every failed login to protocol compatibility.
- Do not infer establishment from TCP reachability, PC18 or initial A/K alone.

## Sources

- [Peer control plane](../../peer/README.md#control-plane)
- [ADR-0260](../../docs/decisions/ADR-0260-cccluster-outbound-handshake.md)
- [TSR-0045](../../docs/troubleshooting/TSR-0045-cccluster-outbound-handshake.md)
- [PC92 qualification](../../docs/pc92-qualification.md)
