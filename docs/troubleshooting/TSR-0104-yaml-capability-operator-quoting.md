---
title: "TSR-0104: YAML Capability Operator Quoting"
description: Quote Boolean operator strings so common YAML client loaders accept schema-5 discovery.
---

Status: Resolved
Date Opened: 2026-10-21
Date Resolved: 2026-10-21
Owner: GoCluster engineering
Technical Area: telnet machine YAML capability readback
Trigger Source: Other
Led To ADR(s): none
Tags: boolean, yaml, interoperability, capabilities, mvp

## RCA Summary

- What happened: the private Boolean MVP's CAPABILITIES response failed in the
  existing bounded Python telnet collector using PyYAML 6.0.3.
- Why: Go's YAML emitter wrote an operator as bare `=`, which PyYAML's YAML 1.1
  resolver recognizes as `tag:yaml.org,2002:value`. Its safe constructor cannot
  construct that tag.
- What fixed it: encode every advertised operator as an explicitly quoted YAML
  string. Do not weaken the client loader or change operator semantics.
- How we know: a regression checks scalar kind, string tag and double-quoted
  style in the actual response; the restarted owner-run MVP passed the standard
  Python decoder, CAPABILITIES-fenced logout and reconnect checks.
- Operator/support answer: use the corrected MVP build. A syntactically valid
  response in the server's own YAML library is not sufficient interoperability
  evidence for common client libraries.

## Triggering Request

- Request date: 2026-10-21
- Request summary: implement the approved Boolean-filter MVP, then test its
  actual behavior on an owner-started isolated local cluster.
- Request reference: Scope Ledger v1 and the private-cluster acceptance session.

## Symptoms and Impact

GET CONFIG and ordinary rule editing worked. A later CAPABILITIES request,
used as the collector's correlated logout fence, failed with:

```text
could not determine a constructor for the tag 'tag:yaml.org,2002:value'
      - =
```

The server stayed running. Client discovery and any higher-level operation
depending on that response could fail. The private test had already completed
15 protocol groups, which did not establish that this separate resource was
portable.

## Timeline

1. 2026-10-21 - Actual private-cluster testing exposed the strict-loader failure.
2. 2026-10-21 - Inspection confirmed a bare operator scalar, not truncation or
   invalid CRLF framing.
3. 2026-10-21 - Added explicit string quoting and a wire-style regression.
4. 2026-10-21 - The owner restarted the corrected binary. LZ3NY-11/LZ3NY-12
   completed the protocol and live/archive witness checks with no wire failures.

## Hypotheses and Tests

1. Broken transport framing.
   - Evidence: the bounded collector received complete document markers and
     reached the YAML constructor at the operator scalar.
   - Outcome: Rejected.
2. Server/client scalar interpretation differs.
   - Evidence: Go decoding accepted the bare token, while PyYAML 6.0.3 selected
     the YAML 1.1 value tag. Explicit double-quoted strings decoded normally.
   - Outcome: Supported.

## Findings

- Root cause: capability serialization emitted bare `=` instead of an explicitly
  quoted operator string, allowing a YAML 1.1 resolver to select a special tag.
- Contributing factor: previous Go-only capability tests shared the server
  library's interpretation. The missing oracle was portable string serialization,
  not the Boolean comparison truth matrix.
- Decision impact: this restores the accepted string contract without selecting
  new filtering policy.

## Decision Linkage

- ADR created/updated: none.
- Decision delta summary: implementation correction to the schema-5 string
  contract accepted in [ADR-0317](../decisions/ADR-0317-boolean-filter-replacement-mvp.md).
- Contract/behavior changes: no expression or matching changes.

## Verification and Monitoring

- Native race tests covering readbacks, machine commands, bounded responses and
  active protocol wiring passed after the fix; repository analyzers passed.
- Actual PyYAML 6.0.3 CAPABILITIES parsing and correlated logout passed on the
  owner-started corrected private binary.
- Keep the quoted-scalar regression when changing capability serialization or
  upgrading YAML libraries.
- Do not mask a future malformed frame with tolerant parsing or report partial
  scenario completion as full acceptance.

## References

- Implementation: [capability metadata](../../telnet/machine_v5_readback_capabilities.go).
- Regression: [machine readback tests](../../telnet/machine_v5_readback_test.go).
- Existing live collector: [telnet command harness](../../scripts/test-telnet-commands.py).
- Related ADR: [Boolean MVP](../decisions/ADR-0317-boolean-filter-replacement-mvp.md).
- Related docs: [machine protocol](../../telnet/README.md#client-yaml-configuration).
- Issues/PRs/commits: none created for this local MVP work.
