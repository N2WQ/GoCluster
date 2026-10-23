---
title: "TSR-0110: Boolean DXCC Prefix Input"
description: Historical canonical DXCC operands lost during Boolean replacement and restored at request preparation.
---

Status: Resolved
Date Opened: 2026-10-23
Date Resolved: 2026-10-23
Owner: Repository maintainer
Technical Area: filter, telnet, CTY, schema-5 requests
Trigger Source: Chat request
Led To ADR(s): ADR-0321
Tags: dxcc, boolean, compatibility

## RCA Summary

- What happened: `DXDXCC<>K` was refused although the old category parser accepted K.
- Why: Boolean DXCC compilation used integer conversion only; the old CTY canonical resolver was not wired into input preparation.
- What fixed it: Human and YAML requests share preparation-only resolution; accepted rules remain numeric.
- How we know: Historical source inspection and focused native tests of both fields/operators, snapshots, failures and persistence.
- Operator/support answer: Use `PASS RULE 0 MODE=CW AND DXDXCC<>K`; acceptance shows `DXDXCC<>291`. Numeric input needs no CTY.

## Triggering Request

- Request date: 2026-10-23
- Request summary: Restore canonical-prefix input alongside numeric codes without changing matching or storage.
- Request reference: Owner conversation, approved Scope Ledger v3.

## Symptoms and Impact

Previously familiar operands required memorized ADIF numbers in the Boolean
checkout. Human commands and machine requests shared the numeric-only compiler.
This record establishes local behavior, not a production incident or deployment.

## Timeline

1. 2026-10-23 17:09 - Investigated refusal of the canonical U.S. label K.
2. 2026-10-23 17:26 - Owner required NEARBY and overflow boundary regressions.
3. 2026-10-23 17:27 - Owner approved v3; implementation and focused validation followed.

## Hypotheses and Tests

1. K was not a supported old operand.
   - Evidence/commands: Historical `telnet/dxcc_commands_test.go` and `parseDXCCList` at `9c735a16b2fe730ce63b5218fda69a9192407243`.
   - Outcome: Rejected. Source verifies k/K, VE, I/IG9/IT9, 3D2/R and 3Y/P, including mixed lists; W6 and K1ABC were rejected.
2. Restoring the existing resolver can preserve numeric matching and persistence.
   - Evidence/commands: `git diff --exit-code 9c735a16b2fe730ce63b5218fda69a9192407243 HEAD -- cty/dxcc.go` showed no resolver changes before implementation. Focused tests exercise the new request wiring.
   - Outcome: Supported. Labels normalize before publication and native reload remains CTY-free.

## Findings

- Root cause: Boolean input compilation omitted existing canonical CTY resolution.

Resolve exact canonical Prefix fields, not callsign lookup keys or portable calls.
One request must reuse one snapshot. Overflowing integers must not become
label candidates, and NEARBY projection must not hide invalid operands.
Classify the complete integer token, as the old parser did: integer conversion
can report overflow before encountering a nonnumeric suffix in a label.

## Decision Linkage

- ADR created/updated: [ADR-0321](../decisions/ADR-0321-boolean-canonical-dxcc-input.md).
- Decision delta summary: Restore historical label resolution at human/YAML request preparation only.
- Contract/behavior changes: Accept canonical labels for both DXCC fields; keep numeric definitions, schema, storage loaders and matching unchanged.

## Verification and Monitoring

- Validation steps run: Native Windows runner for filter, telnet and cty with `-Run 'TestRuleCompile|TestDXCC|TestCanonical' -Timeout 180s`: 93 passed cases, zero skips. Historical tests themselves were source-verified, not executed.
- Signals to monitor: Explicit unknown/conflicting/unavailable-CTY errors; numeric acceptance and readback.
- Rollback triggers: Partial edits, mixed-snapshot resolution, or nonnumeric persistence. Compatible prefeature Boolean storage is expected to read numeric records; historical-binary rollback was not executed.

## References

- Related ADRs: [ADR-0248](../decisions/ADR-0248-canonical-dxcc-input-and-human-labels.md), [ADR-0321](../decisions/ADR-0321-boolean-canonical-dxcc-input.md).
- Related tests: [compiler regressions](../../filter/rule_compile_test.go), [human/YAML regressions](../../telnet/dxcc_commands_test.go).
- Related docs: [DXCC input contract](../../telnet/README.md#canonical-dxcc-prefixes).
