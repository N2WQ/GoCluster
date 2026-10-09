# TSR-0056 - Human Callsign Pattern Admission

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet callsign filters and configuration validation
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

Human DXCALL/DECALL commands accepted unsupported wildcard forms and characters.
The saved configuration could fail machine validation, while the actual matcher
interpreted only exact calls and single leading/trailing wildcard patterns.

## Root cause or best current explanation

Both human list handlers split tokens and immediately applied them without a
grammar check. The machine validator already enforced the supported character
and wildcard domain. PASS/REJECT and CC list aliases shared the unchecked owner.

## Fix or mitigation

The human and machine boundaries share the existing private grammar predicate.
Every parsed token is checked before changing any list. Use ASCII letters,
digits, slash or hyphen, with at most one star at the beginning or end. Exact,
prefix, suffix and standalone-star forms remain supported. An invalid token
refuses the entire list and preserves live rules, revision and saved bytes.

Existing separators, uppercase publication, ordering, duplicate handling and
block precedence remain. Standalone ALL retains its reset/block-all operation;
ALL inside a list is literal. PASS star retains other block patterns; like
every PASS pattern, it removes an identical pattern from the block list.
Stored legacy records are not rewritten or pruned. No matcher, schema, default
or migration changes are introduced; no new durable design decision needs an ADR.

## Checker Boundaries

Both-dialect tests exercise DX/DE allow/block through the actual coordinator,
require mixed invalid lists on both sides of valid tokens to make zero commit
attempts, and compare live rules, revision and immediate disk bytes. Every
supported published configuration must pass complete machine validation.

Consumer tests invoke Filter.Matches with literal exact/prefix/suffix and portable
calls, overlapping block lists, ALL and star controls. The DE fixtures explicitly
supply the baseline field that production enrichment supplies; NewSpot alone
does not populate it. This is matcher evidence, not pipeline enrichment proof.

Permanent fuzzing exercises bounded direct staged commands against an independent
regular-expression grammar, and requires invalid inputs to preserve even the
detached candidate. It does not claim separate wire fuzz qualification.

The cumulative checkout runner requires immediate whole-list refusal, exact
canonical/saved lists, machine VALIDATE and correlated visible/blocked archive
rows with specific DX/DE identities. A fresh process must restore the exact
configuration and visibility. Offline negative controls challenge missing lists,
wrong identities, duplicate rows and incorrect visibility.

History reads finish their bounded semantic body before correlated GET, avoiding
interleaved input echo. The checker requires the fixture's actual 78-column
default layout and exact control comment. It does not qualify the ten-row
automatic-pause trailer or production runtime layout initialization. Distinct
letter-only CW/F8 labels avoid SNR parsing and substring collisions. Success and
failure receipts retain decoded response bytes, including pending partial lines,
within 64 KiB; truncation is explicit. They do not capture raw IAC bytes.

## Decision Disposition And Verification

Observed checks and limits are in the
[checkpoint execution record](../command-fix-execution.md#checkpoint-10-callsign-pattern-admission).
The [telnet guide](../../telnet/README.md#human-configuration-readbacks) describes
supported syntax and the ALL/star distinction.

Support answer: use `K1ABC`, `W1*` or `*ABC`. Check every entry if a list is
refused; no entries from that command were applied. Question marks and stars
inside a pattern do not provide additional wildcard matching.
