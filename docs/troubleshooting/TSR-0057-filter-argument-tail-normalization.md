# TSR-0057 - Filter Argument Tails Lost During Normalization

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet filter arguments and CC shortcut normalization
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

Named CC shortcuts and inline category flags could execute after silently
discarding extra arguments. Feature switches and NOFILTER similarly ignored
their values, so a command such as PASS BEACON OFF could enable beacons.

## Root cause or best current explanation

The CC parser synthesized a domain/value command before validating original
arity. Normalized handler tests could not observe the discarded tail. Shared
feature/NOFILTER handlers applied their action without admitting arguments.

## Fix or mitigation

Validate original shortcut and inline-flag arity before normalization. A named
shortcut such as SET/BEACON or SET/NOFT8 takes no arguments. A category flag such
as SET/FILTER BAND/OFF takes no extra values. Refusal preserves the whole command.

Feature switches and PASS NOFILTER retain no-value and single-ALL forms;
arbitrary values or additional tokens are refused before mutation. Valid CC
category flags still synthesize ALL. Existing mode deltas, supported shortcuts,
dialect fallback/hints and persistence ownership are retained. Unsupported
NOFILTER/OFF is not given new semantics. This repairs approved command admission
without a new durable architecture decision; no ADR is needed.

## Checker Boundaries

Actual engine/coordinator tests exercise both dialects and CC's GO fallback,
all fixed aliases, current dynamic mode shortcuts, all feature/PC93 routes and
representative inline categories. Invalid tails compare live configuration,
revision and immediate saved bytes and require zero commit attempts. Switches
start in both states; valid controls and literal matcher observations prevent
a blanket refusal or a no-op from passing.

Permanent bounded direct-stage fuzzing uses known shortcut/flag prefixes with
arbitrary nonempty tails. It requires a recognized refusal, no publication and
an unchanged detached candidate. This is not wire fuzz qualification.

The cumulative checkout runner exercises raw command lines through real server
dispatch, requires refusal and immediate live/revision/effective/preset/disk
equality, and verifies valid switch direction, exact inline-flag state and known
CW history visibility. Saved valid selections must survive a replacement child.
Offline controls reject missing refusal and changes to each observed owner.
Known refusals are received completely before correlated GET, so its input echo
cannot interrupt the refusal text. Exact-line, CRLF, size/deadline and partial
failure-receipt controls challenge that ordering rather than stripping loose
echo fragments. Unsupported NOFILTER/OFF has an explicit unchanged-state/disk
and zero-commit regression, not merely a skipped positive case.
Network echo, input recovery and the existing zero/one-row history-body checker
retain their prior qualification boundaries.

## Decision Disposition And Verification

Observed checks and limits are in the
[checkpoint execution record](../command-fix-execution.md#checkpoint-11a-original-filter-arguments).
The [telnet guide](../../telnet/README.md#human-configuration-readbacks) describes
the argument rules. Comprehensive HELP/category reconciliation remains later
approved checkpoints.

Support answer: use REJECT BEACON to disable beacons. In CC, use SET/NOBEACON.
Remove unexpected trailing values from named shortcuts and /ON or /OFF flags.
A refusal makes no live or saved change; correctly supplied list values still
belong after unflagged list commands.
