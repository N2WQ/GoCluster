# TSR-0055 - GRID Prefix Acceptance And Mapping Feedback

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet GRID, configuration validation and NEARBY cells
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

Human SET GRID accepted a usable locator prefix with trailing text, while the
exported machine configuration required exactly four or six characters. Valid
locator syntax could also receive an invalid-grid response when its fine cell
could not map. An active human NEARBY grid change checked the fine cell without
requiring a usable resulting coarse cell before publication.

## Root cause or best current explanation

The shared locator parser intentionally accepts a prefix for its general path
use. The human boundary checked only a minimum length and read only the first
argument. The machine boundary checked exact length but combined syntax and
mapping into one error. The prior warning-format regression with a percent
sequence in GRID demonstrated the same human prefix acceptance during the
checkpoint-08 baseline; preserving that input as a valid command would preserve
the defect.

## Fix or mitigation

The telnet boundary requires one exact four- or six-character locator. Human
commands normalize it to uppercase; machine proposals retain their uppercase
and no-whitespace contract. Syntax and unavailable fine mapping are separate
refusals. Active NEARBY also requires the resulting coarse cell before any
candidate mutation. Invalid/unavailable commands preserve current settings and
the stored record. Inactive NEARBY retains its existing fine-only admission.

The general parser, H3 math, table loader and once-owned mappings remain.
Empty machine GRID retains its lookup sentinel. No stored records are rewritten
by a migration. Required mapping startup remains governed by
[ADR-0153](../decisions/ADR-0153-startup-config-diagnostics-and-gridstore-logging.md),
and derived Spot cache ownership by
[ADR-0146](../decisions/ADR-0146-h3-path-cell-duplicate-work-removal.md).
This corrects the existing command contract; no new ADR is required.

## Checker Boundaries

Both-dialect tests call the actual human coordinator, compare immediate saved
bytes and runtime cells/revisions, and validate accepted full configurations
through the machine boundary. Valid active grid changes must update both cells
while retaining the location-restoration snapshot. The warning formatter still
has its direct literal-argument regression; malformed GRID has separate refusal
coverage.

Mapping faults require fresh joined children because H3 maps initialize once.
One child loads no tables; another loads private count-valid tables with only
the target coarse entry removed. Positive fine/old-grid controls and missing
coarse controls are mandatory. A syntax error cannot substitute for an
availability failure, and inactive/active behavior is checked separately.

Fuzzing calls the actual staged handler with bounded direct input and an
independent positional ASCII grammar. The checkout driver requires refusal
without live/revision/file effects, exact canonical/effective/saved agreement,
successful machine VALIDATE, and active cells after server replacement. Pause
remaining time is excluded from equality because time continues during commands.
HELP checks require exact four-or-six wording. These do not establish geographic
model quality or production-load behavior.

## Decision Disposition And Verification

The [execution record](../command-fix-execution.md#checkpoint-09-exact-grid-and-truthful-mapping-availability)
contains observed checks and limitations. The
[telnet guide](../../telnet/README.md#grid-noise-and-nearby) explains syntax,
availability and active NEARBY preparation.

Support answer: use exactly four or six characters, such as FN31 or FN31PR.
An availability refusal means that the server cannot map the valid locator;
it does not mean that the locator syntax is wrong. The prior grid stays active.
