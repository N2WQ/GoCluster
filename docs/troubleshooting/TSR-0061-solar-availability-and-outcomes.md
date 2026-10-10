# TSR-0061 - SOLAR Availability And Outcomes

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: solar summaries, command feedback, configured and effective cadence
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

SET SOLAR promised a periodic message even when the node had no solar manager.
Machine status copied the configured cadence into effective status, and human
settings did not explain that enabled nodes emit only active-condition summaries.

## Root cause or best current explanation

Feedback described the saved cadence without checking service availability or
the existing Summary contract. Summary is empty for disabled, quiet and
unobserved conditions, so its output cannot establish availability.

## Fix or mitigation

A read-only query of immutable manager enablement distinguishes availability
from current conditions. Disabled nodes retain configured and saved cadence;
machine effective cadence is zero. Enabled-node feedback and HELP make delivery
conditional on active conditions. OFF and deliberate human same-value
rescheduling retain their existing behavior. Persistence failure remains warned.

The private dispatch loop keeps aligned ticks, empty-slot advancement and one
message for a late tick. Polling, model thresholds, override gates, timers,
machine admission and LOAD/publication behavior remain unchanged.

## Checker Boundaries

Solarweather owner tests distinguish fresh calm observations from unavailable
feeds and retain the literal active R3/G2 summary. Real telnet control queues and
literal UTC deadlines qualify supplied-summary dispatch before, at, repeatedly
at and after the selected ticks. An empty tick consumes its slot; a subsequent
active summary waits for the next tick. OFF sends nothing. These are composed
owner/dispatcher controls, with the unchanged wrapper inspected from source;
they do not establish a live polling-to-wire bridge or poll-loop retirement.

The isolated checkout uses loaded enabled and disabled profiles without feed
polling. Actual startup/retirement receipts verify availability. Both dialects
check truthful command feedback, commit-failure warnings, immediate saved bytes,
configured/effective status across schemas 1–4, repeated read preservation and
fresh-child restoration. Offline checker faults reject wrong numeric types,
availability, wording, duplicate rows and changed preference owners.

## Decision Disposition And Verification

The [execution record](../command-fix-execution.md#checkpoint-14-solar-availability-and-conditional-outcomes)
retains observed validation and the technical falsifiability disposition.
No durable behavior policy changes; ADR-0089's HELP routing remains intact.
The [telnet guide](../../telnet/README.md#human-configuration-readbacks) and
[operator guide](../OPERATOR_GUIDE.md) explain saved and effective cadence.

Support answer: select 15, 30 or 60 minutes for aligned summary ticks. Enabled
nodes send messages only while conditions are active. A disabled node can save
your choice for future use but currently reports effective zero. Reads preserve
your setting and schedule; a valid human SET deliberately reapplies scheduling.
