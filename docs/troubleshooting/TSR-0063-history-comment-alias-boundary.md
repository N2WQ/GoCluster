# TSR-0063 - History COMMENT Alias Boundary

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: commands, telnet reader, history selections and continuation
Trigger Source: Chat request
Led To ADR(s): ADR-0273

## RCA Summary

A filter-visible configured COMMENT mode alias was supported by the taxonomy,
but history parsing always cut at the first standalone COMMENT. The reader
also enabled arbitrary printable punctuation there, before knowing whether
COMMENT was a mode value. These two scans disagreed with the typed list rule.

## Root cause or best current explanation

History list parsing already distinguished BAND/MODE aliases first or after
a comma from subsequent clauses. COMMENT remainder slicing and the borrowed
reader prefix scan did not apply that decision. Calling the full parser for
each received byte would add unrelated validation and allocation.

## Fix or mitigation

Share one request-local keyword/comma transition in commands. A supported
COMMENT alias is a MODE value first or after a comma; otherwise a standalone
COMMENT starts the literal remainder. Telnet uses the borrowed prefix predicate
after editing. Unsupported or non-filter-visible aliases keep ordinary clause
behavior. Comma-embedded COMMENT is a list value and must be supported.

Existing NEXT guards, dialect restrictions, literal ASCII phrase rules,
validation ordering and connection-owned cursor publication remain. No archive
format, stored preference, process cache, worker or additional lock is added.
The durable rule is recorded in [ADR-0273](../decisions/ADR-0273-history-comment-alias-boundary.md).

## Checker Boundaries

Loaded taxonomy controls cover supported, absent and hidden aliases. Literal
expectations check canonical selections, exact spaces/punctuation and matching
before count-one limits. Composed real reader/history-owner tests cover both
codecs and dialects, editing, refusals without archive calls, complete cursor
preservation and subsequent NEXT. Saved settings, preset, revisions, filter
identity and durable bytes are checked separately.

The cumulative local checkout profiles use explicitly seeded metadata and
actual archive queries, correlated configuration barriers, immediate disk
readback and fresh children. Offline faults reject wrong mode/spacing/count,
incomplete framing, malformed tokens and hidden trailing output. Actual NEXT
requests and connection-owner controls check token validity. These qualify
command behavior; they do not establish model quality, production load or
external-cluster behavior. Bounded fuzzing supplements deterministic vectors.

## Verification And Support

Observed commands and results are retained in the
[execution record](../command-fix-execution.md#checkpoint-16-comment-mode-aliases-and-literal-history-clauses).
For a COMMENT alias mapped to CW, use `MODE COMMENT COMMENT POTA` to select
CW and a POTA phrase. Without that configured alias, `MODE CW COMMENT POTA`
keeps its existing meaning. Copy returned NEXT commands to continue the search.
