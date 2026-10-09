# TSR-0051 - Stored YAML Alias Compatibility

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: persisted user records and preset collections
Trigger Source: Chat request
Led To ADR(s): ADR-0267

## RCA Summary

A file's size does not describe the work needed to decode YAML aliases and
merges. Against the pinned production loader, the 125-byte fixture in
`filter/storage_decode_compat_test.go` reaches a nested merge and is refused with
`yaml: document contains excessive aliasing`. Its 118-byte version without the
root merge is accepted: the same anchors are present but their expansion is not
visited. A neighboring width-six, 116-byte fixture is accepted.

This is a minimized, observed reproducer. It is not the unsupplied 325-byte
fixture referenced by the planning review. The frozen old decoder corpus was
committed before replacing production decoding.

## Root cause or best current explanation

The existing guard measures the proportion of alias-driven decoding visits at
each prefix. Ignored keys can dilute that ratio, and nested custom decoders start
fresh guards. Dropping ignored keys before checking the guard can therefore
refuse a previously accepted record. Skipping generic parent-key interpretation
during merge preparation can instead lose an existing refusal.

The pinned dependency also panics when generic merge bookkeeping uses a map or
list as a parent key. A direct stock-decoder probe reproduced both unhashable-key
panics. Replacement decoding must refuse those malformed inputs without a panic;
no formerly accepted configuration is admitted or removed by that control.

Fresh review also found that a null tag does not uniformly clear a node: the
stock decoder still dispatches mappings and sequences by their node kind. Eight
literal regressions exposed and verified the corrected behavior. A null-tagged
root with inline fields reproduces a stock invalid-reflection panic and is now
refused; other accepted null-tagged mappings/sequences retain their values.

## Checker Boundaries

Compare complete typed values and refusal classes against the frozen record,
reference, baseline and collection callbacks. Keep unchanged migration and
prevalidation helpers identified as shared oracle seams. Literal controls cover
reachable/unused aliases, cycles, raw and interpreted duplicates, merge ordering,
shadowed malformed values, nulls and nested decoder resets.

Physical work follows Content only, including ignored values and unused anchors.
Logical work follows original typed-decoding visits, including aliases, merges,
bookkeeping and custom handoffs. Scalar Value UTF-8 bytes are added to each
applicable visit. These units measure processing work, not file size or measured
heap use. Account once even if the implementation has multiple passes.

Work accounting is introduced with its new thresholds inactive in checkpoint
04a. Complete admission activates only in 04d, after exact persistence and
detached human publication. The existing preset limits and old alias guard
remain in force throughout.

## Fix or mitigation

[ADR-0267](../decisions/ADR-0267-compatible-stored-yaml-decoding.md) records the
storage-local traversal choice. The [execution record](../command-fix-execution.md#checkpoint-04a-compatible-storage-decoding-and-work-accounting)
separates observed qualification from remaining tests. Operator support should
retain refused files and investigate the specific error; automatic deletion,
rewriting or flattening of their aliases is not a repair procedure.
