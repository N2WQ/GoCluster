# ADR-0340: CTY Coordinate and Classification Authority

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Troubleshooting chat; exact Approved v5

## Context

Raw west-positive CTY longitude was sent to an east-positive converter. Exact-only
keys entered prefix fallback. Disk hydration treated partial legacy metadata as
current classification, lacking coordinates/file identity; later fallback could
manufacture JJ00. Clear-on-refresh alone allowed late old results to repopulate.
The owner retained persistence of new estimates and selected a later empty-store
cutover with verified backup. Production operations remain separately authorized.

## Decision

- Preserve raw north-positive/west-positive PrefixInfo and CLI values. Share one
  CTY-specific longitude conversion in grid backfill and SHOW PROP. Preserve the
  generic converter and reported/explicit-grid paths.
- Preserve Data/Keys and full-key lookup. Only ordinary keys enter prefix fallback;
  portable precedence, normalization and beacon handling remain unchanged.
- Disk supplies grid/provenance, not CTY authority. Classification and coordinates
  come from the supplied in-memory snapshot. Legacy disk fields/encoding remain
  compatible but cannot establish a checked CTY result.
- Tag positive and negative cache entries with that database's atomically
  initialized stable identity. A nonzero-size token has no database back-reference.
  The existing bounded LRU owns/evicts tokens; there is no generation registry.
  Late old results keep their old identity and cannot serve new-snapshot callers.
- Retain estimate persistence, reported precedence and ADR-0338 retirement. Add
  no automatic repair/reset/import. Prepare an isolated empty database/recovery
  set and verified backup for the separately authorized operator rollout.

## Alternatives considered

1. Flip longitude in the loader: changes the raw API/CLI and risks double conversion.
2. Delete exact-only keys: breaks full-key lookup and inventory.
3. Trust persisted CTY: lacks coordinates/source identity/current authority.
4. Clear only on refresh: an old lookup may publish afterward.
5. Store database pointers per entry: retains large historical images unnecessarily.
6. Stop persisting estimates: the owner explicitly selected continued persistence.
7. Guess legacy row provenance: broken flags, length and matching values do not
   establish source; an empty rebuild removes ambiguity at the cost of coverage.

## Consequences

### Benefits

Representative coordinates follow the publisher convention. Exact exceptions no
longer change unrelated calls' country filters/license routing. Partial disk
metadata and different-snapshot cache entries cannot replace current classification.
Existing grid evidence and restart persistence remain.

### Risks

Each classification entry gains one pointer; the local allocator size class
adds 16 bytes per cold-entry churn allocation, with unchanged allocation count
and zero warm allocations. This is not a process memory ceiling. Published
databases remain immutable, as required by their existing read-only trie.
Persisted estimates can still age across refresh; cache clearing can temporarily
hide a pending reported grid. No live performance or RF-accuracy claim follows.

### Operational impact

No config/schema/default, queue, timeout or generic boundary change. Empty rebuild
temporarily loses valid locator coverage too. Protected input, safe first stop,
verified backup and isolated recovery remain deployment obligations.

## Links

- [TSR-0126](../troubleshooting/TSR-0126-cty-grid-classification.md)
- [ADR-0338](ADR-0338-grid-store-retirement.md)
- [Operator procedure](../runbooks/cty-grid-rebuild.md)
- [Approved execution/validation](../cty-grid-repair-v5.md)
- Tests: `cty/prefix_contract_test.go`, `cty/maidenhead_test.go`,
  `cty/snapshot_test.go`, cluster `cty_*_test.go`, telnet `cty_*_test.go`.
- Normative: [Country Files](https://www.country-files.com/cty-dat-format/),
  [IARU-R1 Handbook 10.03 pp.107–109](https://www.iaru-r1.org/wp-content/uploads/2026/02/VHF_Handbook_V10_03_final.pdf).
- Supersedes / superseded by: none.
