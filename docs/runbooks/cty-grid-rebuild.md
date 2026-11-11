# CTY grid correction and an empty-store cutover

This is a separately authorized operator rollout, not an automatic migration.
Local implementation and disposable tests do not authorize a production stop,
backup, reset, import or deployment. Record actual build/process identity, paths,
CTY checksum, stop result, backup verification and startup evidence.

## Corrected behavior

Country Files uses north-positive latitude and west-positive longitude. One
CTY-specific conversion negates longitude once for grid backfill and SHOW PROP.
The raw loader/CLI and generic east-positive geographic converter are unchanged.
With the audited file, K1ABC -> FN42, W6ABC -> DM05, JA1ABC -> PM95, VE3 -> FN03
(field FN), and VK -> PG66. These are cells containing representative coordinates,
not verified station locations. ExactCallsign concerns matching, not accuracy.

Exact-only keys retain full-key matching but cannot classify longer unrelated
calls. K1D remains Puerto Rico; K1DW keeps its exception; K1DG falls back to the
ordinary US entry. Correct classification can change country filters and route
login through the existing US license check.

Disk records supply locator/provenance, never runtime CTY classification. Current
positive/negative results belong to the supplied in-memory CTY snapshot. An old
lookup may finish after refresh, but its tag cannot be reused for a new snapshot.

New CTY estimates **continue to be persisted**, marked derived and displayed in
lowercase. Reported locators take precedence. Existing queue-overflow drops and
storage-error handling remain; not every attempted update is guaranteed durable.

## Preparation and protected input

1. Identify the running executable/revision and replacement. The replacement
   must contain the CTY repairs and complete grid retirement under ADR-0338.
   New code cannot protect the first stop of a still-running legacy executable.
2. Rehearse using disposable data. Maintained tests exercise actual grid startup,
   flush/reopen, backup verification, checkpoint selection, restart and storage
   rollback. They do not certify the deployment's service, prior executable,
   production permissions, database or live traffic.
3. Plan concrete controls for every upstream feed, peer link and client entry.
   PAUSE and a quiet display do not prove lookup quiescence. For a build predating
   ADR-0338, establish a deployment-specific first-stop/recovery plan first.
4. Choose space and separate locations for the old store/recovery tree, complete
   verified backup and new store. Keep backup outside active paths and scheduled
   checkpoint retention. Do not delete the old store to make room.
5. Copy the exact intended plist, parse it and record its checksum. Check known
   eastern/western examples against actual entries. Configure `cty.file` to that
   verified copy and keep CTY enabled. Empty `cty.url` defaults to upstream; it
   does not disable refresh.
6. Protect the copy against replacement by the actual node identity. On Windows,
   file write/delete and parent-directory delete-child authority both matter;
   a read-only attribute alone is insufficient. A correctly scoped deployment
   permission policy or read-only mount may satisfy the requirement.
7. Force genuinely changed, valid HTTP content through the actual download path.
   Require replacement denial, unchanged original checksum and successful reading
   of the intended copy at startup. A 304, same-content response, blocked request
   or earlier temporary-file creation failure cannot count as this proof.
8. Keep protection until last-known-good CTY publication is available or the owner
   deliberately restores ordinary refresh. Current download publication precedes
   parsing; this repair does not change it.

The Windows disposable test changes only its own temporary objects, restores
their original DACL/protection, and proves successful publication afterward as a
positive control. Microsoft documents the relevant [file access rights](https://learn.microsoft.com/en-us/windows/win32/fileio/file-access-rights-constants)
and [DACL access requirements](https://learn.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setnamedsecurityinfow).
It does not set production permissions or machine policy.

## Stop, back up, and start clean

1. Suspend planned traffic, stop the node and verify complete process exit and
   the actual stop result. If it panics, shutdown is uncertain or the first stop
   lacks the agreed protection, preserve the old database/recovery tree and defer
   cutover. Do not silently substitute a partial copy.
2. Copy the complete **closed** Pebble database, or use a coherent checkpoint
   including final committed writes. Keep required WAL/manifests and all database
   files. A raw copy of a running database is not this procedure; an earlier
   checkpoint may omit subsequent commits.
3. Verify the backup opens and scans successfully. Inspect a separate verification
   copy when using a writable Store API to check literal records; preserve the
   backup itself. Record verification, location, CTY checksum and prior build.
4. Configure a new absent/empty `grid_db` directory and import **zero legacy
   rows**. Its recovery set must also be new: checkpoints live under
   `<grid_db>/checkpoint`. Keep legacy candidates outside it. Recovery deliberately
   carries a destination's existing checkpoint tree forward; never mix the sets.
5. Record the absent/empty path before first start and hold normal traffic while
   verifying startup and controlled cold fallback. The production grid startup
   creates a fresh store; zero rows before lookup/update is asserted locally.
6. Verify corrected cold fallback and a newly reported locator. SHOW PROP requires
   the user's grid, enabled path reliability and the supported VOACAP provider.
   With the audited input, cold JA1ABC is PM95; backfill timing can label its
   source `cty-derived` or `gridstore-derived`. A genuine report may legitimately
   differ from the representative estimate.
7. Verify cold restart preserves both grid classes/provenance before restoring
   traffic under the separately authorized deployment plan.

## Rollback and limits

Stop the new build through its corrected retirement path, preserve its store
separately, and restore the verified legacy backup with a compatible prior build.
Verify intended rows/path. Keep recovery sets separate. Old affected code/data
restore their old defects. Local rehearsal proves storage/startup wiring, not
the production service or a particular prior executable.

- Empty rebuild loses genuine locators too until relearned. Expect more lowercase
  estimates and reduced empirical path coverage. In-memory path buckets rebuild
  at restart. Recovery depends on traffic; learning admission/weights are unchanged.
- Persisted estimates are not regenerated automatically on every CTY refresh.
  Row age does not guarantee location freshness. Combined-cache clearing may
  temporarily hide a pending reported grid; merge precedence protects its commit.
  Future freshness work must preserve the chosen persistence policy.
- A six-character row was not emitted by this four-character generator, but its
  length cannot establish source, syntax, freshness or station association. Any
  later fill-only import needs separate validation/precedence approval.
- JJ00 is legitimate at the coordinate origin, not a universal deletion rule.
  Matching an old CTY calculation or guessed mirroring cannot establish source.
- Portable/call-area policy, location-quality learning, safe CTY publication and
  generic pole/antimeridian behavior remain separate work.

See [ADR-0340](../decisions/ADR-0340-cty-grid-authority.md),
[TSR-0126](../troubleshooting/TSR-0126-cty-grid-classification.md), and
[v5 execution evidence](../cty-grid-repair-v5.md).
