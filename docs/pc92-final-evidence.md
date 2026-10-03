# Final PC18/PC92 evidence reconciliation

`scripts/pc92-final-qualification.ps1` reconciles the original acceptance
requirements. It does not run the workloads, establish a memory bound from
measurements, or independently certify an engineering review. The current
[validation record](pc92-v15-validation.md) owns execution status. No real final
bundle has passed yet.

Freeze one complete source tree before final execution. Its identity includes
every tracked and non-ignored file, including scripts, tests and documentation,
without changing line endings. Runtime assets, CTY and pinned receiver inputs
are also checked by each workload's original manifests. Keep evidence outside
that tree. Updating the frozen source invalidates reconciliation; do not omit
changed files to obtain a matching digest.

Dot-source `scripts/pc92-qualification-finalize.ps1` and call
`Get-PC92FinalSourceDigest <frozen-root>` to obtain the source digest. Native
Linux evidence must attest the same file bytes using the retained snapshot
manifest. Guest build output and evidence belong outside the source snapshot.
Its path-independent digest is separate from workload manifests, which retain
the actual absolute input paths.
Review the captured `go_build_environment` (including `GOEXPERIMENT`) and
`go_debug` before attesting the allocation derivation. A matching version string
alone does not establish compatible compiler/runtime options. The final checker
does not independently prove that an option preserves the engineering bound.

The bundle is a JSON object with these fields:

| Field | Required content |
| --- | --- |
| `schema_version` | `1` |
| `repository_root` | Absolute path to the frozen source |
| `source_digest` | Exact digest of that source |
| `review` | Object containing absolute `path` and SHA-256 `sha256` of the engineering review JSON |
| `runs` | Array of objects containing `id` and absolute evidence `directory`; every original profile exactly once |

Required run IDs are `runtime/q1`, `runtime/q2`, `runtime/q3`,
`runtime/shipped-q1`, `q4/a`, `q4/b`, `q5/qualification`, `q6/qualification`,
`cache/cache-memory`, `cache/cache-sustained` and `retry/qualification`.
Diagnostic and preflight runs cannot substitute for them. Keep each complete
directory, including verdict, log, observations when applicable, three source
manifests, test executable and sibling `peerdiag.exe`.

Q6 also records named absolute `reference_root` and `perl_executable` paths,
plus `perl_library` and `reference_dll_directory` (empty strings if unused).
The checker revalidates the actual pinned reference and derives its mandatory
Perl source/prefix/executable closure; an empty external-input list or a pin
string alone cannot establish receiver provenance.

The engineering review JSON has `source_digest`, a named `reviewer`, an empty
`open_evidence` array, and arrays named `corrections`, `partitions` and `checks`.
Each entry has `id`, `status: "passed"`, `source_digest`, a substantive `summary`,
an empty `open_evidence` array and a nonempty `artifacts` array. Each artifact
contains an absolute `path` and `sha256`. These are explicit engineering
attestations: hashing a document does not establish that its reasoning is sound.

- Corrections identify every `V15-01` through `V15-14` and `V16-01` through
  `V16-06`, mapping the approved requirement to implementation and executed
  evidence. Missing or incomplete items prevent correction closeout.
- Partitions use `spot_dedupe`, `graph`, `other_dedupe`, `queues`, `staging`,
  `publication`, `projection`, `sqlite`, `diagnostics` and `metadata`, with
  positive integer `bound_bytes`. Their individual limits are respectively
  96,96,32,160,16,12,36,16,3 and13MiB. Every bound includes the applicable
  overlapping and failed-retirement owners. RSS and independent peaks are
  insufficient evidence. Runtime/GC and unchanged configuration remain
  separately reported under the approved boundary.
- Checks use `normal`, `vet`, `staticcheck`, `lint`, `race`, `tagged`, `fuzz`,
  `benchmarks`, `profiles`, `windows-native`, `windows-fallback`, `linux-native`,
  `sqlite-cycles`, `sqlite-sustained`, `packaging` and `documentation`. Each also
  records the actual `command` and integer `exit_code: 0`. Cite exact commands,
  coverage, source/binary provenance, logs and applicable negative controls in
  retained artifacts. The SQLite lifecycle mix and prescribed 30-minute stress
  remain separate requirements; Q4 cannot substitute for them.

Run the final reconciler with a fresh output directory:

```powershell
./scripts/pc92-final-qualification.ps1 -BundlePath <bundle.json> -OutputDirectory <new-directory>
```

It first writes an incomplete verdict and publishes its final result through a
same-directory rename. `audit_corrections_complete` and `overall_accepted` are
separate results. Missing proof or original final-source evidence keeps overall
acceptance false. The final verdict identifies the exact frozen source; it
does not certify a subsequently modified checkout.
Parsed verdict hashes identify the same bytes that were validated. Before
acceptance, the checker rechecks the complete artifact closure, source identity
and external inputs. The final verdict retains those artifact hashes. This
detects changes during reconciliation; it cannot prevent later file changes.

`scripts/test-pc92-final-qualification.ps1` uses synthetic records to test this
checker, including a full positive control. Those logs, binaries, durations and
review attestations are explicitly synthetic and are never execution evidence.
These fixtures substitute a synthetic reference-pin assertion; the separate
wrapper contract suite exercises the real Git pin/dirty-tree checks. Mutation
controls change a verdict, binary, proof artifact or external receiver input
during reconciliation and must prevent acceptance.
