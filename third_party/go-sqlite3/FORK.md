# GoCluster topology SQLite fork

This is the production-package subset of `github.com/ncruces/go-sqlite3`
v0.35.6, upstream commit `091633ebb62b0c78988d3e079149333b36337599`.
The original MIT license is retained. `UPSTREAM-FILES.json` records the original
SHA256 of every copied upstream file. This private fork is used only by optional
peer topology persistence; other SQLite consumers retain their existing driver.
The preserved upstream README's general API, extension, platform and test claims
are upstream documentation; they are not qualification claims for this subset.

The generated runtime in `engine` is an unchanged production-file subset of
`github.com/ncruces/go-sqlite3-wasm/v6` v6.3.35304, upstream commit
`d923b955a175c983578bd2196ce9ac0511f6159d`. Its original license and production-file
manifest are retained. The minimal module files omit unused extension, example,
test and generator dependencies to preserve GoCluster's shared dependency
versions. They do not regenerate or alter SQLite engine code.

V15 repairs bounded initialization, engine and host backing, release ownership,
and Windows WAL fallback. A failed native release keeps its resource owner
reachable and prevents connection replacement. Logical memory limits alone are
not allocation proof. See the [SQLite validation record](../../docs/pc92-v15-sqlite-validation.md)
for execution evidence and outstanding gates.

The repair inventory is intentionally confined to handwritten boundaries:

- `conn.go`, `topology_owner.go`, wrapper initialization/arena/handle tables:
  own partial initialization, exact allocation failures and fixed host tables.
  Sticky cleanup poison gates later SQL, including successful step/error results;
  deferred arena cleanup does not re-enter the engine after poison.
- `internal/sqlite3_wrap` memory and mapping implementations: fixed engine
  reservation, global fallback slots, retained failed native releases and
  retriable view/handle cleanup. Unix direct MmapPtr/MunmapPtr avoids the
  otherwise retained process-global slice-to-mapping registry while preserving
  the existing flags, extents, invalid-length errors and fault seams.
- `vfs/shm_windows.go` and `shm_ofd.go`: bounded mapping descriptors, corrected
  Windows alignment/native-lock shadow coherence, and partial-unmap retirement.
- `vfs/file.go`, directory helpers, temporary-path helpers and VFS dispatch:
  preserve partial file owners, sync the actual directory, bound temporary
  names before construction, and avoid expanded unknown checksum pragma keys.
- `vfs/filesystem_windows.go`, `metadata_windows.go` and native lock handling:
  bounded owner-local paths and metadata preserve the pinned Go branch order
  without a subsequent stdlib pathname retry. Failed temporary metadata release
  transfers into the existing257-slot table; a possible main/metadata pair is
  admitted before acquisition. Cleanup failure takes priority over masked Access,
  journal and modeof errors. Poison permits genuine retirement releases only,
  prevents fallback publication, and cannot hide failed temporary unlocks.
  A consumed Windows File.Close failure retains a terminal owner until process
  exit; this differs from safely retriable mapping/view retirement.
- `internal/util/parse.go`: reject impossible long nonnumeric boolean words
  before lowercase allocation, preserving existing numeric-prefix behavior.
- Private VFS callback string handling: borrow engine-owned URI/name/value
  spans rather than copying SQL-generated strings; enforce existing modeof
  filename admission before its copy, identify the checksum names before value
  conversion, and preserve observed boolean/emptiness semantics. A temporary
  engine-backed VFS lookup name is neither retained nor used across an engine
  entry. The public copied-string APIs remain unchanged. Actual600,000 and
  1,100,000-byte child fixtures retain baseline allocation/panic evidence and
  prior-driver controls. The fixed OS file's psow flag reuses the existing
  generated SQLite URI boolean getter after23 actual prior/candidate comparisons
  demonstrated the upstream Go first-digit parser's numeric incompatibility.
  This preserves inherited integer/hex/overflow/default/duplicate semantics;
  generic custom VFS and public parser APIs remain unchanged. No new engine
  code, allocation, callback registry or numeric parser is introduced.
- `vfs/path_*.go`: adapt the pinned Go 1.26.4 Linux symlink walk with
  1,024-byte composed-workspace admission before allocation. Linux preserves
  the admitted same-inode PWD spelling and reads links into a fixed buffer.
  Windows follows the inherited topology driver's lexical FullPathname:
  bounded native CP_UTF8/flags0 conversion and GetFullPathNameW, the original
  leading-slash exception, no extra
  Clean/case/reparse walk, and OS open follows the path. The lexical database
  name determines WAL/shared-memory sidecars. The introduced Windows reader
  and find-owner code was removed after retaining exact source and the failing
  compatibility artifacts. Current qualification and remaining findings are
  in the linked record, including the repaired malformed-byte differential and
  the remaining data-directory incompatibility.
  Original Go files, their hashes and the BSD license are retained
  under `provenance`; the generated SQLite engine remains unchanged.
- Qualification-only hooks and focused tests: exercise real allocation and
  cleanup sites, provenance, host bounds and separate-process WAL behavior.

The fallback adaptation retains the original v0.30.0 mapping/shadow files under
`provenance`, alongside their hashes. Fork-local attributes disable newline
conversion for these originals and the unchanged generated engine artifacts.
The Go adaptations additionally carry the Go Authors' BSD license in
`provenance/GO-LICENSE.txt`; `GO-SOURCE-FILES.json` hashes the installed Go 1.26.4
source originals used for this adaptation.
Windows fullpath no longer invokes Go's optional volume-link display conversion
or reads/changes GODEBUG. Targeted native saved-target, malformed-byte,
multiprocess alias WAL and fixed-boundary normal/race/checkptr/vet checks pass
on the identified source generation in the validation record.
Owner-local metadata/open/remove/temp operations now bypass the formerly
unbounded second stdlib pathname conversion, including legacy device paths.
Their fixed buffer and retained-owner inventory is in the validation record;
no approximate OS path limit is used. Cross-platform final-source qualification,
whole-file-link sidecar evidence and actual Windows symbolic-link capability
remain separate obligations. The overall latest short runner remains failed for
the confirmed data-directory incompatibility and required symbolic-link gate.
The topology adapter and its DSN/statement scheduling repairs live in GoCluster
`peer/topology_sqlite*.go`, outside this fork. Exact validation status is recorded
in the linked repository document, including failures and superseded runs.
