# Windows diagnostic native boundaries

The helper's path and directory code adapts selected Go 1.26.4 behavior to
enforce its existing memory reservation before native-output allocation.
It is not a general replacement for the Go filesystem package.

`path_windows.go` adapts `os/path_windows.go:addExtendedPrefix` and follows
the runtime's `initLongPathSupport` version predicate. `retention_windows.go`
uses the `os/types_windows.go` directory/name-surrogate predicate. The Go
Authors' copyright notices are retained in the files; the complete BSD license
is [retained in the repository](../../third_party/go-sqlite3/provenance/GO-LICENSE.txt).

Original installed Go 1.26.4 source SHA256 values:

| Source | SHA256 |
| --- | --- |
| os/path_windows.go | 7256e7698c7aed7eba0c3e5e3dd441c7886dfbe21864734f3f8bd3a549cbff58 |
| os/types_windows.go | 32bdbb028336c434525559642424295c71c0dd6e4fef7cec1fd0739ec80feffb |
| runtime/os_windows.go | 1dc3474dd74a8cd30f6a439d59f52e18a5a2669f4a2060be63443aff9d7f9777 |
| syscall/syscall_windows.go | f1b22dde6bb980cf036561cee7ece968daa260cb622f32c96cebd52477b7a346 |

Native cwd and full-path queries admit their reported sizes before allocating;
growth on the second call fails rather than retrying. Executable acquisition
uses one fixed buffer, then admits conversion and later launch backing.
Windows retention owns one find handle and one fixed record. A failed close
retains that owner and ends the whole helper generation after its failure ACK.

Modern Windows preserves supplied path spellings. Older Windows uses the
source-matched bounded normalization at OS-facing calls; logical options and
derived log names stay unchanged. The remaining legacy long-device-path
normalization proof caveat is recorded in the
[ownership validation record](../../docs/pc92-v15-ownership-validation.md).
These source adaptations and focused tests are not full platform qualification.
