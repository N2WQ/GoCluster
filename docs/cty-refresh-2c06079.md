# CTY refresh retained from 2c06079

The owner explicitly selected **retain and document the refresh** during v12
planning. This data change is separate from PC18/PC92 protocol corrections.
V12 leaves the asset and its refresh policy unchanged.

## Provenance and byte identity

The current ignored `data/cty/cty.plist.status.json` records download URL
`https://www.country-files.com/cty/cty.plist`, server Last-Modified
`Tue, 15 Sep 2026 20:52:36 GMT`, and download/check time
`2026-10-02T00:10:55.0624334Z`. The recorded size is 13,291,531 bytes and its
SHA256 matches the current working file. This is local download-status evidence,
not a new network retrieval or proof of the remote server's present contents.

| Asset representation | SHA256 |
| --- | --- |
| Retained current working CRLF bytes | `ba9ffe6b669e144a383f70dbb01f8a48acb534f90ddf712e2ec0cd4ff59d1d8d` |
| Current Git-normalized LF bytes at 2c06079 | `dd8e1a8d980d5c1bd53742d5609ba7f835fe1e57784cfb364e7ab59f6ca1aae3` |
| Prior Git-normalized LF bytes | `167ad1b7f268357d78865e22eeb710661716cffc01030bb1827fa7e2fb79a7dd` |
| Prior bytes with CRLF representation | `e7aef1119855720f27ecce6d1aa6c39b5a7fb7c234c9645bda80fd4c857bb155` |

On 2026-10-01 local time, a read-only Python plist parse/hash comparison confirmed
that the current working bytes normalize exactly to the committed asset. It
compared `git show 2c06079^:data/cty/cty.plist` and the current file: 28,977 old
entries, 26,742 current entries, 3,472 removed keys, 1,237 added keys and 48 common
keys with changed values. These are semantic dictionary comparisons, not line
diff counts. They do not certify geographic, country or zone correctness.

The configured 00:10 UTC refresh and status time are consistent with a scheduled
download. They do not establish which process or actor performed the refresh.
No attribution is asserted. Evidence script/output are retained at
`D:\codex-gocluster-v12-20261001\cty-evidence.py` and
`D:\codex-gocluster-v12-20261001\logs\cty-evidence.json`.

## Qualification consequence

Prior v11 runtime/Q4 evidence using the old asset remains historical. Future
runs must use isolated frozen inputs and record the exact consumed asset hash,
including line-ending representation where byte hashes are used. A parser pass
or identical Git-normalized hash does not let a different consumed byte hash
silently replace a manifest entry. V12 does not claim that old and new data
produce equivalent runtime lookups or reuse old latency results as final-source
evidence.
