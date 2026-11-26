# Download GoCluster

Windows amd64 packages are published on GitHub Releases. Start with the
latest release.

https://github.com/N2WQ/GoCluster/releases/latest

Download this release asset:

```text
gocluster-windows-amd64.zip
```

Do not use GitHub's automatic `Source code (zip)` or `Source code (tar.gz)`
downloads unless you want the developer source tree. They are not ready-to-run
packages.

Extract the zip and open:

```text
ready_to_run/
```

The extracted directory contains:

```text
ready_to_run/
  gocluster.exe
  README.md
  data/
  docs/
```

Start with `ready_to_run/README.md`. It uses the packaged config to show the
Telnet port.

Meta bands let you select several bands by name in filters and history searches.
For example, `WARC` selects 30m, 17m, and 12m. After connecting, type
`HELP BANDS` or see the [meta bands list and examples](../README.md#meta-bands).

## Recover an older Windows ZIP

The following 48 published Windows packages omit the four model files named in their public configuration. Newer affected builds stop at the first missing file. Some earlier builds continue with reduced callsign correction. The missing FCC allowlist also disables its configured exceptions. Identify the tag on the release page for your ZIP. The November 22 build showing commit `0b716ef69dbc` is **261122r949b**.

**Releases with all five files in tagged source** — [261122r949b](https://github.com/N2WQ/GoCluster/tree/261122r949b/data/rbn/priors/), [261121r74c2](https://github.com/N2WQ/GoCluster/tree/261121r74c2/data/rbn/priors/), [261121r4b02](https://github.com/N2WQ/GoCluster/tree/261121r4b02/data/rbn/priors/).

1. Open the link for your exact tag. Open each of `spotter_reliability.txt`, `spotter_reliability_cw.txt`, `spotter_reliability_rtty.txt`, and `confusion_model.json`. Click GitHub’s **Download raw file** button on each file page. Create `data/rbn/priors` beside `gocluster.exe` and save the four files there with their original names.
2. In the same tag, open `data/fcc/allowlist.txt`, click **Download raw file**, and save it to `data/fcc/allowlist.txt` beside the executable. Preserve any allowlist you already maintain for your node.
3. These are Git LFS objects. A file beginning with `version https://git-lfs.github.com/spec/v1` is a pointer. If you see that first line, download the file again using the button. Restart with your usual private config. Check the system log for nonzero counts for the general, CW, and RTTY reliability tables and the loaded confusion model.

**Releases with the four models in tagged source** — [261120r31ab](https://github.com/N2WQ/GoCluster/tree/261120r31ab/data/rbn/priors/), [261120r30c1](https://github.com/N2WQ/GoCluster/tree/261120r30c1/data/rbn/priors/), [261120r1bc6](https://github.com/N2WQ/GoCluster/tree/261120r1bc6/data/rbn/priors/), [261119r566f](https://github.com/N2WQ/GoCluster/tree/261119r566f/data/rbn/priors/). Follow steps 1 and 3. These tags have no committed FCC allowlist. Preserve your operator-maintained allowlist or upgrade to a package that includes the public file.

**Releases whose tagged source also lacks the models** — use your own known-good data from that deployment, or upgrade the binary and public data together when a corrected package is available. Extract it to a fresh directory and follow its upgrade instructions for your private settings. A matching-tag raw download is unavailable for these releases.

- `261016rd63d`, `261016r429e`, `261016r24a6`, `261008r4c40`, `261008r22ba`
- `261007rfc96`, `261007r871c`, `261007r1`, `261006r5`, `261006r3`
- `261006r1`, `261005r2`, `261005r1`, `v26.11.07-820bc5423322`, `v26.19.06-ad9a8a22a6a3`
- `v26.19.06-8c1bc9ca4898`, `v26.17.06-279dcbfe68c0`, `v26.16.06-fe767c94a2ed`, `v26.16.06-6584b4b259b6`, `v26.16.06-0a64243222a8`
- `v26.08.06-c0db403cc41d`, `v26.07.06-e7876e490e4f`, `v26.07.06-a68a7be4855c`, `v26.06.06-afdbb5a10abd`, `v26.06.06-86cfec35cf2d`
- `v26.05.06-77b734664e3b`, `v26.05.06-3ce9e33061ce`, `v26.05.06-064d3dc8fafa`, `v26.04.06-3da210790855`, `v26.25.05-495d64e52470`
- `v26.23.05-5d1b4840245c`, `v26.06.05-ee0ae127e735`, `v26.05.05-3964f88f140f`, `v26.04.05-6cc182fc0677`, `v26.01.05-a417caf6762b`
- `v26.26.04-8a054141d45b`, `v26.26.04-734062af91de`, `v26.26.04-7320c7546789`, `v26.26.04-2665d897747f`, `v26.25.04-ee1cb7c06402`
- `v26.25.04-4dfc98f701f2`

For a real node, copy `ready_to_run/data/config` to a private complete config
directory such as `ready_to_run/data/config.local`, edit that private copy, and
run with `DXC_CONFIG_PATH` pointing at the private directory.

The source repository does not commit generated binaries or zip files. The
release script attaches build artifacts to GitHub Releases so the Git history
stays source-only.
