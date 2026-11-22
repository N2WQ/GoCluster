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

**Release 261121r74c2 requires four extra files before a fresh start.**
Copy these files from the [matching tagged source](https://github.com/N2WQ/GoCluster/tree/261121r74c2/data/rbn/priors)
into `ready_to_run/data/rbn/priors/`.

- `spotter_reliability.txt`
- `spotter_reliability_cw.txt`
- `spotter_reliability_rtty.txt`
- `confusion_model.json`

The supplied pipeline config refers to those files. For documentation links
whose targets are outside the ZIP, open the matching tagged source. The
release tag is recorded in `data/config-bundle.json`.

For a real node, copy `ready_to_run/data/config` to a private complete config
directory such as `ready_to_run/data/config.local`, edit that private copy, and
run with `DXC_CONFIG_PATH` pointing at the private directory.

The source repository does not commit generated binaries or zip files. The
release script attaches build artifacts to GitHub Releases so the Git history
stays source-only.
