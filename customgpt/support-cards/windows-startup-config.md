# Support Card: Windows Startup And Config Failure

## Match

Use when a node operator reports startup failure on Windows, especially after
mentioning local/manual run, PowerShell, console output, `DXC_CONFIG_PATH`,
missing YAML, H3 tables, or gridstore startup messages.

## First Safe Check

Capture the complete console startup output from the project or release
directory, then inspect the active config directory.

```powershell
$env:DXC_CONFIG_PATH
.\gocluster.exe 2>&1 | Tee-Object .\startup.txt
```

For a source checkout, use the documented source run command from the project
directory and apply the same `Tee-Object` capture.

The source launcher is `pwsh -NoProfile -File ./launch-cluster.ps1`. It builds
and launches one fresh Windows amd64 cluster/peerdiag pair in an isolated
directory. Existing CPU profiles select PGO; no profiles select an ordinary
build. Build or profile-merge errors stop startup and never select an old root
executable. Capture the launcher's output to retain its exact executable path.
Windows Application Control can deny an executable before application startup;
capture that OS error separately from YAML/config diagnostics. See
`scripts/README.md` and `docs/ENVIRONMENT.md` for source-build requirements.

## Must Include

- Use exact diagnostic phrases when the user asks what to search for:
  `required config file`, `required YAML setting`, `Config diagnostics`, and
  `Config warning`.
- `DXC_CONFIG_PATH` points to a complete config directory, not a single YAML file.
- The root executable's `--help` and `--version` return before config loading or
  startup. Successful output confirms that invocation ran; it does not establish
  node/config health. Unknown arguments exit 2, except when an existing version
  token takes precedence. Run without arguments to test normal node startup.
- Search the captured startup block for `Config warning`, `Config diagnostics`,
  `required config file`, `required YAML setting`, H3 validation, and gridstore
  open/recovery messages.
- If the configured system log has not opened yet, early startup diagnostics
  may be visible only through console/stderr capture.
- An impossible enabled CustomSCP receiver/cell gate reports its requirement
  key and `call_correction.custom_scp.max_spotters_per_key`. Inspect both in
  `pipeline.yaml`; choose satisfiable minima or raise the existing cap explicitly.
  Startup does not silently lower the requirement. Disabled unused gates and
  separate FT storage are outside this capacity check.
- An invalid configured correction asset reports
  `call_correction.spotter_reliability_file` (or its CW/RTTY variant) or
  `call_correction.confusion_model_file` and the path. Check the named file;
  startup no longer continues with unit weights or a disabled model after a
  load error. Nonfinite weights, negative counts, invalid edges and overflowing
  totals fail. An enabled model still loads at weight zero. Disabling the model
  is a separate explicit setting, not an automatic recovery action.

## Must Avoid

- Do not give `systemctl` or `journalctl` as the first answer for a Windows
  question.
- Do not list only reference YAML files when the question is asking how to
  identify the exact missing startup file or setting.
- Do not treat extra-key warnings as fatal unless the docs identify the key as a
  removed migration key.

## Sources

- `customgpt/troubleshooting-index.md`
- `docs/OPERATOR_GUIDE.md`
- `data/config/README.md`
- `config/config_files.go`
- `main.go`
- `internal/cluster/bootstrap.go`
