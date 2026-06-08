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

## Must Include

- Use exact diagnostic phrases when the user asks what to search for:
  `required config file`, `required YAML setting`, `Config diagnostics`, and
  `Config warning`.
- `DXC_CONFIG_PATH` points to a complete config directory, not a single YAML file.
- Search the captured startup block for `Config warning`, `Config diagnostics`,
  `required config file`, `required YAML setting`, H3 validation, and gridstore
  open/recovery messages.
- If the configured system log has not opened yet, early startup diagnostics
  may be visible only through console/stderr capture.

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
- `internal/cluster/bootstrap.go`
