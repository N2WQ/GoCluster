# Support Card: Linux Service Startup Failure

## Match

Use when a node operator reports that the Linux `gocluster` service fails,
exits, restarts immediately, has no console UI, or behaves differently under
systemd than when run manually.

## First Safe Check

Collect service status, journal output, unit configuration, working directory,
and effective config path before diagnosing.

```bash
sudo systemctl status gocluster
journalctl -u gocluster -n 200 --no-pager
```

## Must Include

- `systemctl status` shows current service state and the failing command.
- `journalctl` shows startup stderr/stdout and early config diagnostics.
- Confirm `WorkingDirectory`, `ExecStart`, `DXC_CONFIG_PATH`, file ownership,
  and `ui.mode: headless`.

## Must Avoid

- Do not use Windows PowerShell commands for Linux service failure.
- Do not diagnose without service logs and unit context.

## Sources

- `customgpt/troubleshooting-index.md`
- `docs/OPERATOR_GUIDE.md`
- `customgpt/external-authorities.md`
