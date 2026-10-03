## DX Cluster — AI Code Assistant Instructions

This file gives concise, repository-specific guidance to help an AI coding agent be productive in the `dxcluster` Go project.

Key entry points
- **`main.go`**: program entry. Loads configuration with `config.Load("config.yaml")`, prints config, creates a telnet server via `telnet.NewServer(...)`, starts it with `Start()`, and handles graceful shutdown by listening for OS signals and calling `Stop()`.
- **`config/config.go`**: single YAML-backed configuration struct. Use `config.Load("config.yaml")` to parse YAML into `*config.Config` and read fields such as `Telnet.Port`, `Telnet.WelcomeMessage`, and `Telnet.MaxConnections`.
- **`telnet/server.go`**: core telnet server implementation. Important symbols: `telnet.NewServer`, `Server.Start`, `Server.Stop`, `Server.registerClient`, `Client.ReadLine`, `Client.Send` and `Client.SendRaw`.

Architecture & patterns
- Lightweight modular layout: root `main.go` wires together small packages under the module name `dxcluster` (see `go.mod`). Keep behavior in packages (`config`, `telnet`).
- Concurrency: `telnet.Server` accepts connections and spins goroutines per client. Client registry uses `clients map[string]*Client` protected by `clientsMutex` (RWMutex). Use the same mutex pattern when touching `clients`.
- Shutdown flow: `main` waits for OS signals, then calls `telnet.Server.Stop()` which closes the listener, closes the `shutdown` channel, and iterates clients to `Close()` their connections. Follow this pattern for other long-running components.

Telnet specifics (do not change lightly)
- `Client.ReadLine()` filters Telnet IAC sequences (bytes 255..251) before returning a logical line. When adding input parsing, keep this low-level filtering in place.
- `Client.Send()` replaces `\n` with `\r\n` for proper Telnet line endings. Preserve this behavior for text responses.
- Option negotiation is sent at login using raw bytes (IAC sequences). If modifying terminal negotiation, test with a real telnet client.

Config conventions
- The repository expects a `config.yaml` at runtime root. Example keys used by code:
  - `server.name`, `server.node_id`
  - `telnet.port`, `telnet.tls_enabled`, `telnet.max_connections`, `telnet.welcome_message`
  - `admin.http_port`, `admin.bind_address` (config present but no admin server implemented yet)

Build / run / debug
- Create release packages with the repository script so binary identity is
  stamped as UTC `YYMMDD`, with source identity in separate metadata:
  - Windows/PowerShell example:
    ```powershell
    .\scripts\create-release.ps1 -PackageOnly -AllowDirty
    ```
  - Real publishing uses a clean committed tree and GitHub CLI authentication:
    ```powershell
    .\scripts\create-release.ps1
    ```
- Run directly for development:
  - `go run .` (the default `config.yaml` is loaded from the current working directory)
- Connect with a telnet client to verify runtime behavior:
  - `telnet localhost 7300` (port comes from `config.yaml` by default)

Dependencies & notable externals
- YAML parsing: `gopkg.in/yaml.v3` (see `config/config.go`). Keep YAML struct tags in sync with `config.yaml`.
- No external network services are used by the code in this repo; network surface is the telnet TCP listener.

Project-specific conventions
- Module path and package imports reference the module name (`dxcluster`). When moving files/packages, ensure import paths remain correct.
- Prefer minimal, focused packages. Business logic should live in packages, not `main`.
- When adding features that accept network clients, follow the `Server` pattern: `NewX`, `Start()`, `Stop()`, and use explicit shutdown channels.

Testing and missing coverage
- There are no tests in the repository currently. If adding tests, focus on:
  - unit tests for `config.Load()` with sample YAML
  - integration-style tests for `telnet.Server` using in-memory network listeners (or real TCP on ephemeral ports)

What to watch for in PRs
- Preserve telnet low-level behavior (IAC handling, CRLF conversions).
- Keep concurrency safe: when manipulating `Server.clients`, use the mutex.
- If adding an admin HTTP server (config includes `admin`), wire graceful shutdown to the same signal handling flow used in `main.go`.

If anything is unclear or you'd like additional sections (examples of small refactors, unit-test scaffolding, or CI steps), tell me which area to expand.
