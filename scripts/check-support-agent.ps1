<#
.SYNOPSIS
  Checks the GoCluster custom GPT support-agent deployment bundle.

.DESCRIPTION
  Validates that support-agent instructions, OpenAPI schema, Worker code,
  quality docs, and routing docs are internally aligned. It also runs a local
  Worker smoke test through Node.js and can optionally check the deployed
  Cloudflare Worker endpoints.

.PARAMETER Deployed
  Also check the deployed Cloudflare Worker URL. Authenticated deployed checks
  run only when the token environment variable is present.

.PARAMETER BaseUrl
  Deployed Worker base URL to check when -Deployed is used.

.PARAMETER TokenEnv
  Environment variable name containing the deployed Worker bearer token.

.NOTES
  Prerequisites: PowerShell, Node.js 18 or newer, network access to GitHub for
  local Worker repository fetches, and optional deployed Worker network access.
  Side effects: creates and removes one temporary Node.js smoke-test file.
  Safety: never prints bearer tokens or private config contents.
#>
[CmdletBinding()]
param(
  [switch]$Deployed,
  [string]$BaseUrl = "https://gocluster-docs-action.n2wq-api.workers.dev",
  [string]$TokenEnv = "GOCLUSTER_DOCS_ACTION_TOKEN"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")

function Write-Pass {
  param([string]$Message)
  Write-Host "PASS: $Message"
}

function Write-Skip {
  param([string]$Message)
  Write-Host "SKIP: $Message"
}

function Assert-True {
  param(
    [bool]$Condition,
    [string]$Message
  )

  if (-not $Condition) {
    throw "FAIL: $Message"
  }

  Write-Pass $Message
}

function Assert-FileExists {
  param([string]$Path)
  Assert-True (Test-Path -LiteralPath $Path -PathType Leaf) "$Path exists"
}

function Assert-ContainsLiteral {
  param(
    [string]$Path,
    [string]$Needle,
    [string]$Message
  )

  $content = Get-Content -Raw -LiteralPath $Path
  Assert-True ($content.IndexOf($Needle, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) $Message
}

function Invoke-SupportAgentWebRequest {
  param(
    [string]$Uri,
    [string]$Method = "GET",
    [hashtable]$Headers = @{}
  )

  try {
    $response = Invoke-WebRequest -Uri $Uri -Method $Method -Headers $Headers -ErrorAction Stop
    return [pscustomobject]@{
      StatusCode = [int]$response.StatusCode
      Content = [string]$response.Content
    }
  } catch {
    $response = $_.Exception.Response
    if ($null -eq $response) {
      throw
    }

    $content = ""
    if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
      $content = $_.ErrorDetails.Message
    } elseif ($response.PSObject.Methods.Name -contains "GetResponseStream") {
      $stream = $response.GetResponseStream()
      if ($null -ne $stream) {
        $reader = New-Object System.IO.StreamReader($stream)
        try {
          $content = $reader.ReadToEnd()
        } finally {
          $reader.Dispose()
        }
      }
    }

    return [pscustomobject]@{
      StatusCode = [int]$response.StatusCode
      Content = $content
    }
  }
}

Push-Location -LiteralPath $repoRoot
try {
  $instructionsPath = "customgpt/support-agent/agent-instructions.txt"
  $schemaPath = "customgpt/support-agent/actions-schema.yaml"
  $workerPath = "customgpt/support-agent/cloudflare-worker.js"

  $requiredFiles = @(
    $instructionsPath,
    $schemaPath,
    $workerPath,
    "customgpt/source-map.md",
    "customgpt/troubleshooting-index.md",
    "customgpt/developer-guide-index.md",
    "customgpt/common-questions.md",
    "customgpt/support-cards/windows-startup-config.md",
    "customgpt/support-cards/log-destination.md",
    "customgpt/support-cards/runtime-yaml-controls.md",
    "customgpt/support-cards/toxicity-filter.md",
    "customgpt/support-cards/path-reliability.md",
    "customgpt/support-cards/truncated-retrieval.md",
    "customgpt/support-cards/ambiguous-short-prompt.md",
    "customgpt/support-cards/telnet-connectivity.md",
    "customgpt/support-cards/linux-service-startup.md",
    "customgpt/support-cards/confidence-glyph.md",
    "customgpt/support-cards/dxsummit-startup-spots.md",
    "customgpt/support-cards/peer-bulletin-dedupe.md",
    "customgpt/support-cards/security-boundary.md",
    "docs/support-agent-coverage-ledger.md",
    "docs/support-agent-quality-contract.md",
    "docs/support-agent-eval-cases.json",
    "docs/support-agent-evals.md",
    "docs/support-agent-runbook.md",
    "scripts/evaluate-support-agent.ps1",
    "scripts/support-agent-eval-runner.mjs"
  )

  foreach ($path in $requiredFiles) {
    Assert-FileExists $path
  }

  $instructions = Get-Content -Raw -LiteralPath $instructionsPath
  Assert-True ($instructions.Length -le 8000) "agent instructions length is within GPT Builder budget ($($instructions.Length) <= 8000)"
  Assert-True ($instructions.Contains("Source:")) "agent instructions require source citations"
  Assert-True ($instructions.IndexOf("choose the narrowest route", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) "agent instructions require narrowest-route retrieval"

  foreach ($operationId in @("getVersion", "getSourceMap", "getTroubleshootingIndex", "getSupportRoute", "searchSupportCorpus", "getDoc", "getBundle", "listDir", "findFiles")) {
    Assert-ContainsLiteral $schemaPath "operationId: $operationId" "schema exposes $operationId"
  }
  Assert-ContainsLiteral $schemaPath "bearerAuth:" "schema requires bearer auth"
  Assert-ContainsLiteral $schemaPath "version: 4.7.0" "schema version is 4.7.0"
  Assert-ContainsLiteral $schemaPath "Choose the most specific symptom route" "schema documents route specificity"
  Assert-ContainsLiteral $schemaPath "SupportRouteResponse" "schema documents support-route response"
  Assert-ContainsLiteral $schemaPath "SearchResponse" "schema documents search response"

  Assert-ContainsLiteral "customgpt/source-map.md" "Support-agent answer quality" "source map routes support-agent answer quality"
  Assert-ContainsLiteral "docs/support-agent-coverage-ledger.md" "Persona-Domain Matrix" "coverage ledger defines persona-domain matrix"
  Assert-ContainsLiteral "customgpt/support-cards/windows-startup-config.md" "required YAML setting" "Windows startup card names exact YAML diagnostic"
  Assert-ContainsLiteral "customgpt/support-cards/ambiguous-short-prompt.md" "Do not infer" "ambiguous prompt card preserves uncertainty"
  Assert-ContainsLiteral "customgpt/support-cards/linux-service-startup.md" "journalctl" "Linux service card names journalctl"
  Assert-ContainsLiteral "customgpt/support-cards/dxsummit-startup-spots.md" "startup_backfill_seconds" "DXSummit card names startup backfill"
  Assert-ContainsLiteral "customgpt/troubleshooting-index.md" "Use the most specific matching symptom route" "troubleshooting index requires most-specific routing"
  Assert-ContainsLiteral "docs/support-agent-quality-contract.md" "Troubleshooting answers" "quality contract defines troubleshooting answer shape"
  Assert-ContainsLiteral "docs/support-agent-quality-contract.md" "scripts/evaluate-support-agent.ps1" "quality contract names local eval harness"
  Assert-ContainsLiteral "docs/support-agent-eval-cases.json" '"schema_version": 1' "eval case catalog has schema version"
  Assert-ContainsLiteral "docs/support-agent-eval-cases.json" '"category": "startup-config-diagnostics"' "eval case catalog covers startup config diagnostics"
  Assert-ContainsLiteral "docs/support-agent-eval-cases.json" '"category": "safety-adversarial"' "eval case catalog covers safety adversarial prompts"
  Assert-ContainsLiteral "docs/support-agent-evals.md" "SA-001" "eval prompts include SA-001 regression coverage"
  Assert-ContainsLiteral "docs/support-agent-evals.md" "cluster fails on startup" "eval prompts include startup transcript coverage"
  Assert-ContainsLiteral "docs/support-agent-runbook.md" "scripts/evaluate-support-agent.ps1" "runbook includes local eval harness"

  Assert-ContainsLiteral $workerPath "GOCLUSTER_DOCS_ACTION_TOKEN" "Worker uses the expected token binding"
  Assert-ContainsLiteral $workerPath 'request.method === "OPTIONS"' "Worker handles CORS preflight"
  Assert-ContainsLiteral $workerPath '"customgpt/support-agent/"' "Worker blocks deployment bundle retrieval"

  $nodeCommand = Get-Command node -ErrorAction SilentlyContinue
  Assert-True ($null -ne $nodeCommand) "node is available for local Worker smoke"

  $workerUrl = [System.Uri]::new((Resolve-Path -LiteralPath $workerPath).Path).AbsoluteUri
  $nodeScript = @'
import fs from "node:fs/promises";
import path from "node:path";
import worker from "__WORKER_URL__";

const repoRoot = __REPO_ROOT_JSON__;
const rawPrefix = "https://raw.githubusercontent.com/N2WQ/GoCluster/main/";
const apiPrefix = "https://api.github.com/repos/N2WQ/GoCluster";
const originalFetch = globalThis.fetch.bind(globalThis);
globalThis.fetch = localFirstFetch;

const token = "local-support-agent-smoke-token";
const env = { GOCLUSTER_DOCS_ACTION_TOKEN: token };
const authHeaders = { Authorization: `Bearer ${token}` };

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function call(path, options = {}) {
  const request = new Request(`https://support-agent.local${path}`, options);
  return await worker.fetch(request, env, {});
}

async function jsonCall(path, options = {}) {
  const response = await call(path, options);
  let body = null;
  const text = await response.text();
  if (text) {
    body = JSON.parse(text);
  }
  return { response, body };
}

async function check(name, fn) {
  await fn();
  console.log(`PASS: ${name}`);
}

await check("OPTIONS /version returns 204", async () => {
  const response = await call("/version", { method: "OPTIONS" });
  assert(response.status === 204, `expected 204, got ${response.status}`);
});

await check("unauthenticated /version returns 401", async () => {
  const { response, body } = await jsonCall("/version");
  assert(response.status === 401, `expected 401, got ${response.status}`);
  assert(body.error === "unauthorized", "expected unauthorized error");
});

await check("authenticated /version returns service status", async () => {
  const { response, body } = await jsonCall("/version", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(body.service === "gocluster-docs-action", "unexpected service");
  assert(body.auth === "bearer", "unexpected auth mode");
});

await check("/source-map returns structured routes", async () => {
  const { response, body } = await jsonCall("/source-map", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(body.path === "customgpt/source-map.md", "unexpected source-map path");
  assert(Array.isArray(body.routes) && body.routes.length > 0, "missing structured routes");
});

await check("/troubleshooting-index returns symptom routes", async () => {
  const { response, body } = await jsonCall("/troubleshooting-index", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(Array.isArray(body.symptom_routes) && body.symptom_routes.length > 0, "missing symptom routes");
  assert(body.content.includes("Windows local run fails"), "missing Windows route");
});

await check("/support-route returns support card and required files", async () => {
  const { response, body } = await jsonCall("/support-route?query=windows%20missing%20yaml%20startup", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(body.support_route.id === "windows-startup-config", `unexpected support route ${body.support_route.id}`);
  assert(Array.isArray(body.files) && body.files.some((file) => file.path === "customgpt/support-cards/windows-startup-config.md"), "missing support card file");
  assert(body.support_route.must_include.includes("required YAML setting"), "missing required YAML setting contract");
});

await check("/search returns bounded support snippets", async () => {
  const { response, body } = await jsonCall("/search?query=go_runtime.max_procs", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(body.result_count > 0, "expected search results");
  assert(body.files.some((file) => file.content.includes("go_runtime.max_procs")), "missing exact key in search snippet");
});

await check("/doc supports bounded line windows", async () => {
  const { response, body } = await jsonCall("/doc?path=README.md&start_line=1&line_count=5", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(body.path === "README.md", "unexpected doc path");
  assert(body.sliced === true, "expected sliced response");
  assert(body.line_count <= 5, "line window exceeded request");
});

await check("/list-dir blocks deployment bundle", async () => {
  const { response, body } = await jsonCall("/list-dir?path=customgpt/support-agent", { headers: authHeaders });
  assert(response.status === 403, `expected 403, got ${response.status}`);
  assert(body.error === "path_not_allowed", "expected path_not_allowed");
});

await check("/find-files discovers safe repo paths", async () => {
  const { response, body } = await jsonCall("/find-files?query=README&path=telnet", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(body.result_count > 0, "expected README match under telnet");
});

await check("/bundle returns all requested files", async () => {
  const { response, body } = await jsonCall("/bundle?path=README.md&path=telnet/README.md", { headers: authHeaders });
  assert(response.status === 200, `expected 200, got ${response.status}`);
  assert(body.file_count === 2, `expected 2 files, got ${body.file_count}`);
});

async function localFirstFetch(input, init) {
  const url = typeof input === "string" ? input : input.url;

  if (url.startsWith(rawPrefix)) {
    return localRawResponse(url.slice(rawPrefix.length));
  }

  if (url.startsWith(`${apiPrefix}/contents`)) {
    return localContentsResponse(url);
  }

  if (url.startsWith(`${apiPrefix}/git/trees/`)) {
    return localTreeResponse();
  }

  return originalFetch(input, init);
}

async function localRawResponse(encodedPath) {
  const repoPath = decodeURIComponent(encodedPath);
  const resolved = safeWorkspacePath(repoPath);
  if (!resolved) {
    return jsonFetchResponse({ message: "not found" }, 404);
  }

  try {
    const content = await fs.readFile(resolved, "utf8");
    return new Response(content, {
      status: 200,
      headers: {
        "content-type": "text/plain; charset=utf-8"
      }
    });
  } catch {
    return new Response("not found", { status: 404 });
  }
}

async function localContentsResponse(urlText) {
  const url = new URL(urlText);
  const marker = "/contents";
  const idx = url.pathname.indexOf(marker);
  const encodedPath = idx >= 0 ? url.pathname.slice(idx + marker.length).replace(/^\/+/, "") : "";
  const repoPath = encodedPath ? decodeURIComponent(encodedPath) : "";
  const resolved = safeWorkspacePath(repoPath);
  if (!resolved) {
    return jsonFetchResponse({ message: "not found" }, 404);
  }

  try {
    const stat = await fs.stat(resolved);
    if (!stat.isDirectory()) {
      return jsonFetchResponse({
        type: "file",
        path: toRepoPath(resolved),
        size: stat.size,
        url: `${apiPrefix}/contents/${encodeURIComponent(toRepoPath(resolved))}`
      });
    }

    const entries = await fs.readdir(resolved, { withFileTypes: true });
    const payload = [];
    for (const entry of entries) {
      if (entry.name.startsWith(".")) {
        continue;
      }
      const child = path.join(resolved, entry.name);
      const repoChild = toRepoPath(child);
      if (shouldSkipTreePath(repoChild)) {
        continue;
      }
      const childStat = await fs.stat(child);
      payload.push({
        type: entry.isDirectory() ? "dir" : "file",
        path: repoChild,
        size: entry.isDirectory() ? null : childStat.size,
        url: `${apiPrefix}/contents/${repoChild.split("/").map(encodeURIComponent).join("/")}`
      });
    }
    return jsonFetchResponse(payload);
  } catch {
    return jsonFetchResponse({ message: "not found" }, 404);
  }
}

async function localTreeResponse() {
  const tree = [];
  await walkWorkspace(repoRoot, tree);
  return jsonFetchResponse({ tree });
}

async function walkWorkspace(dir, tree) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.name.startsWith(".")) {
      continue;
    }
    const fullPath = path.join(dir, entry.name);
    const repoPath = toRepoPath(fullPath);
    if (shouldSkipTreePath(repoPath)) {
      continue;
    }
    if (entry.isDirectory()) {
      await walkWorkspace(fullPath, tree);
      continue;
    }
    const stat = await fs.stat(fullPath);
    tree.push({
      type: "blob",
      path: repoPath,
      size: stat.size
    });
  }
}

function safeWorkspacePath(repoPath) {
  const normalized = normalizeRepoPath(repoPath);
  if (normalized.includes("..")) {
    return "";
  }
  const resolved = path.resolve(repoRoot, normalized);
  if (resolved !== repoRoot && !resolved.startsWith(`${repoRoot}${path.sep}`)) {
    return "";
  }
  return resolved;
}

function toRepoPath(fullPath) {
  return path.relative(repoRoot, fullPath).replace(/\\/g, "/");
}

function normalizeRepoPath(value) {
  return String(value || "").replace(/\\/g, "/").replace(/^\/+/, "").replace(/\/+/g, "/");
}

function shouldSkipTreePath(repoPath) {
  const normalized = normalizeRepoPath(repoPath);
  const skipPrefixes = [
    ".git",
    ".tmp",
    "logs",
    "ready_to_run",
    "data/archive",
    "data/cty",
    "data/fcc",
    "data/grids",
    "data/ipinfo",
    "data/logs",
    "data/rbn",
    "data/reports",
    "data/reputation",
    "data/scp",
    "data/users",
    "tmp"
  ];
  return skipPrefixes.some((prefix) => normalized === prefix || normalized.startsWith(`${prefix}/`));
}

function jsonFetchResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8"
    }
  });
}
'@

  $nodeScript = $nodeScript.Replace("__WORKER_URL__", $workerUrl)
  $repoRootJson = (Resolve-Path -LiteralPath ".").Path | ConvertTo-Json -Compress
  $nodeScript = $nodeScript.Replace("__REPO_ROOT_JSON__", $repoRootJson)
  $tempScript = Join-Path ([System.IO.Path]::GetTempPath()) ("gocluster-support-agent-smoke-" + [System.Guid]::NewGuid().ToString("N") + ".mjs")
  try {
    Set-Content -LiteralPath $tempScript -Value $nodeScript -Encoding UTF8
    & $nodeCommand.Source $tempScript
    if ($LASTEXITCODE -ne 0) {
      throw "FAIL: local Worker smoke failed with exit code $LASTEXITCODE"
    }
  } finally {
    Remove-Item -LiteralPath $tempScript -ErrorAction SilentlyContinue
  }

  if ($Deployed) {
    $trimmedBaseUrl = $BaseUrl.TrimEnd("/")

    $privacy = Invoke-SupportAgentWebRequest -Uri "$trimmedBaseUrl/privacy"
    Assert-True ($privacy.StatusCode -eq 200) "deployed /privacy returns 200"

    $preflight = Invoke-SupportAgentWebRequest -Uri "$trimmedBaseUrl/version" -Method "OPTIONS"
    Assert-True ($preflight.StatusCode -eq 204) "deployed OPTIONS /version returns 204"

    $unauthenticated = Invoke-SupportAgentWebRequest -Uri "$trimmedBaseUrl/version"
    Assert-True ($unauthenticated.StatusCode -eq 401) "deployed unauthenticated /version returns 401"

    $token = [Environment]::GetEnvironmentVariable($TokenEnv)
    if ([string]::IsNullOrWhiteSpace($token)) {
      Write-Skip "deployed authenticated checks because $TokenEnv is not set"
    } else {
      $headers = @{ Authorization = "Bearer $token" }
      $version = Invoke-SupportAgentWebRequest -Uri "$trimmedBaseUrl/version" -Headers $headers
      Assert-True ($version.StatusCode -eq 200) "deployed authenticated /version returns 200"

      $sourceMap = Invoke-SupportAgentWebRequest -Uri "$trimmedBaseUrl/source-map" -Headers $headers
      Assert-True ($sourceMap.StatusCode -eq 200) "deployed authenticated /source-map returns 200"
      Assert-True ($sourceMap.Content.Contains("Support-agent answer quality")) "deployed /source-map includes support-agent quality route"
    }
  }
} finally {
  Pop-Location
}
