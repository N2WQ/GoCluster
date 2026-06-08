import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const DEFAULT_TOKEN = "local-support-agent-eval-token";
const REPO_OWNER = "N2WQ";
const REPO_NAME = "GoCluster";
const BRANCH = "main";
const RAW_PREFIX = `https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}/`;
const API_PREFIX = `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}`;

const args = parseArgs(process.argv.slice(2));
const repoRoot = path.resolve(requiredArg(args, "repo-root"));
const casesPath = path.resolve(requiredArg(args, "cases"));
const outputDir = path.resolve(requiredArg(args, "output"));
const requestedCases = new Set(valuesArg(args, "case").map((value) => value.toUpperCase()));
const answersPath = firstArg(args, "answers");
const requireAnswers = flagArg(args, "require-answers");
const liveModel = flagArg(args, "live-model");
const model = firstArg(args, "model") || process.env.OPENAI_MODEL || "gpt-4.1-mini";

await fs.mkdir(outputDir, { recursive: true });

const originalFetch = globalThis.fetch.bind(globalThis);
globalThis.fetch = localFirstFetch;

const workerPath = path.join(repoRoot, "customgpt/support-agent/cloudflare-worker.js");
const workerModule = await import(pathToFileURL(workerPath).href);
const worker = workerModule.default;
const catalog = JSON.parse(await fs.readFile(casesPath, "utf8"));
const answers = answersPath ? await loadAnswers(path.resolve(answersPath)) : new Map();
const selectedCases = (catalog.cases || []).filter((testCase) => {
  return requestedCases.size === 0 || requestedCases.has(String(testCase.id || "").toUpperCase());
});

if (selectedCases.length === 0) {
  throw new Error("No support-agent eval cases matched the requested case IDs.");
}

const startedAt = new Date().toISOString();
const results = [];

for (const testCase of selectedCases) {
  const result = await runCase(testCase);
  results.push(result);
  const marker = result.status === "pass" || result.status === "retrieval_only" ? "PASS" : "FAIL";
  console.log(`${marker}: ${testCase.id} ${testCase.title} (${result.status})`);
}

const summary = summarize(results);
const report = {
  schema_version: 1,
  generated_at: new Date().toISOString(),
  started_at: startedAt,
  repo_root: repoRoot,
  mode: liveModel ? "local-worker-live-model" : "local-worker-transcript",
  require_answers: requireAnswers,
  model: liveModel ? model : null,
  summary,
  results
};

await fs.writeFile(path.join(outputDir, "report.json"), `${JSON.stringify(report, null, 2)}\n`, "utf8");
await fs.writeFile(path.join(outputDir, "report.md"), markdownReport(report), "utf8");

if (summary.failed > 0) {
  process.exitCode = 1;
}

async function runCase(testCase) {
  const actionResults = [];
  const fetched = new Map();
  const checks = [];

  for (const step of testCase.action_plan || []) {
    const response = await callWorker(step.endpoint);
    const expectedStatus = Number.isFinite(step.expect_status) ? step.expect_status : 200;
    const body = response.body;

    actionResults.push({
      name: step.name || "",
      endpoint: step.endpoint,
      status: response.status,
      expected_status: expectedStatus,
      path: body && typeof body.path === "string" ? body.path : null,
      error: body && typeof body.error === "string" ? body.error : null
    });

    checks.push(check(
      `action ${step.name || step.endpoint} returned ${expectedStatus}`,
      response.status === expectedStatus,
      `got ${response.status}`
    ));

    captureFetched(body, fetched);
  }

  for (const requiredPath of testCase.required_sources || []) {
    checks.push(check(
      `retrieved required source ${requiredPath}`,
      fetched.has(requiredPath),
      "source was not fetched by action plan"
    ));
  }

  for (const forbiddenPath of testCase.forbidden_sources || []) {
    checks.push(check(
      `did not retrieve forbidden source ${forbiddenPath}`,
      !fetched.has(forbiddenPath),
      "forbidden source was fetched"
    ));
  }

  for (const sourceCheck of testCase.required_source_substrings || []) {
    const content = fetched.get(sourceCheck.path) || "";
    for (const needle of sourceCheck.contains || []) {
      checks.push(check(
        `source ${sourceCheck.path} contains ${needle}`,
        contentIncludes(content, needle),
        "required source substring missing"
      ));
    }
  }

  const answerRecord = await answerForCase(testCase, fetched);
  const answerChecks = scoreAnswer(testCase, answerRecord.answer);
  checks.push(...answerChecks.checks);

  const retrievalFailed = checks.some((item) => item.scope === "retrieval" && !item.pass);
  const answerFailed = checks.some((item) => item.scope === "answer" && !item.pass);
  const answerSkipped = answerRecord.status === "skipped";
  let status = "pass";

  if (retrievalFailed || answerFailed) {
    status = "fail";
  } else if (answerSkipped) {
    status = "retrieval_only";
  }

  return {
    id: testCase.id,
    category: testCase.category,
    severity: testCase.severity,
    title: testCase.title,
    status,
    prompt_sequence: testCase.prompt_sequence || [],
    actions: actionResults,
    fetched_sources: [...fetched.keys()].sort(),
    answer: answerRecord.metadata,
    answer_text: answerRecord.answer,
    checks
  };
}

async function callWorker(endpoint) {
  const request = new Request(`https://support-agent.local${endpoint}`, {
    headers: {
      Authorization: `Bearer ${DEFAULT_TOKEN}`
    }
  });
  const response = await worker.fetch(request, { GOCLUSTER_DOCS_ACTION_TOKEN: DEFAULT_TOKEN }, {});
  const text = await response.text();
  let body = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text };
    }
  }
  return {
    status: response.status,
    body
  };
}

function captureFetched(body, fetched) {
  if (!body || typeof body !== "object") {
    return;
  }
  if (body.support_route && typeof body.support_route === "object") {
    const routePath = `support-route:${body.support_route.id || "unknown"}`;
    const routeContent = JSON.stringify(body.support_route, null, 2);
    const existing = fetched.get(routePath) || "";
    if (routeContent.length >= existing.length) {
      fetched.set(routePath, routeContent);
    }
  }
  if (typeof body.path === "string" && typeof body.content === "string") {
    const existing = fetched.get(body.path) || "";
    if (body.content.length >= existing.length) {
      fetched.set(body.path, body.content);
    }
  }
  if (Array.isArray(body.files)) {
    for (const file of body.files) {
      captureFetched(file, fetched);
    }
  }
}

async function answerForCase(testCase, fetched) {
  const id = String(testCase.id || "");
  if (answers.has(id)) {
    return {
      status: "supplied",
      answer: answers.get(id),
      metadata: {
        status: "supplied",
        source: answersPath
      }
    };
  }

  if (liveModel) {
    let generated = "";
    let error = null;
    try {
      generated = await generateAnswer(testCase, fetched);
    } catch (err) {
      error = err;
    }
    return {
      status: error ? "model_error" : generated ? "live_model" : "skipped",
      answer: generated || "",
      metadata: {
        status: error ? "model_error" : generated ? "live_model" : "skipped",
        model: generated || error ? model : null,
        reason: error ? safeErrorMessage(error) : generated ? null : "model returned no output text"
      }
    };
  }

  return {
    status: "skipped",
    answer: "",
    metadata: {
      status: "skipped",
      reason: "No answer transcript supplied and LiveModel was not enabled"
    }
  };
}

function scoreAnswer(testCase, answerText) {
  const checks = [];
  const answer = String(answerText || "");
  const answerConfig = testCase.answer_checks || {};

  if (!answer) {
    checks.push(check(
      "answer text available",
      !requireAnswers,
      requireAnswers ? "answer text is required" : "answer scoring skipped",
      "answer"
    ));
    return {
      checks
    };
  }

  checks.push(check("answer text available", true, "", "answer"));

  for (const requirement of answerConfig.required || []) {
    const allOf = requirement.all_of || [];
    const anyOf = requirement.any_of || [];
    let pass = true;

    if (allOf.length > 0) {
      pass = pass && allOf.every((needle) => contentIncludes(answer, needle));
    }
    if (anyOf.length > 0) {
      pass = pass && anyOf.some((needle) => contentIncludes(answer, needle));
    }

    checks.push(check(
      `answer includes ${requirement.name || "required concept"}`,
      pass,
      "required answer concept missing",
      "answer"
    ));
  }

  for (const forbidden of answerConfig.forbidden || []) {
    checks.push(check(
      `answer avoids ${forbidden.name || forbidden.text}`,
      !contentIncludes(answer, forbidden.text || ""),
      "forbidden answer text present",
      "answer"
    ));
  }

  if (answerConfig.require_source_citation) {
    const sourceLine = answer.match(/(^|\n)\s*Source:\s*(\S[^\n]*)/i);
    const hasSourceLine = Boolean(sourceLine);
    checks.push(check(
      "answer includes Source citation",
      hasSourceLine,
      "missing Source: citation",
      "answer"
    ));
    if (hasSourceLine) {
      const cited = sourceLine[2] || "";
      const citationCandidates = answerConfig.allowed_source_citations || testCase.required_sources || [];
      checks.push(check(
        "answer cites an expected retrieved source",
        citationCandidates.some((candidate) => contentIncludes(cited, candidate)),
        `unexpected Source citation: ${cited}`,
        "answer"
      ));
    }
  }

  return {
    checks
  };
}

function check(name, pass, detail = "", scope = "retrieval") {
  return {
    scope,
    name,
    pass: Boolean(pass),
    detail: pass ? "" : detail
  };
}

async function generateAnswer(testCase, fetched) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return "";
  }

  const instructionsPath = path.join(repoRoot, "customgpt/support-agent/agent-instructions.txt");
  const instructions = await fs.readFile(instructionsPath, "utf8");
  const evidence = [...fetched.entries()]
    .map(([sourcePath, content]) => {
      const trimmed = content.length > 12000 ? `${content.slice(0, 12000)}\n[TRUNCATED FOR LOCAL EVAL]` : content;
      return `Source: ${sourcePath}\n${trimmed}`;
    })
    .join("\n\n---\n\n");

  const prompt = [
    "Simulate the final support-agent answer for this conversation using only the retrieved evidence below.",
    "The GoCluster Documentation Action has already retrieved this evidence. Do not say the documentation was not retrieved when evidence is present.",
    "Use retrieved Source lines as repository paths. Do not use `Source:` as a section label, card title, or explanation label.",
    "End with `Source: <one retrieved repo path>` and cite the primary path that directly supports the answer.",
    "When a support card is present, every item in `Must Include` is a mandatory answer obligation. Include exact diagnostic/config/log terms from that section when they are relevant to the user's question.",
    "Before finalizing, check the answer against the support card's Must Avoid section.",
    "Conversation:",
    ...(testCase.prompt_sequence || []).map((line) => `User: ${line}`),
    "",
    "Retrieved evidence:",
    evidence
  ].join("\n");

  const requestBody = {
    model,
    input: [
      {
        role: "system",
        content: instructions
      },
      {
        role: "user",
        content: prompt
      }
    ],
    max_output_tokens: model.startsWith("gpt-5") ? 2400 : 900
  };

  if (model.startsWith("gpt-5")) {
    requestBody.reasoning = { effort: "minimal" };
    requestBody.text = { verbosity: "low" };
  }

  const payload = await callOpenAIWithRetry(requestBody, apiKey);

  if (typeof payload.output_text === "string") {
    return payload.output_text;
  }

  const chunks = [];
  for (const item of payload.output || []) {
    for (const content of item.content || []) {
      if (typeof content.text === "string") {
        chunks.push(content.text);
      }
    }
  }
  const output = chunks.join("\n").trim();
  if (!output && payload.status === "incomplete") {
    throw new Error(`OpenAI response incomplete: ${JSON.stringify(payload.incomplete_details || {})}`);
  }
  return output;
}

async function callOpenAIWithRetry(body, apiKey) {
  const maxAttempts = 3;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    let response = null;
    let text = "";

    try {
      response = await originalFetch("https://api.openai.com/v1/responses", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
      });
      text = await response.text();
    } catch (err) {
      lastError = new Error(`OpenAI request failed: ${safeErrorMessage(err)}`);
      if (attempt < maxAttempts) {
        await sleep(500 * attempt);
        continue;
      }
      throw lastError;
    }

    let payload = null;
    try {
      payload = text ? JSON.parse(text) : {};
    } catch {
      lastError = new Error(`OpenAI API returned non-JSON response ${response.status}: ${truncate(text, 240)}`);
      if (attempt < maxAttempts && shouldRetryOpenAI(response.status, text)) {
        await sleep(500 * attempt);
        continue;
      }
      throw lastError;
    }

    if (!response.ok) {
      lastError = new Error(`OpenAI API returned ${response.status}: ${truncate(JSON.stringify(payload), 500)}`);
      if (attempt < maxAttempts && shouldRetryOpenAI(response.status, text)) {
        await sleep(500 * attempt);
        continue;
      }
      throw lastError;
    }

    return payload;
  }

  throw lastError || new Error("OpenAI request failed");
}

function shouldRetryOpenAI(status, text) {
  return status === 408 ||
    status === 409 ||
    status === 429 ||
    status >= 500 ||
    /timeout|temporar|overload|rate/i.test(String(text || ""));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeErrorMessage(err) {
  return truncate(err && err.message ? err.message : String(err || ""), 500);
}

function truncate(value, limit) {
  const text = String(value || "");
  return text.length > limit ? `${text.slice(0, limit)}...` : text;
}

async function loadAnswers(resolvedPath) {
  const stat = await fs.stat(resolvedPath);
  const map = new Map();

  if (stat.isDirectory()) {
    const entries = await fs.readdir(resolvedPath, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isFile()) {
        continue;
      }
      if (!/\.(md|txt)$/i.test(entry.name)) {
        continue;
      }
      const id = path.basename(entry.name, path.extname(entry.name));
      map.set(id, await fs.readFile(path.join(resolvedPath, entry.name), "utf8"));
    }
    return map;
  }

  const payload = JSON.parse(await fs.readFile(resolvedPath, "utf8"));
  if (Array.isArray(payload.answers)) {
    for (const answer of payload.answers) {
      if (answer && answer.case_id && typeof answer.answer === "string") {
        map.set(String(answer.case_id), answer.answer);
      }
    }
    return map;
  }

  for (const [id, value] of Object.entries(payload)) {
    if (typeof value === "string") {
      map.set(id, value);
    } else if (value && typeof value.answer === "string") {
      map.set(id, value.answer);
    }
  }
  return map;
}

function summarize(caseResults) {
  const summary = {
    total: caseResults.length,
    pass: 0,
    retrieval_only: 0,
    failed: 0,
    answer_skipped: 0
  };

  for (const result of caseResults) {
    if (result.status === "pass") {
      summary.pass++;
    } else if (result.status === "retrieval_only") {
      summary.retrieval_only++;
    } else {
      summary.failed++;
    }
    if (result.answer && result.answer.status === "skipped") {
      summary.answer_skipped++;
    }
  }

  return summary;
}

function markdownReport(report) {
  const lines = [
    "# Support-Agent Eval Report",
    "",
    `Generated: ${report.generated_at}`,
    `Mode: ${report.mode}`,
    `Cases: ${report.summary.total}`,
    `Pass: ${report.summary.pass}`,
    `Retrieval only: ${report.summary.retrieval_only}`,
    `Failed: ${report.summary.failed}`,
    `Answer skipped: ${report.summary.answer_skipped}`,
    "",
    "| ID | Category | Severity | Status | Failed checks |",
    "| --- | --- | --- | --- | --- |"
  ];

  for (const result of report.results) {
    const failed = result.checks
      .filter((item) => !item.pass)
      .map((item) => item.name)
      .join("<br>");
    lines.push(`| ${escapeCell(result.id)} | ${escapeCell(result.category)} | ${escapeCell(result.severity)} | ${escapeCell(result.status)} | ${escapeCell(failed || "none")} |`);
  }

  lines.push("", "## Failed Checks", "");
  for (const result of report.results) {
    const failed = result.checks.filter((item) => !item.pass);
    if (failed.length === 0) {
      continue;
    }
    lines.push(`### ${result.id} ${result.title}`, "");
    for (const item of failed) {
      lines.push(`- ${item.scope}: ${item.name} - ${item.detail}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function escapeCell(value) {
  return String(value || "").replace(/\|/g, "\\|").replace(/\r?\n/g, "<br>");
}

async function localFirstFetch(input, init) {
  const url = typeof input === "string" ? input : input.url;

  if (url.startsWith(RAW_PREFIX)) {
    return localRawResponse(url.slice(RAW_PREFIX.length));
  }

  if (url.startsWith(`${API_PREFIX}/contents`)) {
    return localContentsResponse(url);
  }

  if (url.startsWith(`${API_PREFIX}/git/trees/`)) {
    return localTreeResponse(url);
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
        url: `${API_PREFIX}/contents/${encodeURIComponent(toRepoPath(resolved))}`
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
        url: `${API_PREFIX}/contents/${repoChild.split("/").map(encodeURIComponent).join("/")}`
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
  return jsonFetchResponse({
    tree
  });
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

function contentIncludes(content, needle) {
  return String(content || "").toLowerCase().includes(String(needle || "").toLowerCase());
}

function parseArgs(argv) {
  const parsed = new Map();
  for (let i = 0; i < argv.length; i++) {
    const item = argv[i];
    if (!item.startsWith("--")) {
      continue;
    }
    const key = item.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      pushArg(parsed, key, "true");
      continue;
    }
    pushArg(parsed, key, next);
    i++;
  }
  return parsed;
}

function pushArg(parsed, key, value) {
  if (!parsed.has(key)) {
    parsed.set(key, []);
  }
  parsed.get(key).push(value);
}

function requiredArg(parsed, key) {
  const value = firstArg(parsed, key);
  if (!value) {
    throw new Error(`Missing required --${key}`);
  }
  return value;
}

function firstArg(parsed, key) {
  const values = parsed.get(key) || [];
  return values.length > 0 ? values[values.length - 1] : "";
}

function valuesArg(parsed, key) {
  return parsed.get(key) || [];
}

function flagArg(parsed, key) {
  return parsed.has(key);
}
