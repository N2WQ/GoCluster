// Public-endpoint fixtures for the bounded support search contract.
// Run with Node.js; all upstream responses are synthetic and no network is used.
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import worker from "../customgpt/support-agent/cloudflare-worker.js";

const originalFetch = globalThis.fetch;
const env = { GOCLUSTER_DOCS_ACTION_TOKEN: "search-fixture-token" };
let fixtures = new Map();
let requests = [];
const blankRegions = (count, text = "needle") => Array.from({ length: count }, () => `${text}\n\n\n\n\n\n`).join("");
globalThis.fetch = async (url) => {
  const path = decodeURIComponent(String(url).split("/main/")[1]);
  requests.push(path);
  const value = fixtures.has(path) ? fixtures.get(path) : "unrelated content";
  if (value === "THROW") throw new Error("synthetic transport failure");
  if (typeof value === "number") return new Response("failure", { status: value });
  return new Response(value);
};
async function search(query = "needle", scope) {
  requests = [];
  const url = new URL("https://fixture.local/search");
  url.searchParams.set("query", query);
  if (scope !== undefined) url.searchParams.set("path", scope);
  const response = await worker.fetch(new Request(url, { headers: { Authorization: "Bearer search-fixture-token" } }), env, {});
  return { status: response.status, body: await response.json() };
}
async function check(name, fn) {
  fixtures = new Map();
  await fn();
  console.log(`PASS: ${name}`);
}
try {
  for (const count of [0, 24, 25, 26]) {
    await check(`${count} distinct regions and exact overflow boundary`, async () => {
      fixtures.set("README.md", blankRegions(count));
      const { status, body } = await search("needle", "README.md");
      assert.equal(status, 200);
      assert.equal(body.result_count, Math.min(count, 25));
      assert.equal(body.results_truncated, count > 25);
      assert.equal(body.truncated, count > 25);
      assert.equal(body.coverage_complete, true);
      assert.equal(body.searched_count, 1);
      assert.equal(body.corpus_count, 46);
      assert.equal(body.eligible_file_count, 1);
      assert.equal(body.limits.max_search_results, 25);
    });
  }
  await check("late exact match outranks 26 early all-word regions", async () => {
    fixtures.set("customgpt/source-map.md", blankRegions(26, "alpha separated beta"));
    fixtures.set("scripts/README.md", "ALPHA BETA");
    const { body } = await search("alpha beta");
    assert.equal(requests.length, 46);
    assert.equal(body.matches[0].path, "scripts/README.md");
    assert.equal(body.matches[0].match_type, "exact");
    assert.equal(body.matches[1].match_type, "all_words");
    assert.equal(body.results_truncated, true);
  });
  await check("file diversity, exact-tier precedence and stable ties", async () => {
    fixtures.set("README.md", blankRegions(26, "alpha beta"));
    fixtures.set("scripts/README.md", "alpha beta");
    fixtures.set("customgpt/source-map.md", "alpha separated beta");
    const { body } = await search("alpha beta");
    assert.deepEqual(body.matches.slice(0, 3).map(m => [m.path, m.matched_line]),
      [["README.md", 1], ["scripts/README.md", 1], ["README.md", 7]]);
    assert(body.matches.every(m => m.match_type === "exact"));
    const repeated = (await search("alpha beta")).body;
    assert.deepEqual(body.matches, repeated.matches);
  });
  await check("overlap merging preserves strongest match and literal line ranges", async () => {
    fixtures.set("README.md", "alpha separated beta\nALPHA BETA\nx\nx\nalpha beta\nz\nz\nz");
    const { body } = await search("alpha beta", "README.md");
    assert.equal(body.result_count, 1);
    const match = body.matches[0];
    assert.deepEqual(match.matched_lines, [1, 2, 5]);
    assert.equal(match.matched_line, 2);
    assert.equal(match.match_type, "exact");
    assert.equal(match.line_start, 1);
    assert.equal(match.line_end, 7);
    assert.equal(match.snippet, "alpha separated beta\nALPHA BETA\nx\nx\nalpha beta\nz\nz");
    assert.equal(body.files[0].content, match.snippet);
    assert.equal(body.files[0].line_count, 7);
  });
  await check("directory scope restricts actual upstream requests", async () => {
    fixtures.set("data/config/data.yaml", "needle");
    const { body } = await search("needle", "data/config/");
    assert.equal(body.result_count, 1);
    assert.equal(body.matches[0].path, "data/config/data.yaml");
    assert(requests.every(p => p.startsWith("data/config/")));
    assert.equal(body.eligible_file_count, requests.length);
  });
  await check("invalid and non-corpus scopes fail before fetching", async () => {
    for (const scope of ["", "../docs", "/docs", "docs/../telnet", ".git", "customgpt/support-agent", "uls/ised_refresh.go", "missing"]) {
      const { status, body } = await search("needle", scope);
      assert.equal(status, 400, scope);
      assert.equal(body.error, "invalid_search_scope");
      assert.equal(requests.length, 0);
    }
  });
  await check("HTTP and thrown failures yield explicit partial evidence", async () => {
    fixtures.set("README.md", "needle");
    fixtures.set("scripts/README.md", 404);
    fixtures.set("docs/ENVIRONMENT.md", "THROW");
    const { status, body } = await search();
    assert.equal(status, 200);
    assert.equal(body.searched_count, 44);
    assert.equal(body.failed_paths.length, 2);
    assert.deepEqual(body.failed_paths.map(f => f.path).sort(), ["docs/ENVIRONMENT.md", "scripts/README.md"]);
    assert.equal(body.coverage_complete, false);
    assert.equal(body.truncated, true);
    assert.equal(body.results_truncated, false);
    assert.equal(body.result_count, 1);
  });
  await check("partial empty search differs from completed empty search", async () => {
    fixtures.set("README.md", 503);
    const partial = (await search()).body;
    assert.equal(partial.result_count, 0);
    assert.equal(partial.coverage_complete, false);
    fixtures.clear();
    const complete = (await search()).body;
    assert.equal(complete.result_count, 0);
    assert.equal(complete.coverage_complete, true);
    assert.equal(complete.truncated, false);
  });
  await check("total HTTP or thrown failure returns 502", async () => {
    for (const failure of [404, "THROW"]) {
      fixtures.set("README.md", failure);
      const { status, body } = await search("needle", "README.md");
      assert.equal(status, 502);
      assert.equal(body.error, "search_sources_unavailable");
      assert.equal(body.searched_count, 0);
      assert.equal(body.coverage_complete, false);
      assert.equal(body.failed_paths.length, 1);
    }
  });
  await check("capped source cannot appear complete or search its synthetic marker", async () => {
    fixtures.set("README.md", "x".repeat(140100) + "\nneedle");
    const { body } = await search("needle", "README.md");
    assert.equal(body.result_count, 0);
    assert.deepEqual(body.source_truncated_paths, ["README.md"]);
    assert.equal(body.coverage_complete, false);
    assert.equal(body.truncated, true);
    assert.equal((await search("TRUNCATED BY WORKER", "README.md")).body.result_count, 0);
  });
  await check("query limit remains enforced", async () => {
    assert.equal((await search("x".repeat(97))).status, 400);
    assert.equal(requests.length, 0);
  });
  await check("matching preserves substrings but does not span lines", async () => {
    fixtures.set("README.md", "alpha\nbeta");
    assert.equal((await search("alpha beta", "README.md")).body.result_count, 0);
    fixtures.set("README.md", "prefixneedleSuffix");
    assert.equal((await search("needle", "README.md")).body.result_count, 1);
  });
  await check("all added corpus evidence and exact diagnostics are discoverable", async () => {
    globalThis.fetch = async url => {
      const sourcePath = decodeURIComponent(String(url).split("/main/")[1]);
      return new Response(await fs.readFile(new URL(`../${sourcePath}`, import.meta.url), "utf8"));
    };
    const additions = [
      ["customgpt/support-cards/configuration-readback.md", "Configuration Readbacks"],
      ["data/config/data.yaml", "ised"], ["scripts/README.md", "create-release.ps1"],
      ["docs/troubleshooting/TSR-0041-exact-call-history-and-scan-cap.md", "exact"],
      ["docs/decision-log.md", "ADR-0254"], ["docs/troubleshooting-log.md", "TSR-0041"],
      ["docs/ENVIRONMENT.md", "Backups"], ["docs/dev-runbook.md", "Validation"],
      ["docs/code-maps/README.md", "Code"], ["docs/fcc-state-validation.md", "FCC"],
      ["docs/canadian-state-validation.md", "ISED"]
    ];
    for (const [sourcePath, query] of additions) {
      const { status, body } = await search(query, sourcePath);
      assert.equal(status, 200, sourcePath);
      assert.equal(body.coverage_complete, true, sourcePath);
      assert(body.matches.some(m => m.path === sourcePath), sourcePath);
    }
    for (const query of ["FCC ULS refresh failed", "ISED startup refresh failed",
      "ISED processing metadata unavailable", "ised: invalid source manifest", "unterminated final record"]) {
      const { body } = await search(query);
      assert(body.matches.some(m => m.path === "data/config/README.md"), query);
    }
  });
} finally {
  globalThis.fetch = originalFetch;
}
