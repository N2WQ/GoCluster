const assert = require("node:assert/strict");
const { before, after, test } = require("node:test");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const { chromium } = require("playwright-core");

const site = pathToFileURL(path.resolve(__dirname, "../site/index.html")).href;
const rule = (slot, expr, action = "pass") => ({ slot, action, expr });
const cw = rule(1, "MODE=CW");
const rtty = rule(1, "MODE=RTTY");
const ft8 = rule(1, "MODE=FT8");
const second = rule(2, "MODE=FT4", "reject");
let browser;

before(async () => {
  browser = await chromium.launch({ channel: process.env.CHROMIUM_CHANNEL || undefined });
});
after(async () => { await browser?.close(); });

// Only the negotiated network peer is simulated. Load the shipped index and
// vendor files, then drive the real editor, revision guard and YAML session.
async function editor(t, filters = { rules: [cw], include_beacons: false }) {
  const page = await browser.newPage({ viewport: { width: 1400, height: 1000 } });
  t.after(() => page.close());
  page.setDefaultTimeout(7000);
  await page.goto(site);
  await page.evaluate(async initial => {
    const peer = window.peer = { filters: initial, revision: 1, patches: [] };
    const session = new GCP.Session({
      emit() {},
      parseYaml: JSON.parse,
      send(text) {
        let reply = { schema_version: 7, revision: String(peer.revision) };
        if (text.startsWith("GET YAML CONFIG SCHEMA 7 ID ")) {
          reply.request_id = text.split(" ").at(-1);
          if (peer.failGet) reply.error = { code: "read_failed", message: "Profile read failed" };
          else {
            const go = {
              include_beacons: "DEFAULT", allow_wwv: "DEFAULT", allow_wcy: "DEFAULT",
              allow_announce: "DEFAULT", allow_self: "DEFAULT", allow_toxic: "DEFAULT",
              ...peer.filters,
            };
            if (peer.omitField) delete go[peer.omitField];
            reply.configuration = { settings: { dialect: "go" }, filters: { go } };
          }
          if (peer.pauseGet) {
            peer.finishGet = () => session.onYaml(JSON.stringify(reply));
            return;
          }
        } else {
          const [header, ...document] = text.split("\n");
          if (header !== "PATCH YAML FILTER SCHEMA 7") throw new Error("Unexpected command: " + header);
          const patch = jsyaml.load(document.join("\n"));
          peer.patches.push(patch);
          reply.request_id = patch.request_id;
          if (patch.if_revision !== String(peer.revision)) {
            reply.error = { code: "revision_conflict", message: "Stale revision" };
          } else {
            Object.assign(peer.filters, patch.configuration.filters, peer.afterPatch);
            reply.revision = String(++peer.revision);
            reply.result = { valid: true, applied: true };
          }
        }
        session.onYaml(JSON.stringify(reply));
      },
    });
    session.state = cluster.mode = cluster.phase = "live";
    session.mode = "json";
    cluster.session = session;
    cluster.loginCall = "TEST-1";
    await cluster.refreshConfig();
    set({ connectOpen: false, drawer: "filters" });
  }, filters);
  await page.getByRole("dialog", { name: "Filters", exact: true }).waitFor();
  return page;
}

const button = (page, name) => page.getByRole("button", { name, exact: true });
const card = (page, slot) => page.locator(".rcard").filter({ has: page.getByText("#" + slot, { exact: true }) });
async function editRule(page, slot, expression) {
  const row = card(page, slot);
  await row.getByRole("button", { name: "Rule actions" }).click();
  await row.getByRole("menuitem", { name: "Edit as expression" }).click();
  await row.getByLabel("Expression", { exact: true }).fill(expression);
}
async function revise(page, fields, refresh = true) {
  await page.evaluate(async ({ fields, refresh }) => {
    Object.assign(peer.filters, fields);
    peer.revision++;
    if (refresh) await cluster.refreshConfig();
  }, { fields, refresh });
}
async function keep(page) {
  await button(page, "Keep my edits").click();
  await button(page, "Keep my edits").waitFor({ state: "hidden" });
}
async function apply(page) {
  await button(page, "Apply").click();
  await page.waitForFunction(() => !S.draft);
  await page.evaluate(() => cluster.refreshConfig());
  return page.evaluate(() => ({ patches: peer.patches, filters: peer.filters, loaded: S.snap.rules }));
}
async function resolveRule(page, slot, expression) {
  await page.getByLabel("Re-enter rule " + slot, { exact: true }).fill(expression);
  await button(page, "Set rule " + slot).click();
}

test("review, PATCH and reload preserve unrelated rule and beacon changes", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await revise(page, { rules: [cw, second], include_beacons: true });
  await keep(page);
  await page.getByRole("button", { name: /1 unapplied change/ }).click();
  await page.getByRole("radio", { name: "What the console sends" }).click();
  const review = await page.locator(".sends").evaluate(el => jsyaml.load(el.textContent.split("\n").slice(1).join("\n")));
  const expected = { rules: [rtty, second] };
  assert.deepEqual(review.configuration.filters, expected);
  const { patches, filters, loaded } = await apply(page);
  assert.equal(patches.length, 1);
  assert.equal(patches[0].if_revision, "2");
  assert.deepEqual(patches[0].configuration.filters, expected);
  assert.deepEqual(filters, { rules: [rtty, second], include_beacons: true });
  assert.deepEqual(loaded, [rtty, second]);
});

for (const [situation, remote] of [["changed", [ft8]], ["deleted", []]]) {
  test("a node-" + situation + " rule requires re-entry at its original slot", async t => {
    const page = await editor(t);
    await editRule(page, 1, rtty.expr);
    await revise(page, { rules: remote });
    await keep(page);
    await page.getByLabel("Re-enter rule 1", { exact: true }).waitFor();
    assert.equal(await button(page, "Apply").isDisabled(), true);
    assert.equal(await page.evaluate(() => peer.patches.length), 0);
    await resolveRule(page, 1, rtty.expr);
    assert.deepEqual((await apply(page)).loaded, [rtty]);
  });
}

test("a conflicting beacon choice distinguishes default from off and sends only the switch", async t => {
  const page = await editor(t, { rules: [cw], include_beacons: "DEFAULT" });
  await page.getByRole("tab", { name: "Switches" }).click();
  await page.getByRole("switch", { name: "Beacon spots" }).click();
  await revise(page, { rules: [cw, second], include_beacons: false });
  await keep(page);
  const select = page.getByLabel("Re-enter beacon switch", { exact: true });
  await select.waitFor();
  assert.equal(await button(page, "Apply").isDisabled(), true);
  await select.selectOption("DEFAULT");
  const { patches, filters, loaded } = await apply(page);
  assert.deepEqual(patches[0].configuration.filters, { include_beacons: "DEFAULT" });
  assert.equal(filters.include_beacons, "DEFAULT");
  assert.deepEqual(loaded, [cw, second]);
});

test("an expression mismatch keeps the draft despite matching rule number and action", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await page.evaluate(rule => { peer.afterPatch = { rules: [rule] }; }, ft8);
  await button(page, "Apply").click();
  await page.waitForFunction(() => peer.patches.length === 1 && !cluster.writing);
  assert.deepEqual(await page.evaluate(() => S.snap.rules), [ft8]);
  assert.equal(await page.evaluate(() => !!S.draft), true);
  await keep(page);
  await resolveRule(page, 1, rtty.expr);
  await page.evaluate(() => { delete peer.afterPatch; });
  assert.deepEqual((await apply(page)).loaded, [rtty]);
});

test("switch readback distinguishes an explicit off from an equivalent node default", async t => {
  const page = await editor(t, { rules: [cw], include_beacons: true });
  await page.getByRole("tab", { name: "Switches" }).click();
  await page.getByRole("switch", { name: "Beacon spots" }).click();
  await page.evaluate(() => { peer.afterPatch = { include_beacons: "DEFAULT" }; });
  await button(page, "Apply").click();
  await page.waitForFunction(() => peer.patches.length === 1 && !cluster.writing);
  await keep(page);
  const select = page.getByLabel("Re-enter beacon switch", { exact: true });
  await select.waitFor();
  assert.equal(await button(page, "Apply").isDisabled(), true);
  await page.evaluate(() => { delete peer.afterPatch; });
  await select.selectOption("off");
  assert.equal((await apply(page)).filters.include_beacons, false);
});

test("successful write followed by a failed readback is preserved and never resent on reload", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await page.evaluate(() => { peer.failGet = true; });
  await button(page, "Apply").click();
  const reload = button(page, "Reload profile");
  await reload.waitFor();
  assert.equal(await button(page, "Apply").isDisabled(), true);
  assert.deepEqual(await page.evaluate(() => peer.filters.rules), [rtty]);
  await page.evaluate(() => { peer.failGet = false; });
  await reload.click();
  await keep(page);
  await button(page, "Close draft").waitFor();
  assert.equal(await page.evaluate(() => peer.patches.length), 1);
});

test("a missing switch on readback cannot confirm a submitted node default", async t => {
  const page = await editor(t);
  await page.getByRole("tab", { name: "Switches" }).click();
  await button(page, "Use node default").click();
  await page.evaluate(() => { peer.omitField = "include_beacons"; });
  await button(page, "Apply").click();
  const reload = button(page, "Reload profile");
  await reload.waitFor();
  assert.equal(await button(page, "Apply").isDisabled(), true);
  assert.deepEqual(await page.evaluate(() => peer.patches[0].configuration.filters), { include_beacons: "DEFAULT" });
  await page.evaluate(() => { delete peer.omitField; });
  await reload.click();
  await keep(page);
  await button(page, "Close draft").waitFor();
  assert.equal(await page.evaluate(() => peer.patches.length), 1);
});

test("revision rejection and failed GET retain edits and require a successful reload", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await revise(page, { rules: [cw, second] }, false);
  await page.evaluate(() => { peer.failGet = true; });
  await button(page, "Apply").click();
  const reload = button(page, "Reload profile");
  await reload.waitFor();
  assert.equal(await button(page, "Apply").isDisabled(), true);
  await page.evaluate(() => { peer.failGet = false; });
  await reload.click();
  await keep(page);
  const { patches, loaded } = await apply(page);
  assert.deepEqual(patches.map(p => p.if_revision), ["1", "2"]);
  assert.deepEqual(loaded, [rtty, second]);
});

test("a further edit during readback survives completion of the first Apply", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await page.evaluate(() => { peer.pauseGet = true; });
  await button(page, "Apply").click();
  await page.waitForFunction(() => !!peer.finishGet);
  await page.getByLabel("Expression", { exact: true }).fill(ft8.expr);
  await page.evaluate(() => { peer.pauseGet = false; peer.finishGet(); });
  await page.waitForFunction(() => !cluster.writing);
  assert.equal(await page.getByLabel("Expression", { exact: true }).inputValue(), ft8.expr);
  assert.equal(await page.evaluate(() => peer.patches.length), 1);
  assert.deepEqual((await apply(page)).loaded, [ft8]);
});

test("reverting a submitted edit survives delayed readback failure and reload", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await page.evaluate(() => { peer.pauseGet = peer.failGet = true; });
  await button(page, "Apply").click();
  await page.waitForFunction(() => !!peer.finishGet);
  await page.getByLabel("Expression", { exact: true }).fill(cw.expr);
  await page.evaluate(() => { peer.pauseGet = peer.failGet = false; peer.finishGet(); });
  await button(page, "Reload profile").click();
  await keep(page);
  // Either a recovered edit or an explicit re-entry can preserve the reversal.
  // Losing it leaves an idle draft with no way to apply the intended CW rule.
  if (await page.getByLabel("Re-enter rule 1", { exact: true }).isVisible()) {
    await resolveRule(page, 1, cw.expr);
  }
  assert.deepEqual(await page.evaluate(() => peer.filters.rules), [rtty]);
  assert.equal(await page.evaluate(() => peer.patches.length), 1);
  const { patches, loaded } = await apply(page);
  assert.deepEqual(patches.map(p => p.configuration.filters.rules), [[rtty], [cw]]);
  assert.deepEqual(loaded, [cw]);
});

test("a delete racing with an edit must be re-entered after a second revision", async t => {
  const page = await editor(t);
  const row = card(page, 1);
  await row.getByRole("button", { name: "Rule actions" }).click();
  await row.getByRole("menuitem", { name: "Delete rule" }).click();
  await revise(page, { rules: [ft8] });
  await keep(page);
  await revise(page, { rules: [rtty, second] });
  await keep(page);
  await button(page, "Delete rule 1").waitFor();
  assert.equal(await button(page, "Apply").isDisabled(), true);
  await button(page, "Delete rule 1").click();
  const { patches, loaded } = await apply(page);
  assert.deepEqual(loaded, [second]);
  assert.deepEqual(patches.map(p => p.if_revision), ["3"]);
});

test("simultaneous additions at the same slot conflict; convergent edits survive", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await button(page, "Add a Show rule").click();
  await editRule(page, 0, "MODE=CW");
  await revise(page, { rules: [rule(0, "MODE=FT4"), rtty, second] });
  await keep(page);
  await page.getByLabel("Re-enter rule 0", { exact: true }).waitFor();
  assert.equal(await page.getByLabel("Re-enter rule 1", { exact: true }).count(), 0);
  assert.equal(await button(page, "Apply").isDisabled(), true);
  await resolveRule(page, 0, "MODE=CW");
  assert.deepEqual((await apply(page)).loaded, [rule(0, "MODE=CW"), rtty, second]);
});
