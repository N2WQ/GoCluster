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
    const peer = window.peer = { filters: initial, revision: 1, patches: [], reads: 0 };
    const session = new GCP.Session({
      emit(event, data) { cluster.onSession(session, event, data); },
      parseYaml: JSON.parse,
      send(text) {
        let reply = { schema_version: 7, revision: String(peer.revision) };
        if (text.startsWith("GET YAML CONFIG SCHEMA 7 ID ")) {
          peer.reads++;
          reply.request_id = text.split(" ").at(-1);
          if (peer.busyReads > 0) {
            peer.busyReads--;
            reply.error = { code: "configuration_busy", message: "Configuration transfer is busy" };
          } else if (peer.failGet) reply.error = { code: "read_failed", message: "Profile read failed" };
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

test("a conflicting beacon choice distinguishes default from explicit on and sends only the switch", async t => {
  const page = await editor(t, { rules: [cw], include_beacons: "DEFAULT" });
  await page.getByRole("tab", { name: "Switches" }).click();
  await page.getByRole("switch", { name: "Beacon spots" }).click();
  await revise(page, { rules: [cw, second], include_beacons: true });
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

test("switch readback distinguishes explicit on from an equivalent node default", async t => {
  const page = await editor(t, { rules: [cw], include_beacons: false });
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
  await select.selectOption("on");
  assert.equal((await apply(page)).filters.include_beacons, true);
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

test("logger notices refresh all four delivery switches in one read", async t => {
  const page = await editor(t, { rules: [cw], include_beacons: false, allow_wwv: false, allow_wcy: false, allow_announce: false });
  await page.getByRole("tab", { name: "Switches" }).click();
  const switches = ["beacon", "wwv", "wcy", "announce"];
  const values = async () => Promise.all(switches.map(name => page.locator("#sw-" + name).getAttribute("aria-checked")));
  assert.deepEqual(await values(), Array(4).fill("false"));
  await page.evaluate(() => {
    Object.assign(peer.filters, { include_beacons: "DEFAULT", allow_wwv: true, allow_wcy: true, allow_announce: true });
    peer.revision++;
    for (let i = 0; i < 5; i++) cluster.session.feed('["notice","config-changed","Logger changed filters.","2"]\n');
  });
  await page.waitForFunction(() => S.snap.revision === "2");
  assert.deepEqual(await values(), Array(4).fill("true"));
  await page.locator(".sw-row").filter({ has: page.locator("#sw-beacon") }).getByText("Node default (on)", { exact: true }).waitFor();
  assert.equal(await page.evaluate(() => peer.reads), 2);
  await page.evaluate(() => {
    // The sample contains a real server-delivered beacon. Hidden mode applies
    // the preview to that sample, including the inherited switch.
    cluster.sample = [{ dxCall: "4U1UN", deCall: "W1AW", mode: "CW", beacon: true }];
    S.showHidden = true;
  });
  assert.equal(await page.evaluate(() => rowsForView()[0].pass), true);
});

test("a notice during a pending read gets a follow-up and preserves the draft", async t => {
  const page = await editor(t);
  await editRule(page, 1, rtty.expr);
  await page.evaluate(() => {
    peer.pauseGet = true;
    cluster.session.feed('["notice","config-changed","Live profile changed.","1"]\n');
  });
  await page.waitForFunction(() => !!peer.finishGet);
  await page.evaluate(rules => {
    peer.filters.rules = rules;
    peer.revision++;
    peer.pauseGet = false;
    cluster.session.feed('["notice","config-changed","Live profile changed.","2"]\n');
  }, [cw, second]);
  await page.waitForTimeout(850);
  assert.equal(await page.evaluate(() => peer.reads), 2);
  await page.evaluate(() => peer.finishGet());
  await button(page, "Keep my edits").waitFor();
  assert.equal(await page.evaluate(() => peer.reads), 3);
  assert.equal(await button(page, "Apply").isDisabled(), true);
  await keep(page);
  assert.deepEqual((await apply(page)).loaded, [rtty, second]);
});

test("a busy automatic read retries and applies the latest settings snapshot", async t => {
  const page = await editor(t);
  await page.evaluate(() => {
    peer.filters.allow_wcy = false;
    peer.revision++;
    peer.busyReads = 1;
    cluster.session.feed('["notice","config-changed","Live profile changed.","2"]\n');
  });
  await page.waitForFunction(() => S.snap.revision === "2" && !S.snap.conn.cfgError);
  assert.equal(await page.evaluate(() => S.snap.switches.wcy), false);
  assert.equal(await page.evaluate(() => peer.reads), 3);
});

test("a retiring session's completed read cannot update a new session's profile", async t => {
  const page = await editor(t);
  const result = await page.evaluate(async () => {
    peer.pauseGet = true;
    const stale = cluster.refreshConfig().catch(error => error.code);
    peer.finishGet(); // The old reply is ready but its continuation has not run.
    cluster.closeSocket();
    const fresh = { schema_version: 7, revision: "new", configuration: { filters: { go: {
      rules: [], include_beacons: true, allow_wwv: true, allow_wcy: true, allow_announce: true, allow_self: true, allow_toxic: true,
    } } } };
    cluster.session = { closed: false, getYaml: async () => ({ doc: fresh, text: "" }) };
    await cluster.refreshConfig();
    return { old: await stale, latest: S.snap.revision, error: S.snap.conn.cfgError };
  });
  assert.deepEqual(result, { old: "disconnected", latest: "new", error: "" });
});

test("the header shows the build response from the current live session", async t => {
  const page = await browser.newPage({ viewport: { width: 1200, height: 900 } });
  t.after(() => page.close());
  await page.goto(site);
  assert.equal(await page.locator(".bname").textContent(), "GoCluster by N2WQ");
  assert.equal(await page.locator(".bsub").count(), 0);
  await page.evaluate(() => {
    window.buildCommands = [];
    window.connectForVersion = () => {
      const session = cluster.session = new GCP.Session({
        send(text) { buildCommands.push(text); },
        emit(event, data) { cluster.onSession(session, event, data); },
      });
      cluster.mode = "live";
      cluster.deploy.startupHistory = 0;
      session.mode = "json";
      session.setState("live");
      return session;
    };
    window.firstVersionSession = connectForVersion();
    firstVersionSession.feed('["text","Build version: vdev-previous"]\n');
  });
  await page.locator(".bsub").waitFor({ state: "visible" });
  assert.equal(await page.locator(".bsub").textContent(), "vdev-previous");
  await page.evaluate(() => {
    cluster.closeSocket();
    window.currentVersionSession = connectForVersion();
    cluster.onSession(firstVersionSession, "build-version", "obsolete");
  });
  await page.locator(".bsub").waitFor({ state: "detached" });
  await page.evaluate(() => currentVersionSession.feed('["text","Build version: 261124"]\n'));
  await page.locator(".bsub").waitFor({ state: "visible" });
  assert.equal(await page.locator(".bsub").textContent(), "v261124");
  assert.equal(await page.evaluate(() => buildCommands.filter(command => command === "SHOW BUILD").length), 2);
});

test("the current connection's IPv4 follows the version and a retired reply is ignored", async t => {
  const page = await browser.newPage({ viewport: { width: 1200, height: 900 } });
  t.after(() => page.close());
  await page.route("http://console.test/**", route => {
    const { pathname } = new URL(route.request().url());
    return route.fulfill({ path: path.join(__dirname, "../site", pathname === "/" ? "index.html" : pathname.slice(1)) });
  });
  await page.goto("http://console.test/");
  await page.evaluate(() => {
    window.visitorCalls = [];
    window.fetch = (path, opts) => new Promise(resolve => visitorCalls.push({ path, opts, resolve }));
    window.connectToNode = version => {
      cluster.closeSocket();
      const session = cluster.session = new GCP.Session({
        send() {},
        emit(event, data) { cluster.onSession(session, event, data); },
      });
      cluster.mode = "live";
      cluster.deploy.startupHistory = 0;
      session.mode = "json";
      session.setState("live");
      session.feed(JSON.stringify(["text", "Build version: " + version]) + "\n");
    };
    window.finishVisitor = (index, ipv4, ok = true) => visitorCalls[index].resolve({ ok, json: async () => ({ ipv4 }) });
    connectToNode("261124");
    cluster.onLive();
    finishVisitor(0, "192.0.2.17");
  });
  const subtitle = page.locator(".bsub");
  await subtitle.filter({ hasText: /^v261124 · 192\.0\.2\.17$/ }).waitFor({ state: "visible" });
  assert.deepEqual(await page.evaluate(() => visitorCalls.map(({ path, opts }) => [path, opts.cache])), [["/visitor-ip", "no-store"]]);

  await page.evaluate(() => {
    connectToNode("261125");
    connectToNode("261126");
    finishVisitor(2, "198.51.100.24");
    finishVisitor(1, "203.0.113.33");
  });
  await subtitle.filter({ hasText: /^v261126 · 198\.51\.100\.24$/ }).waitFor({ state: "visible" });
  assert.equal(await page.evaluate(() => visitorCalls[1].opts.signal.aborted), true);

  await page.evaluate(() => { connectToNode("261127"); finishVisitor(3, null); });
  await subtitle.filter({ hasText: "v261127" }).waitFor();
  assert.equal(await subtitle.textContent(), "v261127");

  await page.evaluate(() => { connectToNode("261128"); finishVisitor(4, "192.0.2.19", false); });
  await subtitle.filter({ hasText: "v261128" }).waitFor();
  assert.equal(await subtitle.textContent(), "v261128");
});
