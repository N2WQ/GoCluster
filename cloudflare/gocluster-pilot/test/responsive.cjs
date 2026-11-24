const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const { before, after, test } = require("node:test");
const { chromium } = require("playwright-core");

const site = path.resolve(__dirname, "../site");
const index = pathToFileURL(path.join(site, "index.html")).href;
const capture = readFileSync(path.join(site, "replay/session.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
const short = { width: 844, height: 260 };
let browser;

before(async () => {
  browser = await chromium.launch({ channel: process.env.CHROMIUM_CHANNEL || undefined });
});
after(async () => { await browser?.close(); });

const button = (scope, name) => scope.getByRole("button", { name, exact: true });
const view = (page, name) => page.getByRole("link", { name, exact: true });

async function openPage(t, viewport, touch = true) {
  const page = await browser.newPage({ viewport, isMobile: touch, hasTouch: touch });
  page.setDefaultTimeout(5000);
  t.after(() => page.close());
  await page.goto(index);
  await page.getByRole("dialog", { name: "Connect to GoCluster" }).waitFor();
  return page;
}

async function simulatedNode(page, count = 70) {
  // Drive the shipped client with captured wire records and a local command sink.
  await page.evaluate(({ records, count }) => {
    window.commands = [];
    const feed = record => session.onJsonLine(JSON.stringify(record));
    const session = new GCP.Session({
      emit(event, data) { cluster.onSession(session, event, data); },
      parseYaml: JSON.parse,
      send(command) {
        window.commands.push(command);
        queueMicrotask(() => {
          if (command.startsWith("SH DX")) records.filter(record => ["hb", "h", "he"].includes(record[0])).forEach(feed);
          else if (command.startsWith("DX ")) feed(["text", "Spot queued."]);
          else if (command.startsWith("GET YAML")) session.onYaml(JSON.stringify({
            schema_version: 7, request_id: command.split(" ").at(-1), revision: "1",
            configuration: { settings: { dialect: "go" }, filters: { go: {
              include_beacons: "DEFAULT", allow_wwv: "DEFAULT", allow_wcy: "DEFAULT", allow_announce: "DEFAULT",
              allow_self: "DEFAULT", allow_toxic: "DEFAULT", rules: [{ slot: 1, action: "pass", expr: "MODE=CW" }],
            } } },
          }));
        });
      },
    });
    session.state = cluster.mode = cluster.phase = "live";
    session.mode = "json";
    cluster.session = session;
    cluster.loginCall = "TEST-1";
    records.filter(record => record[0] === "schema").forEach(feed);
    cluster.refreshConfig();
    const sample = records.find(record => record[0] === "s");
    window.feedSpots = (first, last) => {
      for (let id = first; id <= last; id++) {
        const record = [...sample];
        record[1] = id;
        record[2] = String(BigInt(Date.now() - (last - id) * 1000) * 1000000n);
        record[18] = ["W1A" + String(id).padStart(2, "0"), ...sample[18].slice(1)];
        feed(record);
      }
    };
    window.feedSpots(1, count);
    set({ connectOpen: false });
  }, { records: capture, count });
  await page.locator(".stream .row").first().waitFor();
}

async function inViewport(locator) {
  assert.equal(await locator.evaluate(el => {
    const r = el.getBoundingClientRect();
    return el.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2));
  }), true, "control must be hit-testable in the viewport");
}

test("short landscape scrolls the page and keeps the four views operable", async t => {
  const page = await openPage(t, short);
  await simulatedNode(page);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), short.width);
  await page.mouse.move(short.width - 3, short.height - 3);
  await page.mouse.wheel(0, 1000);
  await page.waitForFunction(() => scrollY > 0);
  await inViewport(button(page, "Expand console"));
  await button(page, "Expand console").click();
  const cmd = page.getByRole("textbox", { name: "Cluster command" });
  await cmd.fill("SHOW HOLD");
  await cmd.press("Enter");
  await page.waitForFunction(() => window.commands.includes("SHOW HOLD"));
  assert.ok((await page.locator(".spot-scroll").boundingBox()).height >= 100);

  await view(page, "History").click();
  await page.getByLabel("Call, prefix or country", { exact: true }).fill("W1AW");
  await button(page, "Search").click();
  await page.locator(".hist .row").first().waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("region", { name: "History" }).waitFor();
  assert.equal(await page.getByLabel("Call, prefix or country", { exact: true }).inputValue(), "W1AW");
  assert.equal(await page.locator(".hist .row").count(), 2);

  await view(page, "Band map").click();
  await page.locator(".bm-spot").first().click();
  const details = page.getByRole("dialog", { name: /Spot details for/ });
  await details.waitFor();
  await button(details, "Close").click();
  await view(page, "Openings").click();
  await button(page, "20m to North America: 70 stations").click();
  await page.getByRole("region", { name: "Spots" }).waitFor();
});

test("rotation keeps the Spots scrollport paused until the operator resumes", async t => {
  const page = await openPage(t, { width: 390, height: 844 });
  await simulatedNode(page);
  const spots = page.locator(".spot-scroll");
  await spots.evaluate(el => { el.scrollTop = 480; });
  await page.waitForFunction(() => S.freezeId === 70);
  await page.setViewportSize(short);
  await page.evaluate(() => window.feedSpots(71, 72));
  const newer = button(page, "2 newer spots");
  await newer.waitFor();
  assert.ok(await spots.evaluate(el => el.scrollTop > 60));
  assert.equal(await page.locator(".stream .row").first().getByText("W1A70").count(), 1);
  await newer.click();
  await page.waitForFunction(() => S.freezeId === 0);
  await page.locator(".stream .row").first().getByText("W1A72").waitFor();
});

test("a taller viewport can fit the paused list without resuming it", async t => {
  const page = await openPage(t, short);
  await simulatedNode(page, 6);
  await page.locator(".spot-scroll").evaluate(el => { el.scrollTop = el.scrollHeight; });
  await page.waitForFunction(() => S.freezeId === 6);
  await page.setViewportSize({ width: 390, height: 1100 });
  await page.waitForFunction(() => document.querySelector(".spot-scroll").scrollTop === 0);
  await page.evaluate(() => window.feedSpots(7, 7));
  await button(page, "1 newer spot").click();
  await page.waitForFunction(() => S.freezeId === 0);
  await page.locator(".stream .row").first().getByText("W1A07").waitFor();
});

test("320 CSS-pixel reflow retains toolbar actions, filter edits and keyboard focus", async t => {
  const page = await openPage(t, { width: 320, height: 568 }, false);
  await simulatedNode(page);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 320);
  const filters = page.getByRole("button", { name: /^Filters/ });
  await filters.focus();
  await filters.press("Enter");
  const drawer = page.getByRole("dialog", { name: "Filters" });
  await drawer.waitFor();
  assert.equal(await drawer.getAttribute("aria-modal"), "true");
  assert.equal(await button(drawer, "Close filters").evaluate(el => el === document.activeElement), true);
  await button(drawer, "Rule actions").click();
  await drawer.getByRole("menuitem", { name: "Edit as expression" }).click();
  const expr = drawer.getByLabel("Expression", { exact: true });
  await expr.fill("MODE=RTTY");
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.waitForFunction(() => !document.querySelector(".drawer").hasAttribute("aria-modal"));
  assert.equal(await expr.evaluate(el => el === document.activeElement), true);
  await page.setViewportSize(short);
  await page.waitForFunction(() => document.querySelector(".drawer").hasAttribute("aria-modal"));
  assert.equal(await expr.inputValue(), "MODE=RTTY");
  assert.equal(await expr.evaluate(el => el === document.activeElement), true);
  await button(drawer, "Apply").scrollIntoViewIfNeeded();
  await inViewport(button(drawer, "Apply"));
  await page.keyboard.press("Escape");
  await drawer.waitFor({ state: "hidden" });
  assert.equal(await filters.evaluate(el => el === document.activeElement), true);
  await page.setViewportSize({ width: 320, height: 568 });
  await button(page, "Activity and bulletins").click();
  await button(page.getByRole("dialog", { name: "Activity" }), "Close").click();
  await button(page, "Settings").click();
  await button(page.getByRole("dialog", { name: "Settings" }), "Close settings").click();
  await button(page, "Cluster login and connection").click();
  const connect = page.getByRole("dialog", { name: "Connect to GoCluster" });
  await page.setViewportSize(short);
  await button(connect, "Replay the pilot capture").scrollIntoViewIfNeeded();
  await inViewport(button(connect, "Replay the pilot capture"));
  await button(connect, "Close").click();
});

test("Post a spot can be completed after the visible viewport contracts", async t => {
  const page = await openPage(t, { width: 390, height: 844 });
  await simulatedNode(page);
  const post = button(page, "Post a spot");
  await post.focus();
  await post.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Post a spot" });
  await dialog.getByLabel("Frequency (kHz)").fill("14025");
  await dialog.getByLabel("Callsign heard").fill("W1AW");
  const comment = dialog.getByLabel("Comment");
  await comment.fill("UP 2");
  await page.setViewportSize(short);
  // Model a keyboard reducing the visual viewport without changing the layout viewport.
  await page.evaluate(() => {
    Object.defineProperties(visualViewport, {
      height: { configurable: true, value: 130 },
      offsetTop: { configurable: true, value: 24 },
    });
    visualViewport.dispatchEvent(new Event("resize"));
  });
  await page.waitForFunction(() => {
    const panel = document.querySelector("dialog[open]").getBoundingClientRect();
    const input = document.getElementById("pt").getBoundingClientRect();
    return Math.abs(panel.y - 24) < 1 && Math.abs(panel.height - 130) < 1 && input.top >= 24 && input.bottom <= 154;
  });
  assert.equal(await comment.inputValue(), "UP 2");
  await inViewport(comment);
  await dialog.evaluate(el => el.scrollTop = el.scrollHeight);
  await inViewport(button(dialog, "Post spot"));
  await button(dialog, "Post spot").click();
  await dialog.waitFor({ state: "hidden" });
  assert.equal(await page.evaluate(() => window.commands.filter(cmd => cmd.startsWith("DX ")).join()), "DX 14025.00 W1AW UP 2");
  assert.equal(await post.evaluate(el => el === document.activeElement), true);
});

test("desktop keeps a docked console and nonmodal side panel", async t => {
  const page = await openPage(t, { width: 1440, height: 1000 }, false);
  await simulatedNode(page);
  await button(page, "Expand console").click();
  await page.getByRole("button", { name: /^Filters/ }).click();
  const drawer = page.getByRole("dialog", { name: "Filters" });
  await drawer.waitFor();
  assert.equal(await drawer.getAttribute("aria-modal"), null);
  assert.equal(await page.evaluate(() => document.documentElement.scrollHeight), 1000);
  await inViewport(button(page, "Collapse console"));
  await view(page, "History").click();
  await page.getByRole("region", { name: "History" }).waitFor();
});
