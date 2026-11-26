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

async function simulatedNode(page, count = 70, grid = "") {
  // Drive the shipped client with captured wire records and a local command sink.
  await page.evaluate(({ records, count, grid }) => {
    window.commands = [];
    window.profileGrid = grid;
    window.effectiveGrid = "";
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
            configuration: { settings: { dialect: "go", grid: window.profileGrid }, filters: { go: {
              include_beacons: "DEFAULT", allow_wwv: "DEFAULT", allow_wcy: "DEFAULT", allow_announce: "DEFAULT",
              allow_self: "DEFAULT", allow_toxic: "DEFAULT", rules: [{ slot: 1, action: "pass", expr: "MODE=CW" }],
            } } },
            status: { effective: { grid: window.effectiveGrid, grid_derived: !!window.effectiveGrid } },
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
  }, { records: capture, count, grid });
  await page.locator(".stream .row").first().waitFor();
}

test("Spotter and Via keep independent preferences and identify the same report", async t => {
  const page = await openPage(t, { width: 1440, height: 900 }, false);
  await page.evaluate(() => localStorage.setItem("gcui.prefs", JSON.stringify({
    colOrder: ["dx", "de", "freq"], colShow: { de: false }, colWidths: { de: 143, conf: 36 },
  })));
  await page.reload();
  await simulatedNode(page, 1);

  const spots = page.getByRole("region", { name: "Spots" });
  const settings = page.getByRole("dialog", { name: "Settings" });
  const toggle = name => settings.getByRole("switch", { name, exact: true });
  const columns = () => spots.locator(".thead > .th").evaluateAll(cells => cells.map(cell => cell.dataset.column));
  await button(page, "Settings").click();
  assert.equal(await toggle("Show Spotter").getAttribute("aria-checked"), "false");
  assert.equal(await toggle("Show Via").getAttribute("aria-checked"), "true");
  await toggle("Show Spotter").click();
  await button(settings, "Close settings").click();
  assert.deepEqual((await columns()).slice(0, 4), ["dx", "de", "via", "freq"]);
  assert.equal((await spots.locator(".th-de").boundingBox()).width, 143);
  const confidence = spots.locator(".th-conf .col-label");
  assert.match(await confidence.textContent(), /CONF$/);
  assert.equal(await confidence.evaluate(el => el.scrollWidth <= el.clientWidth), true);
  const viaHeader = spots.locator(".th-via");
  const grip = await viaHeader.getByRole("separator", { name: "Resize Via" }).boundingBox();
  const viaWidth = Math.round((await viaHeader.boundingBox()).width);
  const gripX = Math.round(grip.x + grip.width / 2);
  const gripY = Math.round(grip.y + grip.height / 2);
  await page.mouse.move(gripX, gripY);
  await page.mouse.down();
  await page.mouse.move(gripX + 20, gripY);
  await page.mouse.up();
  await page.waitForFunction(width => S.prefs.colWidths.via === width, viaWidth + 20);
  await page.waitForFunction(width => document.querySelector(".th-via")?.offsetWidth === width, viaWidth + 20);
  assert.equal((await spots.locator(".th-de").boundingBox()).width, 143);

  const reports = [
    ["K1RBN", "EA8BFK-#", "RBN", "RBN", 31, "RBN"],
    ["K1PSK", "N1PSK", "PSKREPORTER", "PSKREPORTER", 9, "PSKr"],
    ["K1DXS", "N1DXS", "UPSTREAM", "DXSUMMIT", null, "DXs"],
    ["K1UP", "N1UP", "UPSTREAM", "N2UP-1", null, "N2UP-1"],
    ["K1UP0", "N1UP", "UPSTREAM", null, null, "Upstream"],
    ["K1FT", "N1FT-#", "FT8", "RBN-DIGITAL", 3, "RBN"],
    ["K1UNK", "N1UNK", "PEER", null, null, "Peer"],
    ["K1RBN", "UA9BA", "PEER", "EA2CW-1", 5, "EA2CW-1"],
  ];
  const sample = capture.find(record => record[0] === "s");
  await page.evaluate(({ sample, reports }) => {
    reports.forEach(([dx, de, source, node, snr], index) => {
      const record = [...sample];
      record[1] = 100 + index;
      record[2] = String(BigInt(Date.now() + index) * 1000000n);
      record[8] = snr;
      record[12] = source;
      record[13] = node;
      record[14] = source === "UPSTREAM";
      record[18] = [dx, ...sample[18].slice(1)];
      record[19] = [de, ...sample[19].slice(1)];
      cluster.session.onJsonLine(JSON.stringify(record));
    });
  }, { sample, reports });
  const cells = async (row, spotter, via) => {
    assert.equal(await row.locator(".c-de .mono").first().textContent(), spotter);
    assert.equal(await row.locator(".c-via").textContent(), via);
  };
  for (const [dx, de, , , , via] of reports.slice(1, -1)) {
    await cells(spots.getByRole("row").filter({ has: page.getByText(dx, { exact: true }) }), de, via);
  }
  const group = spots.locator(".repeat-group").filter({ hasText: "K1RBN" });
  await cells(group.locator(".repeat-summary"), "EA8BFK-#", "RBN");
  await group.getByRole("button", { name: /2 reports/ }).click();
  await cells(group.locator(".repeat-member").first(), "UA9BA", "EA2CW-1");

  await page.setViewportSize({ width: 320, height: 568 });
  await group.locator(".repeat-summary .c-via").waitFor();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 320);
  await page.setViewportSize({ width: 1440, height: 900 });
  await button(page, "Settings").click();
  await toggle("Show Via").click();
  await button(settings, "Move Via left").click();
  await button(settings, "Close settings").click();
  assert.equal((await columns()).includes("via"), false);
  assert.equal((await columns()).includes("de"), true);

  await page.reload();
  await simulatedNode(page, 1);
  await view(page, "History").click();
  await button(page, "Search").click();
  const history = page.getByRole("region", { name: "History" });
  const archived = history.getByRole("row").filter({ hasText: "W1AW" });
  await archived.locator(".c-de").waitFor();
  assert.equal(await history.locator(".th-via").count(), 0);
  await button(page, "Settings").click();
  assert.equal(await toggle("Show Via").getAttribute("aria-checked"), "false");
  await toggle("Show Via").click();
  await button(settings, "Close settings").click();
  assert.deepEqual(await history.locator(".th").evaluateAll(cells => cells.slice(0, 4).map(cell => cell.dataset.column)),
    ["dx", "via", "de", "freq"]);
  await cells(archived, "K9XYZ", "Local");
});

test("spot geometry shares column preferences and the current grid across Spots and History", async t => {
  const page = await openPage(t, { width: 1440, height: 1000 }, false);
  await page.evaluate(() => localStorage.setItem("gcui.prefs", JSON.stringify({ colOrder: ["dx", "time"], colShow: { conf: false } })));
  await page.reload();
  await simulatedNode(page, 1, "FN41");

  const spots = page.getByRole("region", { name: "Spots" });
  const history = page.getByRole("region", { name: "History" });
  await spots.getByText("≈000°", { exact: true }).waitFor();
  assert.equal(await spots.locator(".th-distance, .th-conf").count(), 0);
  assert.equal(await spots.locator(".th").first().getAttribute("data-column"), "dx");

  const settings = page.getByRole("dialog", { name: "Settings" });
  const toggle = name => settings.getByRole("switch", { name, exact: true });
  const units = name => settings.getByRole("radio", { name, exact: true });
  await button(page, "Settings").click();
  assert.equal(await toggle("Show Azimuth").getAttribute("aria-checked"), "true");
  assert.equal(await toggle("Show Distance").getAttribute("aria-checked"), "false");
  await toggle("Show Distance").click();
  await button(settings, "Close settings").click();
  // FN41 and FN42 lie on one meridian, one degree of latitude apart.
  await spots.getByText("≈111 km", { exact: true }).waitFor();

  await view(page, "History").click();
  await button(page, "Search").click();
  const w1aw = history.getByRole("row").filter({ hasText: "W1AW" });
  await w1aw.getByText("≈000°", { exact: true }).waitFor();
  await w1aw.getByText("≈111 km", { exact: true }).waitFor();
  await page.evaluate(async () => {
    window.profileGrid = "";
    window.effectiveGrid = "FN43";
    await cluster.refreshConfig();
  });
  await w1aw.getByText("≈180°", { exact: true }).waitFor();
  assert.match(await w1aw.locator(".c-azimuth").getAttribute("title"), /derived user and DX locator/);

  await button(page, "Settings").click();
  await units("Miles").click();
  await toggle("Show Azimuth").click();
  await button(settings, "Close settings").click();
  await w1aw.getByText("≈69 mi", { exact: true }).waitFor();
  assert.equal(await history.locator(".c-azimuth").count(), 0);

  await view(page, "Spots").click();
  await page.reload();
  await simulatedNode(page, 1, "FN41");
  await spots.getByText("≈69 mi", { exact: true }).waitFor();
  assert.equal(await spots.locator(".c-azimuth").count(), 0);
  assert.equal(await spots.locator(".th").first().getAttribute("data-column"), "dx");
  await page.setViewportSize({ width: 320, height: 568 });
  await spots.getByText("Distance", { exact: true }).last().waitFor();
  assert.equal(await spots.locator(".row .c-distance").evaluate(el => el.scrollWidth <= el.clientWidth), true);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 320);
});

test("geometry handles six-character, missing, coincident and antipodal grid centers", async t => {
  const page = await openPage(t, { width: 1440, height: 1000 }, false);
  await page.evaluate(() => localStorage.setItem("gcui.prefs", JSON.stringify({ colShow: { distance: true } })));
  await page.reload();
  await simulatedNode(page, 1, "FN42AA");
  const sample = capture.find(record => record[0] === "s");
  const add = async (call, grid, derived = false) => {
    await page.evaluate(({ sample, call, grid, derived }) => {
      const record = [...sample];
      record[2] = String(BigInt(Date.now()) * 1000000n);
      record[12] = "RBN";
      record[14] = false;
      record[18] = [...sample[18]];
      record[18][0] = call;
      record[18][7] = grid;
      record[18][8] = derived;
      record[18][9] = "FN";
      cluster.session.onJsonLine(JSON.stringify(record));
    }, { sample, call, grid, derived });
    return page.getByRole("region", { name: "Spots" }).getByRole("row").filter({ hasText: call });
  };
  const cells = async (row, azimuth, distance) => {
    await row.locator(".c-azimuth .mono").getByText(azimuth, { exact: true }).waitFor();
    await row.locator(".c-distance .mono").getByText(distance, { exact: true }).waitFor();
  };
  // A to M changes latitude by half a degree within the same square.
  await cells(await add("K1FINE", "FN42AM", true), "≈000°", "≈56 km");
  await cells(await add("K1SAME", "FN42AA"), "—", "0 km");
  await cells(await add("K1COARSE", null), "—", "—");
  await cells(await add("K1BAD", "FN42ZZ"), "—", "—");

  await page.evaluate(async () => { window.profileGrid = "FN31"; await cluster.refreshConfig(); });
  await cells(await add("K1OPP", "OE38"), "—", "20015 km");
  await page.evaluate(async () => { window.profileGrid = "RJ90"; await cluster.refreshConfig(); });
  await cells(await add("K1SEAM", "AJ00"), "090°", "222 km");
  await page.evaluate(async () => { window.profileGrid = "RJ"; await cluster.refreshConfig(); });
  await cells(page.getByRole("row").filter({ hasText: "K1SEAM" }), "—", "—");

  await add("K1SEAM", "AJ00");
  await page.setViewportSize({ width: 320, height: 568 });
  const group = page.locator(".repeat-summary").filter({ hasText: "K1SEAM" });
  await group.waitFor();
  assert.equal(await group.evaluate(el => {
    const spotter = el.querySelector(".c-de").getBoundingClientRect();
    const azimuth = el.querySelector(".c-azimuth").getBoundingClientRect();
    return spotter.bottom <= azimuth.top;
  }), true, "the path metrics must not overlap the spotter on grouped phone rows");
});

async function inViewport(locator) {
  assert.equal(await locator.evaluate(el => {
    const r = el.getBoundingClientRect();
    return el.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2));
  }), true, "control must be hit-testable in the viewport");
}

function contrastRatio(text, background) {
  const luminance = color => color.map(channel => channel / 255)
    .map(channel => channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4)
    .reduce((sum, channel, i) => sum + channel * [0.2126, 0.7152, 0.0722][i], 0);
  const [darker, lighter] = [luminance(text), luminance(background)].sort((a, b) => a - b);
  return (lighter + 0.05) / (darker + 0.05);
}

async function textContrast(locator) {
  // These checks use solid surfaces and badge fills. Composite any ancestor opacity over them.
  const read = el => {
    if (!el.isConnected) return null;
    const channels = color => {
      const normalized = color.startsWith("color(srgb ");
      if (!normalized && !/^rgba?\(/.test(color)) throw new Error(`Unexpected computed color ${color}`);
      const [r, g, b, alpha = 1] = color.match(/-?(?:\d*\.)?\d+(?:e[+-]?\d+)?/gi).map(Number);
      const rgb = [r, g, b].map(channel => normalized ? channel * 255 : channel);
      return { rgb, alpha };
    };
    const layers = [];
    for (let node = el; node; node = node.parentElement) {
      const style = getComputedStyle(node);
      const color = channels(style.backgroundColor);
      layers.unshift({ ...color, opacity: Number(style.opacity) });
      if (color.alpha === 1 && style.opacity === "1" && layers.length > 1) break;
    }
    const mix = (fg, bg, alpha) => fg.map((channel, i) => channel * alpha + bg[i] * (1 - alpha));
    let background = [255, 255, 255];
    const opacity = [];
    for (const layer of layers) {
      if (layer.opacity < 1) opacity.push([background, layer.opacity]);
      background = mix(layer.rgb, background, layer.alpha);
    }
    const ink = channels(getComputedStyle(el).color);
    let foreground = mix(ink.rgb, background, ink.alpha);
    for (const [under, alpha] of opacity.reverse()) {
      foreground = mix(foreground, under, alpha);
      background = mix(background, under, alpha);
    }
    return [foreground, background];
  };
  // Closing a docked drawer can replace the measured cell as columns reflow.
  const sample = (await locator.evaluate(read)) ?? (await locator.evaluate(read));
  assert(sample, "badge or text must be attached when its contrast is measured");
  const [foreground, background] = sample;
  return contrastRatio(foreground, background);
}

test("filter-hidden text retains contrast in light and dark", async t => {
  const page = await openPage(t, { width: 1440, height: 900 }, false);
  await page.emulateMedia({ reducedMotion: "reduce" });
  await simulatedNode(page, 1);
  await page.evaluate(sample => {
    const record = [...sample];
    record[1] = 2;
    record[2] = String(BigInt(Date.now()) * 1000000n);
    record[11] = ["POTA"];
    cluster.session.onJsonLine(JSON.stringify(record));
  }, capture.filter(record => record[0] === "s")[1]);
  await button(page, "Hidden").click();
  await page.mouse.move(0, 0);
  const row = page.locator(".stream .row.hidden");
  await row.waitFor();
  const call = row.locator(".c-dx b");
  assert.match(await call.evaluate(el => getComputedStyle(el).textDecorationLine), /line-through/);
  await row.locator(".why").filter({ hasText: "no Show" }).waitFor();
  for (const theme of ["light", "dark"]) {
    for (const contrast of ["standard", "high"]) {
      await page.evaluate(prefs => setPrefs(prefs), { theme, contrast });
      const minimum = contrast === "high" ? 7 : 4.5;
      for (const selector of [".age", ".c-dx b", ".why", ".mode", ".conf", ".ev"]) {
        const ratio = await textContrast(row.locator(selector));
        assert(ratio >= minimum, `${theme} ${contrast} ${selector}: ${ratio.toFixed(2)} < ${minimum}`);
      }
    }
  }
});

test("maximum-contrast confidence badges retain legible text in the list and details", async t => {
  const page = await openPage(t, { width: 1440, height: 900 }, false);
  await page.emulateMedia({ reducedMotion: "reduce" });
  await simulatedNode(page, 1);
  await page.evaluate(sample => {
    for (const [i, confidence] of ["C", "?", "B"].entries()) {
      const record = [...sample];
      record[1] = i + 2;
      record[2] = String(BigInt(Date.now()) * 1000000n);
      record[16] = confidence;
      record[18] = [`K1ZZ${i}`, ...sample[18].slice(1)];
      cluster.session.onJsonLine(JSON.stringify(record));
    }
  }, capture.find(record => record[0] === "s"));
  const badge = (scope, letter) => scope.locator(".conf").getByText(letter, { exact: true });
  for (const theme of ["light", "dark"]) {
    await page.evaluate(theme => setPrefs({ theme, contrast: "high" }), theme);
    for (const [i, letter] of ["C", "?", "B"].entries()) {
      const row = page.getByRole("row").filter({ hasText: `K1ZZ${i}` });
      const listBadge = badge(row, letter);
      await listBadge.waitFor();
      const ratio = await textContrast(listBadge);
      assert(ratio >= 7, `${theme} ${letter}: ${ratio.toFixed(2)} < 7`);
      await row.click();
      const details = page.getByRole("dialog", { name: `Spot details for K1ZZ${i}` });
      const detailBadge = badge(details, letter);
      await detailBadge.waitFor();
      assert(await textContrast(detailBadge) >= 7, `${theme} ${letter} in spot details`);
      await button(details, "Close").click();
      await details.waitFor({ state: "detached" });
    }
  }
});

test("keyboard focus has a visible toolbar outline including search", async t => {
  const page = await openPage(t, { width: 1440, height: 900 }, false);
  await simulatedNode(page, 1);
  for (const width of [1920, 320]) {
    await page.setViewportSize({ width, height: 900 });
    for (const theme of ["light", "dark"]) {
      for (const contrast of ["high", "standard"]) {
        const expected = await page.evaluate(prefs => {
          setPrefs(prefs);
          const toolbar = document.querySelector(".topbar");
          toolbar.tabIndex = -1;
          toolbar.focus();
          return [...toolbar.querySelectorAll("a[href], button, input")]
            .filter(el => el.getClientRects().length && !el.disabled).length;
        }, { theme, contrast });
        for (let i = 0; i < expected; i++) {
          await page.keyboard.press("Tab");
          const [name, style, thickness, ink, deck] = await page.evaluate(() => {
            const el = document.activeElement;
            const toolbar = el.closest(".topbar");
            if (!toolbar || !el.matches(":focus-visible")) return [];
            const cue = getComputedStyle(el.closest(".search") || el);
            return [el.getAttribute("aria-label") || el.textContent.trim(), cue.outlineStyle, parseFloat(cue.outlineWidth),
              cue.outlineColor, getComputedStyle(toolbar).backgroundColor];
          });
          assert(name, `Tab ${i + 1} at ${width}px must reach the toolbar`);
          assert(style === "solid" && thickness >= 2, `${width}px ${theme} ${contrast}: ${name} needs a visible ring`);
          // The maximum-contrast deck is solid. Standard mode has a gradient and is checked visually.
          if (contrast === "high") {
            const rgb = color => color.match(/\d+/g).slice(0, 3).map(Number);
            assert(contrastRatio(rgb(ink), rgb(deck)) >= 3, `${width}px ${theme} ${contrast}: ${name} focus contrast`);
          }
        }
      }
    }
  }
});

test("the desktop icon toolbar fits one row and keeps navigation and spot controls usable", async t => {
  const page = await openPage(t, { width: 1280, height: 900 }, false);
  await simulatedNode(page, 1);
  await page.evaluate(() => {
    Object.assign(cluster, { buildVersion: "261124-long-review-identifier", visitorIP: "203.255.255.254",
      nodeCall: "VE3LONG-15", loginCall: "VE3LONG-15" });
    cluster.changed();
    setPrefs({ textSize: "normal", contrast: "high" });
  });
  await page.locator(".bsub-ip").getByText("203.255.255.254", { exact: true }).waitFor();
  await page.evaluate(() => document.fonts.ready);
  for (const width of [1280, 1320, 1321, 1400, 1401, 1440, 1700, 1701, 1920]) {
    await page.setViewportSize({ width, height: 900 });
    const layout = await page.locator(".topbar").evaluate(header => {
      const groups = Array.from(header.children, el => el.getBoundingClientRect());
      const centers = groups.map(rect => rect.y + rect.height / 2);
      const sizes = Array.from(header.querySelectorAll(".tool-btn"), el => [el.offsetWidth, el.offsetHeight]);
      return { spread: Math.max(...centers) - Math.min(...centers), sizes, overflow: document.documentElement.scrollWidth - innerWidth };
    });
    assert(layout.spread < 2, `toolbar wrapped at ${width}px`);
    assert.deepEqual(layout.sizes, Array.from({ length: 8 }, () => [38, 38]));
    assert.equal(layout.overflow, 0);
  }
  await page.keyboard.press("Tab");
  const history = view(page, "History");
  await history.focus();
  await history.locator(".tool-tip").waitFor();
  await history.press("Enter");
  await page.locator('.views a[href="#history"][aria-current=page]').waitFor();
  await view(page, "Spots").click();

  const grouping = page.getByRole("switch", { name: "Group repeats", exact: true });
  const grouped = await grouping.getAttribute("aria-checked");
  await grouping.click();
  await page.locator(`.spot-tools [role="switch"][aria-checked="${grouped === "true" ? "false" : "true"}"]`).waitFor();
  await button(page, "Pause live spots").click();
  const resume = button(page, "Resume live spots");
  await resume.hover();
  assert.match(await resume.locator(".tool-tip").textContent(), /[0-9]+s$/);
  await resume.click();
  await button(page, "Pause live spots").waitFor();
  assert.deepEqual(await page.evaluate(() => window.commands.filter(c => /^(PAUSE|RESUME)/.test(c))), ["PAUSE 60", "RESUME"]);
});

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
  await simulatedNode(page, 5);
  await page.locator(".spot-scroll").evaluate(el => { el.scrollTop = el.scrollHeight; });
  await page.waitForFunction(() => S.freezeId === 5);
  await page.setViewportSize({ width: 390, height: 1100 });
  await page.waitForFunction(() => document.querySelector(".spot-scroll").scrollTop === 0);
  await page.evaluate(() => window.feedSpots(6, 6));
  await button(page, "1 newer spot").click();
  await page.waitForFunction(() => S.freezeId === 0);
  await page.locator(".stream .row").first().getByText("W1A06").waitFor();
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
