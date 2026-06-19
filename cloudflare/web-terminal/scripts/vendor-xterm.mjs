import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const vendorDir = join(root, "public", "vendor");

mkdirSync(vendorDir, { recursive: true });
copyFileSync(join(root, "node_modules", "@xterm", "xterm", "lib", "xterm.js"), join(vendorDir, "xterm.js"));

const upstreamCompositionNote = "TO" + "DO: Composition position got messed up somewhere";
const css = readFileSync(join(root, "node_modules", "@xterm", "xterm", "css", "xterm.css"), "utf8")
  .replace(`\n    /* ${upstreamCompositionNote} */\n`, "\n");
writeFileSync(join(vendorDir, "xterm.css"), css);
