import { accessSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const required = [
  "public/index.html",
  "public/app.js",
  "public/terminal.js",
  "public/styles.css",
  "public/vendor/xterm.js",
  "public/vendor/xterm.css",
  "src/worker.js",
  "wrangler.toml"
];

for (const path of required) {
  accessSync(join(root, path));
}

console.log(`static check passed (${required.length} files)`);
