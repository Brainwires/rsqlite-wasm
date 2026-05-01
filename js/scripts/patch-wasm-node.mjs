// Post-process the wasm-pack `--target nodejs` output so it loads cleanly
// in Vitest under our ESM package. wasm-pack emits a CJS main file but
// puts ESM-syntax snippets next to it; under `"type": "module"` Node tries
// to interpret both as ESM and the main file's `require()` calls fail.
//
// Fix: drop a `package.json` inside the dist dir that pins the type to
// commonjs, then rewrite the snippet from ESM `export` declarations to
// CommonJS `module.exports`.

import * as fs from "node:fs";
import * as path from "node:path";

const root = path.join(import.meta.dirname, "..", "dist", "wasm-node");

if (!fs.existsSync(root)) {
  console.error(`patch-wasm-node: ${root} does not exist`);
  process.exit(1);
}

// 1. Pin the directory to CommonJS so .js files in it run as CJS.
fs.writeFileSync(
  path.join(root, "package.json"),
  JSON.stringify({ type: "commonjs" })
);

// 2. Rewrite snippets: convert `export function foo(...)` declarations
//    into local declarations + a final `module.exports = { ... }`.
function patchSnippet(file) {
  const src = fs.readFileSync(file, "utf8");
  const exportNames = [];
  const re = /^export\s+function\s+([a-zA-Z_$][\w$]*)/gm;
  let m;
  while ((m = re.exec(src)) !== null) exportNames.push(m[1]);
  if (exportNames.length === 0) return;

  const stripped = src.replace(/^export\s+function\s+/gm, "function ");
  const tail = `\nmodule.exports = { ${exportNames.join(", ")} };\n`;
  fs.writeFileSync(file, stripped + tail);
}

const snippetsDir = path.join(root, "snippets");
if (fs.existsSync(snippetsDir)) {
  for (const entry of fs.readdirSync(snippetsDir, {
    recursive: true,
    withFileTypes: true,
  })) {
    if (entry.isFile() && entry.name.endsWith(".js")) {
      patchSnippet(path.join(entry.parentPath, entry.name));
    }
  }
}
