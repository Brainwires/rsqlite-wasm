import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  test: {
    // The browser wasm-pack output (`./wasm/rsqlite_wasm.js`, relative to the
    // src files) does not exist under node and cannot be loaded here. The
    // wrapper unit tests (index/worker) replace it with vi.mock, but vitest
    // must still be able to *resolve* the specifier before the mock factory
    // applies. Alias the file:// URL to a tiny stub module so resolution
    // succeeds; the per-test vi.mock factory then supplies the real fakes.
    alias: [
      {
        find: new URL("./src/wasm/rsqlite_wasm.js", import.meta.url).href,
        replacement: fileURLToPath(
          new URL("./test/fixtures/wasm-stub.ts", import.meta.url)
        ),
      },
    ],
    // The Node-target wasm-pack output uses CommonJS and synchronous WASM
    // initialization, which works under Node without any browser shims.
    environment: "node",
    include: ["test/**/*.test.ts"],
    // Each test file gets a fresh WASM module — keeps state isolated.
    isolate: true,
    testTimeout: 10000,
    coverage: {
      provider: "v8",
      // The .ts entry points are user-facing surface; the wasm glue under
      // dist/ is generated and excluded by default.
      include: ["src/**/*.ts"],
      exclude: ["dist/**", "scripts/**", "test/**", "**/*.d.ts"],
      reporter: ["text", "lcov", "html"],
      reportsDirectory: "coverage",
      // No threshold. The Node test target loads the wasm-bindgen output
      // directly and exercises the Rust surface (where the real coverage
      // gate is — see `scripts/coverage.sh rust`). The src/*.ts wrappers
      // (Database, WorkerDatabase) are thin JS shims around web APIs
      // (dynamic import, Worker) that don't run under node, so a
      // threshold here would be aspirational rather than meaningful.
      // Coverage is reported as an artifact for inspection.
    },
  },
});
