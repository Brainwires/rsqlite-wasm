import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The Node-target wasm-pack output uses CommonJS and synchronous WASM
    // initialization, which works under Node without any browser shims.
    environment: "node",
    include: ["test/**/*.test.ts"],
    // Each test file gets a fresh WASM module — keeps state isolated.
    isolate: true,
    testTimeout: 10000,
  },
});
