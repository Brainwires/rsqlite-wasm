// Resolvable stand-in for the browser wasm-pack output
// (`./wasm/rsqlite_wasm.js`) which does not exist under node. vitest aliases
// the wasm file:// URL to this module so the dynamic import in index.ts /
// worker.ts can be *resolved*; the actual fakes are supplied per-test via
// vi.mock factories, which take precedence over this stub. This default
// implementation only exists so an un-mocked import does not crash at load.
export default function init(): Promise<void> {
  return Promise.resolve();
}

export class WasmDatabase {
  static openInMemory(): unknown {
    return {};
  }
}
