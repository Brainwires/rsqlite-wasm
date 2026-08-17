// Integration tests for the `node:fs`-backed file VFS (the `nodefs` feature).
// These drive the REAL `--target nodejs` wasm build against actual temp files —
// they are what prove first-class server-side persistence, not just the wrapper
// dispatch covered in index.test.ts.

import { describe, it, expect, afterEach } from "vitest";
import { existsSync, rmSync, statSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadWasmForTests } from "./wasm-loader.js";

const { WasmDatabase } = loadWasmForTests();

const tempFiles: string[] = [];
function tempDbPath(stem: string): string {
  const dir = mkdtempSync(join(tmpdir(), "rsqlite-file-"));
  const p = join(dir, `${stem}.db`);
  tempFiles.push(p);
  return p;
}

afterEach(() => {
  for (const p of tempFiles.splice(0)) {
    try {
      rmSync(p, { force: true });
    } catch {
      /* best effort */
    }
  }
});

describe("file backend (node:fs VFS)", () => {
  it("creates a real file on disk and persists across reopen", () => {
    const path = tempDbPath("persist");
    expect(existsSync(path)).toBe(false);

    // Session 1: create + insert, then drop the handle.
    let db = WasmDatabase.openWithFile(path);
    db.exec("CREATE TABLE todos (id INTEGER PRIMARY KEY, title TEXT NOT NULL)");
    db.execParams("INSERT INTO todos (title) VALUES (?)", ["buy milk"]);
    db.execParams("INSERT INTO todos (title) VALUES (?)", ["walk dog"]);
    db.free();

    expect(existsSync(path)).toBe(true);
    expect(statSync(path).size).toBeGreaterThan(100); // at least a header

    // Session 2: a fresh handle must observe the committed rows.
    db = WasmDatabase.openWithFile(path);
    const rows = db.query("SELECT id, title FROM todos ORDER BY id") as Array<{
      id: number;
      title: string;
    }>;
    db.free();

    expect(rows).toEqual([
      { id: 1, title: "buy milk" },
      { id: 2, title: "walk dog" },
    ]);
  });

  it("round-trips to a buffer that matches the file bytes", () => {
    const path = tempDbPath("buffer");
    const db = WasmDatabase.openWithFile(path);
    db.exec("CREATE TABLE t (v INTEGER)");
    db.exec("INSERT INTO t VALUES (42)");
    const buf = db.toBuffer();
    db.free();

    // A valid SQLite database starts with the "SQLite format 3\0" magic.
    const magic = new TextDecoder().decode(buf.subarray(0, 15));
    expect(magic).toBe("SQLite format 3");
    expect(buf.length).toBe(statSync(path).size);
  });

  it("survives updates and deletes (mutation persists, not just inserts)", () => {
    const path = tempDbPath("mutate");
    let db = WasmDatabase.openWithFile(path);
    db.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)");
    db.execParams("INSERT INTO kv VALUES (?, ?)", ["a", "1"]);
    db.execParams("INSERT INTO kv VALUES (?, ?)", ["b", "2"]);
    db.execParams("UPDATE kv SET v = ? WHERE k = ?", ["99", "a"]);
    db.execParams("DELETE FROM kv WHERE k = ?", ["b"]);
    db.free();

    db = WasmDatabase.openWithFile(path);
    const rows = db.query("SELECT k, v FROM kv ORDER BY k") as Array<{
      k: string;
      v: string;
    }>;
    db.free();

    expect(rows).toEqual([{ k: "a", v: "99" }]);
  });
});
