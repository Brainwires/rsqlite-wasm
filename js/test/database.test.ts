import { describe, it, expect, beforeAll } from "vitest";
import { loadWasmForTests, type WasmDatabaseInstance } from "./wasm-loader";

let WasmDatabase: ReturnType<typeof loadWasmForTests>["WasmDatabase"];

beforeAll(() => {
  WasmDatabase = loadWasmForTests().WasmDatabase;
});

function fresh(): WasmDatabaseInstance {
  return WasmDatabase.openInMemory();
}

describe("Database lifecycle", () => {
  it("opens an in-memory database", () => {
    const db = fresh();
    expect(db).toBeDefined();
    db.free();
  });

  it("constructs via `new` for an empty in-memory db too", () => {
    const db = new WasmDatabase();
    expect(db).toBeDefined();
    db.free();
  });

  it("free() makes the instance unusable", () => {
    const db = fresh();
    db.free();
    expect(() => db.exec("SELECT 1")).toThrow();
  });
});

describe("DDL + DML", () => {
  it("creates a table and inserts rows", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    const inserted = db.exec("INSERT INTO t (name) VALUES ('Alice'), ('Bob')");
    expect(Number(inserted)).toBe(2);
    db.free();
  });

  it("query returns rows as objects keyed by column name", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    db.exec("INSERT INTO t VALUES (1, 'Alice')");
    const rows = db.query("SELECT id, name FROM t") as Array<{
      id: number;
      name: string;
    }>;
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe(1);
    expect(rows[0].name).toBe("Alice");
    db.free();
  });

  it("queryOne returns null for an empty result", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const row = db.queryOne("SELECT * FROM t WHERE id = 99");
    expect(row).toBeNull();
    db.free();
  });

  it("queryOne returns the first row", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (n INTEGER)");
    db.exec("INSERT INTO t VALUES (10), (20)");
    const row = db.queryOne("SELECT n FROM t ORDER BY n") as { n: number };
    expect(row.n).toBe(10);
    db.free();
  });
});

describe("Parameter binding", () => {
  it("binds positional integers", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)");
    db.execParams("INSERT INTO t VALUES (?, ?)", [1, 42]);
    const row = db.queryOne("SELECT n FROM t WHERE id = 1") as { n: number };
    expect(row.n).toBe(42);
    db.free();
  });

  it("binds strings", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    db.execParams("INSERT INTO t VALUES (?, ?)", [1, "with 'quotes'"]);
    const row = db.queryOne("SELECT name FROM t WHERE id = 1") as { name: string };
    expect(row.name).toBe("with 'quotes'");
    db.free();
  });

  it("binds null", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, opt TEXT)");
    db.execParams("INSERT INTO t VALUES (?, ?)", [1, null]);
    const row = db.queryOne("SELECT opt FROM t WHERE id = 1") as { opt: unknown };
    expect(row.opt).toBeNull();
    db.free();
  });

  it("queryParams filters with bound parameters", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)");
    db.exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    const rows = db.queryParams("SELECT n FROM t WHERE n > ?", [15]);
    expect(rows).toHaveLength(2);
    db.free();
  });
});

describe("Transactions", () => {
  it("commit persists rows", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)");
    db.exec("BEGIN");
    db.exec("INSERT INTO t VALUES (1, 10)");
    db.exec("COMMIT");
    const rows = db.query("SELECT n FROM t");
    expect(rows).toHaveLength(1);
    db.free();
  });

  it("rollback discards rows", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)");
    db.exec("BEGIN");
    db.exec("INSERT INTO t VALUES (1, 10)");
    db.exec("ROLLBACK");
    const rows = db.query("SELECT n FROM t");
    expect(rows).toHaveLength(0);
    db.free();
  });
});

describe("Buffer roundtrip", () => {
  it("toBuffer + fromBuffer preserves data", () => {
    const db1 = fresh();
    db1.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    db1.exec("INSERT INTO t VALUES (1, 'snapshot')");
    const buf = db1.toBuffer();
    expect(buf.byteLength).toBeGreaterThan(0);
    db1.free();

    const db2 = WasmDatabase.fromBuffer(buf);
    const row = db2.queryOne("SELECT name FROM t WHERE id = 1") as {
      name: string;
    };
    expect(row.name).toBe("snapshot");
    db2.free();
  });

  it("fromBuffer rejects garbage input", () => {
    const garbage = new Uint8Array([0, 1, 2, 3, 4]);
    expect(() => WasmDatabase.fromBuffer(garbage)).toThrow();
  });
});

describe("execMany", () => {
  it("runs multiple statements separated by semicolons", () => {
    const db = fresh();
    db.execMany(
      `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);
       INSERT INTO t VALUES (1, 10);
       INSERT INTO t VALUES (2, 20);`
    );
    const rows = db.query("SELECT COUNT(*) AS c FROM t") as Array<{
      c: number;
    }>;
    expect(rows[0].c).toBe(2);
    db.free();
  });
});

describe("Error paths", () => {
  it("syntax errors throw", () => {
    const db = fresh();
    expect(() => db.exec("SELECTT * FROM nope")).toThrow();
    db.free();
  });

  it("constraint violations throw", () => {
    const db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    expect(() => db.exec("INSERT INTO t (id) VALUES (1)")).toThrow();
    db.free();
  });

  it("query against missing table throws", () => {
    const db = fresh();
    expect(() => db.query("SELECT * FROM nope")).toThrow();
    db.free();
  });
});
